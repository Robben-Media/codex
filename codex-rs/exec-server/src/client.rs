use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use arc_swap::ArcSwap;
use codex_app_server_protocol::JSONRPCNotification;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::watch;

use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tracing::debug;

use crate::ProcessId;
use crate::client_api::ExecServerClientConnectOptions;
use crate::client_api::RemoteExecServerConnectArgs;
use crate::connection::JsonRpcConnection;
use crate::process::ExecProcessEvent;
use crate::process::ExecProcessEventLog;
use crate::process::ExecProcessEventReceiver;
use crate::protocol::EXEC_CLOSED_METHOD;
use crate::protocol::EXEC_EXITED_METHOD;
use crate::protocol::EXEC_METHOD;
use crate::protocol::EXEC_OUTPUT_DELTA_METHOD;
use crate::protocol::EXEC_READ_METHOD;
use crate::protocol::EXEC_TERMINATE_METHOD;
use crate::protocol::EXEC_WRITE_METHOD;
use crate::protocol::ExecClosedNotification;
use crate::protocol::ExecExitedNotification;
use crate::protocol::ExecOutputDeltaNotification;
use crate::protocol::ExecParams;
use crate::protocol::ExecResponse;
use crate::protocol::FS_COPY_METHOD;
use crate::protocol::FS_CREATE_DIRECTORY_METHOD;
use crate::protocol::FS_GET_METADATA_METHOD;
use crate::protocol::FS_READ_DIRECTORY_METHOD;
use crate::protocol::FS_READ_FILE_METHOD;
use crate::protocol::FS_REMOVE_METHOD;
use crate::protocol::FS_WRITE_FILE_METHOD;
use crate::protocol::FsCopyParams;
use crate::protocol::FsCopyResponse;
use crate::protocol::FsCreateDirectoryParams;
use crate::protocol::FsCreateDirectoryResponse;
use crate::protocol::FsGetMetadataParams;
use crate::protocol::FsGetMetadataResponse;
use crate::protocol::FsReadDirectoryParams;
use crate::protocol::FsReadDirectoryResponse;
use crate::protocol::FsReadFileParams;
use crate::protocol::FsReadFileResponse;
use crate::protocol::FsRemoveParams;
use crate::protocol::FsRemoveResponse;
use crate::protocol::FsWriteFileParams;
use crate::protocol::FsWriteFileResponse;
use crate::protocol::HTTP_REQUEST_BODY_DELTA_METHOD;
use crate::protocol::HTTP_REQUEST_METHOD;
use crate::protocol::HttpRequestBodyDeltaNotification;
use crate::protocol::HttpRequestParams;
use crate::protocol::HttpRequestResponse;
use crate::protocol::INITIALIZE_METHOD;
use crate::protocol::INITIALIZED_METHOD;
use crate::protocol::InitializeParams;
use crate::protocol::InitializeResponse;
use crate::protocol::ProcessOutputChunk;
use crate::protocol::ReadParams;
use crate::protocol::ReadResponse;
use crate::protocol::TerminateParams;
use crate::protocol::TerminateResponse;
use crate::protocol::WriteParams;
use crate::protocol::WriteResponse;
use crate::rpc::RpcCallError;
use crate::rpc::RpcClient;
use crate::rpc::RpcClientEvent;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);
const PROCESS_EVENT_CHANNEL_CAPACITY: usize = 256;
const PROCESS_EVENT_RETAINED_BYTES: usize = 1024 * 1024;
/// Maximum queued body frames per streamed executor HTTP response.
const HTTP_BODY_DELTA_CHANNEL_CAPACITY: usize = 256;

impl Default for ExecServerClientConnectOptions {
    fn default() -> Self {
        Self {
            client_name: "codex-core".to_string(),
            initialize_timeout: INITIALIZE_TIMEOUT,
            resume_session_id: None,
        }
    }
}

impl From<RemoteExecServerConnectArgs> for ExecServerClientConnectOptions {
    fn from(value: RemoteExecServerConnectArgs) -> Self {
        Self {
            client_name: value.client_name,
            initialize_timeout: value.initialize_timeout,
            resume_session_id: value.resume_session_id,
        }
    }
}

impl RemoteExecServerConnectArgs {
    pub fn new(websocket_url: String, client_name: String) -> Self {
        Self {
            websocket_url,
            client_name,
            connect_timeout: CONNECT_TIMEOUT,
            initialize_timeout: INITIALIZE_TIMEOUT,
            resume_session_id: None,
        }
    }
}

pub(crate) struct SessionState {
    wake_tx: watch::Sender<u64>,
    events: ExecProcessEventLog,
    ordered_events: StdMutex<OrderedSessionEvents>,
    failure: Mutex<Option<String>>,
}

#[derive(Default)]
struct OrderedSessionEvents {
    last_published_seq: u64,
    // Server-side output, exit, and closed notifications are emitted by
    // different tasks and can reach the client out of order. Keep future events
    // here until all lower sequence numbers have been published.
    pending: BTreeMap<u64, ExecProcessEvent>,
}

#[derive(Clone)]
pub(crate) struct Session {
    client: ExecServerClient,
    process_id: ProcessId,
    state: Arc<SessionState>,
}

struct Inner {
    client: RpcClient,
    // The remote transport delivers one shared notification stream for every
    // process on the connection. Keep a local process_id -> session registry so
    // we can turn those connection-global notifications into process wakeups
    // without making notifications the source of truth for output delivery.
    sessions: ArcSwap<HashMap<ProcessId, Arc<SessionState>>>,
    // ArcSwap makes reads cheap on the hot notification path, but writes still
    // need serialization so concurrent register/remove operations do not
    // overwrite each other's copy-on-write updates.
    sessions_write_lock: Mutex<()>,
    // Streaming HTTP responses are keyed by a caller-chosen request id because
    // they share the same connection-global notification channel as process
    // output. Keep the routing table local to the client so higher layers can
    // consume body chunks like a normal byte stream.
    http_body_streams: ArcSwap<HashMap<String, HttpBodyStreamRoute>>,
    http_body_streams_write_lock: Mutex<()>,
    session_id: std::sync::RwLock<Option<String>>,
    reader_task: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct HttpBodyStreamRoute {
    tx: mpsc::Sender<HttpRequestBodyDeltaNotification>,
    failure: Arc<StdMutex<Option<String>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.reader_task.abort();
    }
}

#[derive(Clone)]
pub struct ExecServerClient {
    inner: Arc<Inner>,
}

/// Request-scoped stream of body chunks for an executor HTTP response.
///
/// The initial `http/request` call returns status and headers. This stream then
/// receives the ordered `http/request/bodyDelta` notifications for that request
/// id until EOF or a terminal error.
pub struct HttpResponseBodyStream {
    inner: Arc<Inner>,
    request_id: String,
    next_seq: u64,
    rx: mpsc::Receiver<HttpRequestBodyDeltaNotification>,
    failure: Arc<StdMutex<Option<String>>>,
    // Terminal frames can carry a final chunk; return that once, then EOF.
    pending_eof: bool,
    closed: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecServerError {
    #[error("failed to spawn exec-server: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("timed out connecting to exec-server websocket `{url}` after {timeout:?}")]
    WebSocketConnectTimeout { url: String, timeout: Duration },
    #[error("failed to connect to exec-server websocket `{url}`: {source}")]
    WebSocketConnect {
        url: String,
        #[source]
        source: tokio_tungstenite::tungstenite::Error,
    },
    #[error("timed out waiting for exec-server initialize handshake after {timeout:?}")]
    InitializeTimedOut { timeout: Duration },
    #[error("exec-server transport closed")]
    Closed,
    #[error("failed to serialize or deserialize exec-server JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("exec-server protocol error: {0}")]
    Protocol(String),
    #[error("exec-server rejected request ({code}): {message}")]
    Server { code: i64, message: String },
}

impl ExecServerClient {
    pub async fn connect_websocket(
        args: RemoteExecServerConnectArgs,
    ) -> Result<Self, ExecServerError> {
        let websocket_url = args.websocket_url.clone();
        let connect_timeout = args.connect_timeout;
        let (stream, _) = timeout(connect_timeout, connect_async(websocket_url.as_str()))
            .await
            .map_err(|_| ExecServerError::WebSocketConnectTimeout {
                url: websocket_url.clone(),
                timeout: connect_timeout,
            })?
            .map_err(|source| ExecServerError::WebSocketConnect {
                url: websocket_url.clone(),
                source,
            })?;

        Self::connect(
            JsonRpcConnection::from_websocket(
                stream,
                format!("exec-server websocket {websocket_url}"),
            ),
            args.into(),
        )
        .await
    }

    pub async fn initialize(
        &self,
        options: ExecServerClientConnectOptions,
    ) -> Result<InitializeResponse, ExecServerError> {
        let ExecServerClientConnectOptions {
            client_name,
            initialize_timeout,
            resume_session_id,
        } = options;

        timeout(initialize_timeout, async {
            let response: InitializeResponse = self
                .inner
                .client
                .call(
                    INITIALIZE_METHOD,
                    &InitializeParams {
                        client_name,
                        resume_session_id,
                    },
                )
                .await?;
            {
                let mut session_id = self
                    .inner
                    .session_id
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *session_id = Some(response.session_id.clone());
            }
            self.notify_initialized().await?;
            Ok(response)
        })
        .await
        .map_err(|_| ExecServerError::InitializeTimedOut {
            timeout: initialize_timeout,
        })?
    }

    pub async fn exec(&self, params: ExecParams) -> Result<ExecResponse, ExecServerError> {
        self.inner
            .client
            .call(EXEC_METHOD, &params)
            .await
            .map_err(Into::into)
    }

    pub async fn read(&self, params: ReadParams) -> Result<ReadResponse, ExecServerError> {
        self.inner
            .client
            .call(EXEC_READ_METHOD, &params)
            .await
            .map_err(Into::into)
    }

    pub async fn write(
        &self,
        process_id: &ProcessId,
        chunk: Vec<u8>,
    ) -> Result<WriteResponse, ExecServerError> {
        self.inner
            .client
            .call(
                EXEC_WRITE_METHOD,
                &WriteParams {
                    process_id: process_id.clone(),
                    chunk: chunk.into(),
                },
            )
            .await
            .map_err(Into::into)
    }

    pub async fn terminate(
        &self,
        process_id: &ProcessId,
    ) -> Result<TerminateResponse, ExecServerError> {
        self.inner
            .client
            .call(
                EXEC_TERMINATE_METHOD,
                &TerminateParams {
                    process_id: process_id.clone(),
                },
            )
            .await
            .map_err(Into::into)
    }

    pub async fn fs_read_file(
        &self,
        params: FsReadFileParams,
    ) -> Result<FsReadFileResponse, ExecServerError> {
        self.inner
            .client
            .call(FS_READ_FILE_METHOD, &params)
            .await
            .map_err(Into::into)
    }

    pub async fn fs_write_file(
        &self,
        params: FsWriteFileParams,
    ) -> Result<FsWriteFileResponse, ExecServerError> {
        self.inner
            .client
            .call(FS_WRITE_FILE_METHOD, &params)
            .await
            .map_err(Into::into)
    }

    pub async fn fs_create_directory(
        &self,
        params: FsCreateDirectoryParams,
    ) -> Result<FsCreateDirectoryResponse, ExecServerError> {
        self.inner
            .client
            .call(FS_CREATE_DIRECTORY_METHOD, &params)
            .await
            .map_err(Into::into)
    }

    pub async fn fs_get_metadata(
        &self,
        params: FsGetMetadataParams,
    ) -> Result<FsGetMetadataResponse, ExecServerError> {
        self.inner
            .client
            .call(FS_GET_METADATA_METHOD, &params)
            .await
            .map_err(Into::into)
    }

    pub async fn fs_read_directory(
        &self,
        params: FsReadDirectoryParams,
    ) -> Result<FsReadDirectoryResponse, ExecServerError> {
        self.inner
            .client
            .call(FS_READ_DIRECTORY_METHOD, &params)
            .await
            .map_err(Into::into)
    }

    pub async fn fs_remove(
        &self,
        params: FsRemoveParams,
    ) -> Result<FsRemoveResponse, ExecServerError> {
        self.inner
            .client
            .call(FS_REMOVE_METHOD, &params)
            .await
            .map_err(Into::into)
    }

    pub async fn fs_copy(&self, params: FsCopyParams) -> Result<FsCopyResponse, ExecServerError> {
        self.inner
            .client
            .call(FS_COPY_METHOD, &params)
            .await
            .map_err(Into::into)
    }

    /// Performs an executor-side HTTP request and buffers the response body.
    pub async fn http_request(
        &self,
        params: HttpRequestParams,
    ) -> Result<HttpRequestResponse, ExecServerError> {
        self.inner
            .client
            .call(HTTP_REQUEST_METHOD, &params)
            .await
            .map_err(Into::into)
    }

    /// Performs an executor-side HTTP request and returns a body stream.
    ///
    /// The method sets `stream_response` and fills `request_id` when needed so
    /// callers do not have to manage the body notification routing key.
    pub async fn http_request_stream(
        &self,
        mut params: HttpRequestParams,
    ) -> Result<(HttpRequestResponse, HttpResponseBodyStream), ExecServerError> {
        params.stream_response = true;
        let request_id = params
            .request_id
            .get_or_insert_with(|| format!("http-{}", uuid::Uuid::new_v4()))
            .clone();
        let (tx, rx) = mpsc::channel(HTTP_BODY_DELTA_CHANNEL_CAPACITY);
        let failure = Arc::new(StdMutex::new(None));
        self.inner
            .insert_http_body_stream(
                request_id.clone(),
                HttpBodyStreamRoute {
                    tx,
                    failure: Arc::clone(&failure),
                },
            )
            .await?;
        let response = self.http_request(params).await;
        if response.is_err() {
            self.inner.remove_http_body_stream(&request_id).await;
        }
        Ok((
            response?,
            HttpResponseBodyStream {
                inner: Arc::clone(&self.inner),
                request_id,
                next_seq: 1,
                rx,
                failure,
                pending_eof: false,
                closed: false,
            },
        ))
    }

    pub(crate) async fn register_session(
        &self,
        process_id: &ProcessId,
    ) -> Result<Session, ExecServerError> {
        let state = Arc::new(SessionState::new());
        self.inner
            .insert_session(process_id, Arc::clone(&state))
            .await?;
        Ok(Session {
            client: self.clone(),
            process_id: process_id.clone(),
            state,
        })
    }

    pub(crate) async fn unregister_session(&self, process_id: &ProcessId) {
        self.inner.remove_session(process_id).await;
    }

    pub fn session_id(&self) -> Option<String> {
        self.inner
            .session_id
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    async fn connect(
        connection: JsonRpcConnection,
        options: ExecServerClientConnectOptions,
    ) -> Result<Self, ExecServerError> {
        let (rpc_client, mut events_rx) = RpcClient::new(connection);
        let inner = Arc::new_cyclic(|weak| {
            let weak = weak.clone();
            let reader_task = tokio::spawn(async move {
                while let Some(event) = events_rx.recv().await {
                    match event {
                        RpcClientEvent::Notification(notification) => {
                            if let Some(inner) = weak.upgrade()
                                && let Err(err) =
                                    handle_server_notification(&inner, notification).await
                            {
                                fail_all_in_flight_work(
                                    &inner,
                                    format!("exec-server notification handling failed: {err}"),
                                )
                                .await;
                                return;
                            }
                        }
                        RpcClientEvent::Disconnected { reason } => {
                            if let Some(inner) = weak.upgrade() {
                                fail_all_in_flight_work(
                                    &inner,
                                    disconnected_message(reason.as_deref()),
                                )
                                .await;
                            }
                            return;
                        }
                    }
                }
            });

            Inner {
                client: rpc_client,
                sessions: ArcSwap::from_pointee(HashMap::new()),
                sessions_write_lock: Mutex::new(()),
                http_body_streams: ArcSwap::from_pointee(HashMap::new()),
                http_body_streams_write_lock: Mutex::new(()),
                session_id: std::sync::RwLock::new(None),
                reader_task,
            }
        });

        let client = Self { inner };
        client.initialize(options).await?;
        Ok(client)
    }

    async fn notify_initialized(&self) -> Result<(), ExecServerError> {
        self.inner
            .client
            .notify(INITIALIZED_METHOD, &serde_json::json!({}))
            .await
            .map_err(ExecServerError::Json)
    }
}

impl From<RpcCallError> for ExecServerError {
    fn from(value: RpcCallError) -> Self {
        match value {
            RpcCallError::Closed => Self::Closed,
            RpcCallError::Json(err) => Self::Json(err),
            RpcCallError::Server(error) => Self::Server {
                code: error.code,
                message: error.message,
            },
        }
    }
}

impl HttpResponseBodyStream {
    /// Receives the next response-body chunk.
    ///
    /// Returns `Ok(None)` at EOF and converts sequence gaps or executor-side
    /// stream errors into protocol errors.
    pub async fn recv(&mut self) -> Result<Option<Vec<u8>>, ExecServerError> {
        if self.pending_eof {
            self.pending_eof = false;
            self.close().await;
            return Ok(None);
        }

        let Some(delta) = self.rx.recv().await else {
            if let Some(error) = self.take_failure() {
                self.close().await;
                return Err(ExecServerError::Protocol(format!(
                    "http response stream `{}` failed: {error}",
                    self.request_id
                )));
            }
            self.close().await;
            return Ok(None);
        };
        if delta.seq != self.next_seq {
            self.close().await;
            return Err(ExecServerError::Protocol(format!(
                "http response stream `{}` received seq {}, expected {}",
                self.request_id, delta.seq, self.next_seq
            )));
        }
        self.next_seq += 1;
        let chunk = delta.delta.into_inner();

        if let Some(error) = delta.error {
            self.close().await;
            return Err(ExecServerError::Protocol(format!(
                "http response stream `{}` failed: {error}",
                self.request_id
            )));
        }
        if delta.done {
            self.close().await;
            if chunk.is_empty() {
                return Ok(None);
            }
            self.pending_eof = true;
        }
        Ok(Some(chunk))
    }

    /// Takes a deferred stream failure set by notification routing.
    fn take_failure(&self) -> Option<String> {
        self.failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// Removes this stream from the connection routing table once it is done.
    async fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.inner.remove_http_body_stream(&self.request_id).await;
    }
}

impl Drop for HttpResponseBodyStream {
    /// Schedules stream-route cleanup if the consumer drops before EOF.
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        let inner = Arc::clone(&self.inner);
        let request_id = self.request_id.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                inner.remove_http_body_stream(&request_id).await;
            });
        }
    }
}

impl SessionState {
    fn new() -> Self {
        let (wake_tx, _wake_rx) = watch::channel(0);
        Self {
            wake_tx,
            events: ExecProcessEventLog::new(
                PROCESS_EVENT_CHANNEL_CAPACITY,
                PROCESS_EVENT_RETAINED_BYTES,
            ),
            ordered_events: StdMutex::new(OrderedSessionEvents::default()),
            failure: Mutex::new(None),
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.wake_tx.subscribe()
    }

    pub(crate) fn subscribe_events(&self) -> ExecProcessEventReceiver {
        self.events.subscribe()
    }

    fn note_change(&self, seq: u64) {
        let next = (*self.wake_tx.borrow()).max(seq);
        let _ = self.wake_tx.send(next);
    }

    /// Publishes a process event only when all earlier sequenced events have
    /// already been published.
    ///
    /// Returns `true` only when this call actually publishes the ordered
    /// `Closed` event. The caller uses that signal to remove the session route
    /// after the terminal event is visible to subscribers, rather than when a
    /// possibly-early closed notification first arrives.
    fn publish_ordered_event(&self, event: ExecProcessEvent) -> bool {
        let Some(seq) = event.seq() else {
            self.events.publish(event);
            return false;
        };

        let mut ready = Vec::new();
        {
            let mut ordered_events = self
                .ordered_events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // We have already delivered this sequence number or moved past it,
            // so accepting it again would duplicate output or lifecycle events.
            if seq <= ordered_events.last_published_seq {
                return false;
            }

            ordered_events.pending.entry(seq).or_insert(event);
            loop {
                let next_seq = ordered_events.last_published_seq + 1;
                let Some(event) = ordered_events.pending.remove(&next_seq) else {
                    break;
                };
                ordered_events.last_published_seq += 1;
                ready.push(event);
            }
        }

        let mut published_closed = false;
        for event in ready {
            published_closed |= matches!(&event, ExecProcessEvent::Closed { .. });
            self.events.publish(event);
        }
        published_closed
    }

    async fn set_failure(&self, message: String) {
        let mut failure = self.failure.lock().await;
        let should_publish = failure.is_none();
        if should_publish {
            *failure = Some(message.clone());
        }
        drop(failure);
        let next = (*self.wake_tx.borrow()).saturating_add(1);
        let _ = self.wake_tx.send(next);
        if should_publish {
            let _ = self.publish_ordered_event(ExecProcessEvent::Failed(message));
        }
    }

    async fn failed_response(&self) -> Option<ReadResponse> {
        self.failure
            .lock()
            .await
            .clone()
            .map(|message| self.synthesized_failure(message))
    }

    fn synthesized_failure(&self, message: String) -> ReadResponse {
        let next_seq = (*self.wake_tx.borrow()).saturating_add(1);
        ReadResponse {
            chunks: Vec::new(),
            next_seq,
            exited: true,
            exit_code: None,
            closed: true,
            failure: Some(message),
        }
    }
}

impl Session {
    pub(crate) fn process_id(&self) -> &ProcessId {
        &self.process_id
    }

    pub(crate) fn subscribe_wake(&self) -> watch::Receiver<u64> {
        self.state.subscribe()
    }

    pub(crate) fn subscribe_events(&self) -> ExecProcessEventReceiver {
        self.state.subscribe_events()
    }

    pub(crate) async fn read(
        &self,
        after_seq: Option<u64>,
        max_bytes: Option<usize>,
        wait_ms: Option<u64>,
    ) -> Result<ReadResponse, ExecServerError> {
        if let Some(response) = self.state.failed_response().await {
            return Ok(response);
        }

        match self
            .client
            .read(ReadParams {
                process_id: self.process_id.clone(),
                after_seq,
                max_bytes,
                wait_ms,
            })
            .await
        {
            Ok(response) => Ok(response),
            Err(err) if is_transport_closed_error(&err) => {
                let message = disconnected_message(/*reason*/ None);
                self.state.set_failure(message.clone()).await;
                Ok(self.state.synthesized_failure(message))
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn write(&self, chunk: Vec<u8>) -> Result<WriteResponse, ExecServerError> {
        self.client.write(&self.process_id, chunk).await
    }

    pub(crate) async fn terminate(&self) -> Result<(), ExecServerError> {
        self.client.terminate(&self.process_id).await?;
        Ok(())
    }

    pub(crate) async fn unregister(&self) {
        self.client.unregister_session(&self.process_id).await;
    }
}

impl Inner {
    fn get_session(&self, process_id: &ProcessId) -> Option<Arc<SessionState>> {
        self.sessions.load().get(process_id).cloned()
    }

    async fn insert_session(
        &self,
        process_id: &ProcessId,
        session: Arc<SessionState>,
    ) -> Result<(), ExecServerError> {
        let _sessions_write_guard = self.sessions_write_lock.lock().await;
        let sessions = self.sessions.load();
        if sessions.contains_key(process_id) {
            return Err(ExecServerError::Protocol(format!(
                "session already registered for process {process_id}"
            )));
        }
        let mut next_sessions = sessions.as_ref().clone();
        next_sessions.insert(process_id.clone(), session);
        self.sessions.store(Arc::new(next_sessions));
        Ok(())
    }

    async fn remove_session(&self, process_id: &ProcessId) -> Option<Arc<SessionState>> {
        let _sessions_write_guard = self.sessions_write_lock.lock().await;
        let sessions = self.sessions.load();
        let session = sessions.get(process_id).cloned();
        session.as_ref()?;
        let mut next_sessions = sessions.as_ref().clone();
        next_sessions.remove(process_id);
        self.sessions.store(Arc::new(next_sessions));
        session
    }

    async fn take_all_sessions(&self) -> HashMap<ProcessId, Arc<SessionState>> {
        let _sessions_write_guard = self.sessions_write_lock.lock().await;
        let sessions = self.sessions.load();
        let drained_sessions = sessions.as_ref().clone();
        self.sessions.store(Arc::new(HashMap::new()));
        drained_sessions
    }

    /// Removes every streamed HTTP response route after a transport failure.
    async fn take_all_http_body_streams(&self) -> HashMap<String, HttpBodyStreamRoute> {
        let _streams_write_guard = self.http_body_streams_write_lock.lock().await;
        let streams = self.http_body_streams.load();
        let drained_streams = streams.as_ref().clone();
        self.http_body_streams.store(Arc::new(HashMap::new()));
        drained_streams
    }

    /// Looks up the route for a streamed HTTP response body notification.
    fn get_http_body_stream(&self, request_id: &str) -> Option<HttpBodyStreamRoute> {
        self.http_body_streams.load().get(request_id).cloned()
    }

    /// Registers a request id before issuing an executor streaming HTTP call.
    async fn insert_http_body_stream(
        &self,
        request_id: String,
        route: HttpBodyStreamRoute,
    ) -> Result<(), ExecServerError> {
        let _streams_write_guard = self.http_body_streams_write_lock.lock().await;
        let streams = self.http_body_streams.load();
        if streams.contains_key(&request_id) {
            return Err(ExecServerError::Protocol(format!(
                "http response stream already registered for request {request_id}"
            )));
        }
        let mut next_streams = streams.as_ref().clone();
        next_streams.insert(request_id, route);
        self.http_body_streams.store(Arc::new(next_streams));
        Ok(())
    }

    /// Removes a request id after EOF, error, request failure, or stream drop.
    async fn remove_http_body_stream(&self, request_id: &str) -> Option<HttpBodyStreamRoute> {
        let _streams_write_guard = self.http_body_streams_write_lock.lock().await;
        let streams = self.http_body_streams.load();
        let stream = streams.get(request_id).cloned();
        stream.as_ref()?;
        let mut next_streams = streams.as_ref().clone();
        next_streams.remove(request_id);
        self.http_body_streams.store(Arc::new(next_streams));
        stream
    }
}

impl HttpBodyStreamRoute {
    /// Records a terminal failure that the stream sees after queued chunks drain.
    fn set_failure(&self, message: String) {
        *self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(message);
    }
}

fn disconnected_message(reason: Option<&str>) -> String {
    match reason {
        Some(reason) => format!("exec-server transport disconnected: {reason}"),
        None => "exec-server transport disconnected".to_string(),
    }
}

fn is_transport_closed_error(error: &ExecServerError) -> bool {
    matches!(error, ExecServerError::Closed)
        || matches!(
            error,
            ExecServerError::Server {
                code: -32000,
                message,
            } if message == "JSON-RPC transport closed"
        )
}

async fn fail_all_sessions(inner: &Arc<Inner>, message: String) {
    let sessions = inner.take_all_sessions().await;

    for (_, session) in sessions {
        session.set_failure(message.clone()).await;
    }
}

/// Fails all in-flight work that depends on the shared JSON-RPC transport.
async fn fail_all_in_flight_work(inner: &Arc<Inner>, message: String) {
    fail_all_sessions(inner, message.clone()).await;
    fail_all_http_body_streams(inner, message).await;
}

/// Fails active streamed HTTP bodies so callers do not wait forever after a
/// transport disconnect or notification handling failure.
async fn fail_all_http_body_streams(inner: &Arc<Inner>, message: String) {
    let streams = inner.take_all_http_body_streams().await;
    for (request_id, route) in streams {
        route.set_failure(message.clone());
        let _ = route.tx.try_send(HttpRequestBodyDeltaNotification {
            request_id,
            seq: 1,
            delta: Vec::new().into(),
            done: true,
            error: Some(message.clone()),
        });
    }
}

async fn handle_server_notification(
    inner: &Arc<Inner>,
    notification: JSONRPCNotification,
) -> Result<(), ExecServerError> {
    match notification.method.as_str() {
        EXEC_OUTPUT_DELTA_METHOD => {
            let params: ExecOutputDeltaNotification =
                serde_json::from_value(notification.params.unwrap_or(Value::Null))?;
            if let Some(session) = inner.get_session(&params.process_id) {
                session.note_change(params.seq);
                let published_closed =
                    session.publish_ordered_event(ExecProcessEvent::Output(ProcessOutputChunk {
                        seq: params.seq,
                        stream: params.stream,
                        chunk: params.chunk,
                    }));
                if published_closed {
                    inner.remove_session(&params.process_id).await;
                }
            }
        }
        EXEC_EXITED_METHOD => {
            let params: ExecExitedNotification =
                serde_json::from_value(notification.params.unwrap_or(Value::Null))?;
            if let Some(session) = inner.get_session(&params.process_id) {
                session.note_change(params.seq);
                let published_closed = session.publish_ordered_event(ExecProcessEvent::Exited {
                    seq: params.seq,
                    exit_code: params.exit_code,
                });
                if published_closed {
                    inner.remove_session(&params.process_id).await;
                }
            }
        }
        EXEC_CLOSED_METHOD => {
            let params: ExecClosedNotification =
                serde_json::from_value(notification.params.unwrap_or(Value::Null))?;
            if let Some(session) = inner.get_session(&params.process_id) {
                session.note_change(params.seq);
                // Closed is terminal, but it can arrive before tail output or
                // exited. Keep routing this process until the ordered publisher
                // says Closed has actually been delivered.
                let published_closed =
                    session.publish_ordered_event(ExecProcessEvent::Closed { seq: params.seq });
                if published_closed {
                    inner.remove_session(&params.process_id).await;
                }
            }
        }
        HTTP_REQUEST_BODY_DELTA_METHOD => {
            let params: HttpRequestBodyDeltaNotification =
                serde_json::from_value(notification.params.unwrap_or(Value::Null))?;
            // Unknown request ids are ignored intentionally: a consumer may
            // have dropped its body stream after receiving the headers.
            if let Some(route) = inner.get_http_body_stream(&params.request_id) {
                let request_id = params.request_id.clone();
                match route.tx.try_send(params) {
                    Ok(()) => {}
                    Err(TrySendError::Closed(_)) => {
                        inner.remove_http_body_stream(&request_id).await;
                        debug!("http response stream receiver dropped before body delta delivery");
                    }
                    Err(TrySendError::Full(_)) => {
                        route.set_failure("body delta channel filled before delivery".to_string());
                        inner.remove_http_body_stream(&request_id).await;
                        debug!(
                            "closing http response stream `{request_id}` after body delta backpressure"
                        );
                    }
                }
            }
        }
        other => {
            debug!("ignoring unknown exec-server notification: {other}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use codex_app_server_protocol::JSONRPCMessage;
    use codex_app_server_protocol::JSONRPCNotification;
    use codex_app_server_protocol::JSONRPCResponse;
    use pretty_assertions::assert_eq;
    use tokio::io::AsyncBufReadExt;
    use tokio::io::AsyncWrite;
    use tokio::io::AsyncWriteExt;
    use tokio::io::BufReader;
    use tokio::io::duplex;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::time::Duration;
    use tokio::time::timeout;

    use super::ExecServerClient;
    use super::ExecServerClientConnectOptions;
    use crate::ProcessId;
    use crate::connection::JsonRpcConnection;
    use crate::process::ExecProcessEvent;
    use crate::protocol::EXEC_CLOSED_METHOD;
    use crate::protocol::EXEC_EXITED_METHOD;
    use crate::protocol::EXEC_OUTPUT_DELTA_METHOD;
    use crate::protocol::ExecClosedNotification;
    use crate::protocol::ExecExitedNotification;
    use crate::protocol::ExecOutputDeltaNotification;
    use crate::protocol::ExecOutputStream;
    use crate::protocol::HTTP_REQUEST_BODY_DELTA_METHOD;
    use crate::protocol::HTTP_REQUEST_METHOD;
    use crate::protocol::HttpHeader;
    use crate::protocol::HttpRequestBodyDeltaNotification;
    use crate::protocol::HttpRequestParams;
    use crate::protocol::HttpRequestResponse;
    use crate::protocol::INITIALIZE_METHOD;
    use crate::protocol::INITIALIZED_METHOD;
    use crate::protocol::InitializeResponse;
    use crate::protocol::ProcessOutputChunk;

    async fn read_jsonrpc_line<R>(lines: &mut tokio::io::Lines<BufReader<R>>) -> JSONRPCMessage
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let line = timeout(Duration::from_secs(1), lines.next_line())
            .await
            .expect("json-rpc read should not time out")
            .expect("json-rpc read should succeed")
            .expect("json-rpc connection should stay open");
        serde_json::from_str(&line).expect("json-rpc line should parse")
    }

    async fn write_jsonrpc_line<W>(writer: &mut W, message: JSONRPCMessage)
    where
        W: AsyncWrite + Unpin,
    {
        let encoded = serde_json::to_string(&message).expect("json-rpc message should serialize");
        writer
            .write_all(format!("{encoded}\n").as_bytes())
            .await
            .expect("json-rpc line should write");
    }

    #[tokio::test]
    async fn process_events_are_delivered_in_seq_order_when_notifications_are_reordered() {
        let (client_stdin, server_reader) = duplex(1 << 20);
        let (mut server_writer, client_stdout) = duplex(1 << 20);
        let (notifications_tx, mut notifications_rx) = mpsc::channel(16);
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_reader).lines();
            let initialize = read_jsonrpc_line(&mut lines).await;
            let request = match initialize {
                JSONRPCMessage::Request(request) if request.method == INITIALIZE_METHOD => request,
                other => panic!("expected initialize request, got {other:?}"),
            };
            write_jsonrpc_line(
                &mut server_writer,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(InitializeResponse {
                        session_id: "session-1".to_string(),
                    })
                    .expect("initialize response should serialize"),
                }),
            )
            .await;

            let initialized = read_jsonrpc_line(&mut lines).await;
            match initialized {
                JSONRPCMessage::Notification(notification)
                    if notification.method == INITIALIZED_METHOD => {}
                other => panic!("expected initialized notification, got {other:?}"),
            }

            while let Some(message) = notifications_rx.recv().await {
                write_jsonrpc_line(&mut server_writer, message).await;
            }
        });

        let client = ExecServerClient::connect(
            JsonRpcConnection::from_stdio(
                client_stdout,
                client_stdin,
                "test-exec-server-client".to_string(),
            ),
            ExecServerClientConnectOptions::default(),
        )
        .await
        .expect("client should connect");

        let process_id = ProcessId::from("reordered");
        let session = client
            .register_session(&process_id)
            .await
            .expect("session should register");
        let mut events = session.subscribe_events();

        for message in [
            JSONRPCMessage::Notification(JSONRPCNotification {
                method: EXEC_CLOSED_METHOD.to_string(),
                params: Some(
                    serde_json::to_value(ExecClosedNotification {
                        process_id: process_id.clone(),
                        seq: 4,
                    })
                    .expect("closed notification should serialize"),
                ),
            }),
            JSONRPCMessage::Notification(JSONRPCNotification {
                method: EXEC_OUTPUT_DELTA_METHOD.to_string(),
                params: Some(
                    serde_json::to_value(ExecOutputDeltaNotification {
                        process_id: process_id.clone(),
                        seq: 1,
                        stream: ExecOutputStream::Stdout,
                        chunk: b"one".to_vec().into(),
                    })
                    .expect("output notification should serialize"),
                ),
            }),
            JSONRPCMessage::Notification(JSONRPCNotification {
                method: EXEC_EXITED_METHOD.to_string(),
                params: Some(
                    serde_json::to_value(ExecExitedNotification {
                        process_id: process_id.clone(),
                        seq: 3,
                        exit_code: 0,
                    })
                    .expect("exit notification should serialize"),
                ),
            }),
            JSONRPCMessage::Notification(JSONRPCNotification {
                method: EXEC_OUTPUT_DELTA_METHOD.to_string(),
                params: Some(
                    serde_json::to_value(ExecOutputDeltaNotification {
                        process_id: process_id.clone(),
                        seq: 2,
                        stream: ExecOutputStream::Stderr,
                        chunk: b"two".to_vec().into(),
                    })
                    .expect("output notification should serialize"),
                ),
            }),
        ] {
            notifications_tx
                .send(message)
                .await
                .expect("notification should queue");
        }

        let mut delivered = Vec::new();
        for _ in 0..4 {
            delivered.push(
                timeout(Duration::from_secs(1), events.recv())
                    .await
                    .expect("process event should not time out")
                    .expect("process event stream should stay open"),
            );
        }

        assert_eq!(
            delivered,
            vec![
                ExecProcessEvent::Output(ProcessOutputChunk {
                    seq: 1,
                    stream: ExecOutputStream::Stdout,
                    chunk: b"one".to_vec().into(),
                }),
                ExecProcessEvent::Output(ProcessOutputChunk {
                    seq: 2,
                    stream: ExecOutputStream::Stderr,
                    chunk: b"two".to_vec().into(),
                }),
                ExecProcessEvent::Exited {
                    seq: 3,
                    exit_code: 0,
                },
                ExecProcessEvent::Closed { seq: 4 },
            ]
        );

        drop(notifications_tx);
        drop(client);
        server.await.expect("server task should finish");
    }

    #[tokio::test]
    async fn wake_notifications_do_not_block_other_sessions() {
        let (client_stdin, server_reader) = duplex(1 << 20);
        let (mut server_writer, client_stdout) = duplex(1 << 20);
        let (notifications_tx, mut notifications_rx) = mpsc::channel(16);
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_reader).lines();
            let initialize = read_jsonrpc_line(&mut lines).await;
            let request = match initialize {
                JSONRPCMessage::Request(request) if request.method == INITIALIZE_METHOD => request,
                other => panic!("expected initialize request, got {other:?}"),
            };
            write_jsonrpc_line(
                &mut server_writer,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(InitializeResponse {
                        session_id: "session-1".to_string(),
                    })
                    .expect("initialize response should serialize"),
                }),
            )
            .await;

            let initialized = read_jsonrpc_line(&mut lines).await;
            match initialized {
                JSONRPCMessage::Notification(notification)
                    if notification.method == INITIALIZED_METHOD => {}
                other => panic!("expected initialized notification, got {other:?}"),
            }

            while let Some(message) = notifications_rx.recv().await {
                write_jsonrpc_line(&mut server_writer, message).await;
            }
        });

        let client = ExecServerClient::connect(
            JsonRpcConnection::from_stdio(
                client_stdout,
                client_stdin,
                "test-exec-server-client".to_string(),
            ),
            ExecServerClientConnectOptions::default(),
        )
        .await
        .expect("client should connect");

        let noisy_process_id = ProcessId::from("noisy");
        let quiet_process_id = ProcessId::from("quiet");
        let _noisy_session = client
            .register_session(&noisy_process_id)
            .await
            .expect("noisy session should register");
        let quiet_session = client
            .register_session(&quiet_process_id)
            .await
            .expect("quiet session should register");
        let mut quiet_wake_rx = quiet_session.subscribe_wake();

        for seq in 0..=4096 {
            notifications_tx
                .send(JSONRPCMessage::Notification(JSONRPCNotification {
                    method: EXEC_OUTPUT_DELTA_METHOD.to_string(),
                    params: Some(
                        serde_json::to_value(ExecOutputDeltaNotification {
                            process_id: noisy_process_id.clone(),
                            seq,
                            stream: ExecOutputStream::Stdout,
                            chunk: b"x".to_vec().into(),
                        })
                        .expect("output notification should serialize"),
                    ),
                }))
                .await
                .expect("output notification should queue");
        }

        notifications_tx
            .send(JSONRPCMessage::Notification(JSONRPCNotification {
                method: EXEC_EXITED_METHOD.to_string(),
                params: Some(
                    serde_json::to_value(ExecExitedNotification {
                        process_id: quiet_process_id,
                        seq: 1,
                        exit_code: 17,
                    })
                    .expect("exit notification should serialize"),
                ),
            }))
            .await
            .expect("exit notification should queue");

        timeout(Duration::from_secs(1), quiet_wake_rx.changed())
            .await
            .expect("quiet session should receive wake before timeout")
            .expect("quiet wake channel should stay open");
        assert_eq!(*quiet_wake_rx.borrow(), 1);

        drop(notifications_tx);
        drop(client);
        server.await.expect("server task should finish");
    }

    /// What this tests: streamed executor HTTP response frames are routed by
    /// request id, delivered in sequence, concatenated by the caller, and
    /// removed from the routing table after EOF.
    #[tokio::test]
    async fn http_response_body_stream_receives_ordered_deltas() {
        // Phase 1: create an in-memory JSON-RPC peer so this test covers only
        // client protocol routing, without depending on the HTTP runner layer.
        let (client_stdin, server_reader) = duplex(1 << 20);
        let (mut server_writer, client_stdout) = duplex(1 << 20);
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_reader).lines();
            // Phase 2: complete the exec-server initialize handshake expected
            // before any executor methods are available.
            let initialize = read_jsonrpc_line(&mut lines).await;
            let request = match initialize {
                JSONRPCMessage::Request(request) if request.method == INITIALIZE_METHOD => request,
                other => panic!("expected initialize request, got {other:?}"),
            };
            write_jsonrpc_line(
                &mut server_writer,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(InitializeResponse {
                        session_id: "session-1".to_string(),
                    })
                    .expect("initialize response should serialize"),
                }),
            )
            .await;

            let initialized = read_jsonrpc_line(&mut lines).await;
            match initialized {
                JSONRPCMessage::Notification(notification)
                    if notification.method == INITIALIZED_METHOD => {}
                other => panic!("expected initialized notification, got {other:?}"),
            }

            // Phase 3: assert the client converts `http_request_stream` into a
            // streaming `http/request` call with the caller-provided request id.
            let http_request = read_jsonrpc_line(&mut lines).await;
            let request = match http_request {
                JSONRPCMessage::Request(request) if request.method == HTTP_REQUEST_METHOD => {
                    request
                }
                other => panic!("expected http/request, got {other:?}"),
            };
            let params: HttpRequestParams = serde_json::from_value(
                request
                    .params
                    .clone()
                    .expect("http/request should include params"),
            )
            .expect("http/request params should deserialize");
            assert_eq!(
                params,
                HttpRequestParams {
                    method: "GET".to_string(),
                    url: "https://example.test/mcp".to_string(),
                    headers: vec![HttpHeader {
                        name: "accept".to_string(),
                        value: "text/event-stream".to_string(),
                    }],
                    body: None,
                    timeout_ms: None,
                    request_id: Some("stream-1".to_string()),
                    stream_response: true,
                }
            );

            write_jsonrpc_line(
                &mut server_writer,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(HttpRequestResponse {
                        status: 200,
                        headers: vec![HttpHeader {
                            name: "content-type".to_string(),
                            value: "text/event-stream".to_string(),
                        }],
                        body: Vec::new().into(),
                    })
                    .expect("http/request response should serialize"),
                }),
            )
            .await;

            // Phase 4: emit body notifications in the order the stream should
            // expose them to the caller. The terminal frame intentionally carries
            // bytes so the test guards against treating `done` as "no payload".
            for delta in [
                HttpRequestBodyDeltaNotification {
                    request_id: "stream-1".to_string(),
                    seq: 1,
                    delta: b"hello ".to_vec().into(),
                    done: false,
                    error: None,
                },
                HttpRequestBodyDeltaNotification {
                    request_id: "stream-1".to_string(),
                    seq: 2,
                    delta: b"world".to_vec().into(),
                    done: false,
                    error: None,
                },
                HttpRequestBodyDeltaNotification {
                    request_id: "stream-1".to_string(),
                    seq: 3,
                    delta: b"!".to_vec().into(),
                    done: true,
                    error: None,
                },
            ] {
                write_jsonrpc_line(
                    &mut server_writer,
                    JSONRPCMessage::Notification(JSONRPCNotification {
                        method: HTTP_REQUEST_BODY_DELTA_METHOD.to_string(),
                        params: Some(
                            serde_json::to_value(delta)
                                .expect("body delta notification should serialize"),
                        ),
                    }),
                )
                .await;
            }
        });

        // Phase 5: start a streaming HTTP request through the public client API.
        let client = ExecServerClient::connect(
            JsonRpcConnection::from_stdio(
                client_stdout,
                client_stdin,
                "test-exec-server-client".to_string(),
            ),
            ExecServerClientConnectOptions::default(),
        )
        .await
        .expect("client should connect");

        let (response, mut body_stream) = client
            .http_request_stream(HttpRequestParams {
                method: "GET".to_string(),
                url: "https://example.test/mcp".to_string(),
                headers: vec![HttpHeader {
                    name: "accept".to_string(),
                    value: "text/event-stream".to_string(),
                }],
                body: None,
                timeout_ms: None,
                request_id: Some("stream-1".to_string()),
                stream_response: false,
            })
            .await
            .expect("http/request stream should start");

        // Phase 6: verify headers/status are returned immediately while the
        // response body is left for the notification stream.
        assert_eq!(
            response,
            HttpRequestResponse {
                status: 200,
                headers: vec![HttpHeader {
                    name: "content-type".to_string(),
                    value: "text/event-stream".to_string(),
                }],
                body: Vec::new().into(),
            }
        );

        // Phase 7: drain the body stream and verify both byte order and route
        // cleanup after EOF.
        let mut body = Vec::new();
        while let Some(chunk) = body_stream
            .recv()
            .await
            .expect("http response body delta should decode")
        {
            body.extend_from_slice(&chunk);
        }

        assert_eq!(
            (
                body,
                client.inner.get_http_body_stream("stream-1").is_none()
            ),
            (b"hello world!".to_vec(), true)
        );

        drop(client);
        server.await.expect("server task should finish");
    }

    /// What this tests: a cancelled streaming HTTP request can leave a route
    /// whose receiver has already been dropped, and the next body notification
    /// removes that stale route instead of leaking the request id.
    #[tokio::test]
    async fn http_response_body_stream_removes_route_when_receiver_is_closed() {
        // Phase 1: create an in-memory JSON-RPC peer and coordination channels so
        // the test can cancel after route registration but before http/request
        // returns headers.
        let (client_stdin, server_reader) = duplex(1 << 20);
        let (mut server_writer, client_stdout) = duplex(1 << 20);
        let (request_seen_tx, request_seen_rx) = oneshot::channel();
        let (send_delta_tx, send_delta_rx) = oneshot::channel();
        let (delta_written_tx, delta_written_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();

        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_reader).lines();

            // Phase 2: complete the initialize handshake.
            let initialize = read_jsonrpc_line(&mut lines).await;
            let request = match initialize {
                JSONRPCMessage::Request(request) if request.method == INITIALIZE_METHOD => request,
                other => panic!("expected initialize request, got {other:?}"),
            };
            write_jsonrpc_line(
                &mut server_writer,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(InitializeResponse {
                        session_id: "session-1".to_string(),
                    })
                    .expect("initialize response should serialize"),
                }),
            )
            .await;

            let initialized = read_jsonrpc_line(&mut lines).await;
            match initialized {
                JSONRPCMessage::Notification(notification)
                    if notification.method == INITIALIZED_METHOD => {}
                other => panic!("expected initialized notification, got {other:?}"),
            }

            // Phase 3: observe the http/request call, but deliberately do not
            // respond so the client-side future can be cancelled while its route
            // is still registered.
            let http_request = read_jsonrpc_line(&mut lines).await;
            match http_request {
                JSONRPCMessage::Request(request) if request.method == HTTP_REQUEST_METHOD => {}
                other => panic!("expected http/request, got {other:?}"),
            }
            request_seen_tx
                .send(())
                .expect("test should wait for http/request");

            // Phase 4: after the request future is cancelled, send one body frame.
            // The router should see the closed receiver and remove the stale route.
            send_delta_rx
                .await
                .expect("test should request a body delta");
            write_jsonrpc_line(
                &mut server_writer,
                JSONRPCMessage::Notification(JSONRPCNotification {
                    method: HTTP_REQUEST_BODY_DELTA_METHOD.to_string(),
                    params: Some(
                        serde_json::to_value(HttpRequestBodyDeltaNotification {
                            request_id: "cancelled-stream".to_string(),
                            seq: 1,
                            delta: b"unused".to_vec().into(),
                            done: false,
                            error: None,
                        })
                        .expect("body delta notification should serialize"),
                    ),
                }),
            )
            .await;
            delta_written_tx
                .send(())
                .expect("test should wait for body delta write");

            finish_rx.await.expect("test should finish server task");
        });

        let client = ExecServerClient::connect(
            JsonRpcConnection::from_stdio(
                client_stdout,
                client_stdin,
                "test-exec-server-client".to_string(),
            ),
            ExecServerClientConnectOptions::default(),
        )
        .await
        .expect("client should connect");

        // Phase 5: start a streaming request, wait until its route is registered,
        // then cancel before the server returns headers.
        let client_for_request = client.clone();
        let stream_task = tokio::spawn(async move {
            client_for_request
                .http_request_stream(HttpRequestParams {
                    method: "GET".to_string(),
                    url: "https://example.test/mcp".to_string(),
                    headers: Vec::new(),
                    body: None,
                    timeout_ms: None,
                    request_id: Some("cancelled-stream".to_string()),
                    stream_response: false,
                })
                .await
        });
        request_seen_rx
            .await
            .expect("server should observe http/request");
        assert!(
            client
                .inner
                .get_http_body_stream("cancelled-stream")
                .is_some()
        );
        stream_task.abort();
        let _ = stream_task.await;

        // Phase 6: send a body notification to the closed receiver and wait until
        // the notification handler removes the stale route.
        send_delta_tx
            .send(())
            .expect("server task should wait for body delta trigger");
        delta_written_rx
            .await
            .expect("server should write body delta");
        timeout(Duration::from_secs(1), async {
            loop {
                if client
                    .inner
                    .get_http_body_stream("cancelled-stream")
                    .is_none()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closed receiver should remove stale route");

        finish_tx
            .send(())
            .expect("server task should wait for test completion");
        drop(client);
        server.await.expect("server task should finish");
    }

    /// What this tests: an in-flight streamed HTTP body is failed when the
    /// shared JSON-RPC transport disconnects before a terminal body frame.
    #[tokio::test]
    async fn http_response_body_stream_fails_when_transport_disconnects() {
        // Phase 1: create an in-memory JSON-RPC peer so the server side can
        // drop the transport after returning HTTP response headers.
        let (client_stdin, server_reader) = duplex(1 << 20);
        let (mut server_writer, client_stdout) = duplex(1 << 20);
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_reader).lines();

            // Phase 2: complete the initialize handshake.
            let initialize = read_jsonrpc_line(&mut lines).await;
            let request = match initialize {
                JSONRPCMessage::Request(request) if request.method == INITIALIZE_METHOD => request,
                other => panic!("expected initialize request, got {other:?}"),
            };
            write_jsonrpc_line(
                &mut server_writer,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(InitializeResponse {
                        session_id: "session-1".to_string(),
                    })
                    .expect("initialize response should serialize"),
                }),
            )
            .await;

            let initialized = read_jsonrpc_line(&mut lines).await;
            match initialized {
                JSONRPCMessage::Notification(notification)
                    if notification.method == INITIALIZED_METHOD => {}
                other => panic!("expected initialized notification, got {other:?}"),
            }

            // Phase 3: return response headers for a streaming HTTP request
            // without sending the terminal body notification.
            let http_request = read_jsonrpc_line(&mut lines).await;
            let request = match http_request {
                JSONRPCMessage::Request(request) if request.method == HTTP_REQUEST_METHOD => {
                    request
                }
                other => panic!("expected http/request, got {other:?}"),
            };
            write_jsonrpc_line(
                &mut server_writer,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(HttpRequestResponse {
                        status: 200,
                        headers: Vec::new(),
                        body: Vec::new().into(),
                    })
                    .expect("http/request response should serialize"),
                }),
            )
            .await;

            // Phase 4: drop the server transport. The client must wake the body
            // stream instead of leaving recv() parked forever.
        });

        let client = ExecServerClient::connect(
            JsonRpcConnection::from_stdio(
                client_stdout,
                client_stdin,
                "test-exec-server-client".to_string(),
            ),
            ExecServerClientConnectOptions::default(),
        )
        .await
        .expect("client should connect");

        // Phase 5: start a streaming HTTP request and receive the headers.
        let (_response, mut body_stream) = client
            .http_request_stream(HttpRequestParams {
                method: "GET".to_string(),
                url: "https://example.test/mcp".to_string(),
                headers: Vec::new(),
                body: None,
                timeout_ms: None,
                request_id: Some("disconnect-stream".to_string()),
                stream_response: false,
            })
            .await
            .expect("http/request stream should start");

        // Phase 6: assert transport disconnect wakes the body stream with a
        // terminal error instead of hanging.
        let error = timeout(Duration::from_secs(1), body_stream.recv())
            .await
            .expect("disconnect should wake http body stream")
            .expect_err("disconnect should fail the http body stream");
        assert!(
            error
                .to_string()
                .contains("exec-server transport disconnected"),
            "unexpected stream error: {error}"
        );

        drop(client);
        server.await.expect("server task should finish");
    }

    /// What this tests: body-delta backpressure closes the route as an error
    /// rather than letting callers accept a truncated body as clean EOF.
    #[tokio::test]
    async fn http_response_body_stream_reports_backpressure_truncation() {
        // Phase 1: create an in-memory JSON-RPC peer that can enqueue more
        // body frames than the client-side channel can hold.
        let (client_stdin, server_reader) = duplex(1 << 20);
        let (mut server_writer, client_stdout) = duplex(1 << 20);
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_reader).lines();

            // Phase 2: complete the initialize handshake.
            let initialize = read_jsonrpc_line(&mut lines).await;
            let request = match initialize {
                JSONRPCMessage::Request(request) if request.method == INITIALIZE_METHOD => request,
                other => panic!("expected initialize request, got {other:?}"),
            };
            write_jsonrpc_line(
                &mut server_writer,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(InitializeResponse {
                        session_id: "session-1".to_string(),
                    })
                    .expect("initialize response should serialize"),
                }),
            )
            .await;

            let initialized = read_jsonrpc_line(&mut lines).await;
            match initialized {
                JSONRPCMessage::Notification(notification)
                    if notification.method == INITIALIZED_METHOD => {}
                other => panic!("expected initialized notification, got {other:?}"),
            }

            // Phase 3: return response headers for a streaming HTTP request.
            let http_request = read_jsonrpc_line(&mut lines).await;
            let request = match http_request {
                JSONRPCMessage::Request(request) if request.method == HTTP_REQUEST_METHOD => {
                    request
                }
                other => panic!("expected http/request, got {other:?}"),
            };
            write_jsonrpc_line(
                &mut server_writer,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(HttpRequestResponse {
                        status: 200,
                        headers: Vec::new(),
                        body: Vec::new().into(),
                    })
                    .expect("http/request response should serialize"),
                }),
            )
            .await;

            // Phase 4: send one more body frame than the bounded client channel
            // can accept before the consumer starts draining.
            for seq in 1..=(super::HTTP_BODY_DELTA_CHANNEL_CAPACITY as u64 + 1) {
                write_jsonrpc_line(
                    &mut server_writer,
                    JSONRPCMessage::Notification(JSONRPCNotification {
                        method: HTTP_REQUEST_BODY_DELTA_METHOD.to_string(),
                        params: Some(
                            serde_json::to_value(HttpRequestBodyDeltaNotification {
                                request_id: "backpressure-stream".to_string(),
                                seq,
                                delta: b"x".to_vec().into(),
                                done: false,
                                error: None,
                            })
                            .expect("body delta notification should serialize"),
                        ),
                    }),
                )
                .await;
            }
        });

        let client = ExecServerClient::connect(
            JsonRpcConnection::from_stdio(
                client_stdout,
                client_stdin,
                "test-exec-server-client".to_string(),
            ),
            ExecServerClientConnectOptions::default(),
        )
        .await
        .expect("client should connect");

        // Phase 5: start the streaming request but intentionally do not drain
        // the body until the server has overfilled the route.
        let (_response, mut body_stream) = client
            .http_request_stream(HttpRequestParams {
                method: "GET".to_string(),
                url: "https://example.test/mcp".to_string(),
                headers: Vec::new(),
                body: None,
                timeout_ms: None,
                request_id: Some("backpressure-stream".to_string()),
                stream_response: false,
            })
            .await
            .expect("http/request stream should start");

        // Phase 6: drain queued chunks and assert the truncated stream ends in
        // an explicit error, not a clean EOF.
        let mut chunks = 0;
        loop {
            let next = timeout(Duration::from_secs(1), body_stream.recv())
                .await
                .expect("backpressure should close http body stream");
            match next {
                Ok(Some(_chunk)) => {
                    chunks += 1;
                }
                Ok(None) => panic!("backpressure truncation should not look like clean EOF"),
                Err(error) => {
                    assert!(
                        error
                            .to_string()
                            .contains("body delta channel filled before delivery"),
                        "unexpected stream error: {error}"
                    );
                    break;
                }
            }
        }
        assert_eq!(chunks, super::HTTP_BODY_DELTA_CHANNEL_CAPACITY);

        drop(client);
        server.await.expect("server task should finish");
    }
}
