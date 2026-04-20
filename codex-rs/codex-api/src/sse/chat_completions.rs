use crate::common::ResponseEvent;
use crate::common::ResponseStream;
use crate::error::ApiError;
use crate::telemetry::SseTelemetry;
use codex_client::ByteStream;
use codex_client::StreamResponse;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ReasoningItemContent;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::TokenUsage;
use eventsource_stream::Eventsource;
use futures::StreamExt;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::time::timeout;
use tracing::debug;
use tracing::trace;

pub fn spawn_chat_completions_stream(
    stream_response: StreamResponse,
    idle_timeout: Duration,
    telemetry: Option<Arc<dyn SseTelemetry>>,
) -> ResponseStream {
    let (tx_event, rx_event) = mpsc::channel::<Result<ResponseEvent, ApiError>>(1600);
    tokio::spawn(process_chat_completions_sse(
        stream_response.bytes,
        tx_event,
        idle_timeout,
        telemetry,
    ));

    ResponseStream { rx_event }
}

#[derive(Debug, Default)]
struct ToolCallAccumulator {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

#[derive(Debug, Default)]
struct ChatCompletionsState {
    response_id: Option<String>,
    usage: Option<TokenUsage>,
    created_sent: bool,
    completed_sent: bool,
    message_item_started: bool,
    reasoning_item_started: bool,
    tool_calls: BTreeMap<i64, ToolCallAccumulator>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChunk {
    id: Option<String>,
    choices: Vec<ChatCompletionChoice>,
    usage: Option<ChatUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChoice {
    index: i64,
    delta: ChatCompletionDelta,
    finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ChatCompletionDelta {
    content: Option<String>,
    reasoning_content: Option<String>,
    tool_calls: Option<Vec<ChatToolCallDelta>>,
}

#[derive(Debug, Deserialize)]
struct ChatToolCallDelta {
    index: i64,
    id: Option<String>,
    function: Option<ChatFunctionDelta>,
}

#[derive(Debug, Deserialize)]
struct ChatFunctionDelta {
    name: Option<String>,
    arguments: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatUsage {
    prompt_tokens: i64,
    completion_tokens: i64,
    total_tokens: i64,
    completion_tokens_details: Option<ChatCompletionTokensDetails>,
    prompt_tokens_details: Option<ChatPromptTokensDetails>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionTokensDetails {
    reasoning_tokens: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ChatPromptTokensDetails {
    cached_tokens: Option<i64>,
}

impl From<ChatUsage> for TokenUsage {
    fn from(usage: ChatUsage) -> Self {
        Self {
            input_tokens: usage.prompt_tokens,
            cached_input_tokens: usage
                .prompt_tokens_details
                .and_then(|details| details.cached_tokens)
                .unwrap_or(0),
            output_tokens: usage.completion_tokens,
            reasoning_output_tokens: usage
                .completion_tokens_details
                .and_then(|details| details.reasoning_tokens)
                .unwrap_or(0),
            total_tokens: usage.total_tokens,
        }
    }
}

fn decode_chat_tool_name(name: String) -> (Option<String>, String) {
    if let Some((namespace, tool_name)) = name.rsplit_once("__")
        && namespace.starts_with("mcp__")
        && !tool_name.is_empty()
    {
        return (Some(namespace.to_string()), tool_name.to_string());
    }
    (None, name)
}

fn tool_call_item(call: ToolCallAccumulator) -> Option<ResponseItem> {
    let call_id = call.id?;
    let name = call.name?;
    let (namespace, name) = decode_chat_tool_name(name);
    Some(ResponseItem::FunctionCall {
        id: Some(call_id.clone()),
        name,
        namespace,
        arguments: call.arguments,
        call_id,
    })
}

async fn emit_completed(
    state: &mut ChatCompletionsState,
    tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>,
) -> bool {
    if state.completed_sent {
        return true;
    }
    for (_, call) in std::mem::take(&mut state.tool_calls) {
        if let Some(item) = tool_call_item(call)
            && tx_event
                .send(Ok(ResponseEvent::OutputItemDone(item)))
                .await
                .is_err()
        {
            return false;
        }
    }
    state.completed_sent = true;
    tx_event
        .send(Ok(ResponseEvent::Completed {
            response_id: state.response_id.clone().unwrap_or_default(),
            token_usage: state.usage.take(),
        }))
        .await
        .is_ok()
}

pub async fn process_chat_completions_sse(
    stream: ByteStream,
    tx_event: mpsc::Sender<Result<ResponseEvent, ApiError>>,
    idle_timeout: Duration,
    telemetry: Option<Arc<dyn SseTelemetry>>,
) {
    let mut stream = stream.eventsource();
    let mut state = ChatCompletionsState::default();

    loop {
        let start = Instant::now();
        let response = timeout(idle_timeout, stream.next()).await;
        if let Some(t) = telemetry.as_ref() {
            t.on_sse_poll(&response, start.elapsed());
        }
        let sse = match response {
            Ok(Some(Ok(sse))) => sse,
            Ok(Some(Err(e))) => {
                debug!("SSE Error: {e:#}");
                let _ = tx_event.send(Err(ApiError::Stream(e.to_string()))).await;
                return;
            }
            Ok(None) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(
                        "stream closed before chat completion finished".into(),
                    )))
                    .await;
                return;
            }
            Err(_) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream("idle timeout waiting for SSE".into())))
                    .await;
                return;
            }
        };

        trace!("SSE event: {}", &sse.data);
        if sse.data.trim() == "[DONE]" {
            let _ = emit_completed(&mut state, &tx_event).await;
            return;
        }

        let chunk: ChatCompletionChunk = match serde_json::from_str(&sse.data) {
            Ok(chunk) => chunk,
            Err(e) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(format!(
                        "failed to parse chat completion chunk: {e}"
                    ))))
                    .await;
                return;
            }
        };
        if state.response_id.is_none() {
            state.response_id = chunk.id.clone();
        }
        if !state.created_sent {
            state.created_sent = true;
            if tx_event.send(Ok(ResponseEvent::Created)).await.is_err() {
                return;
            }
        }
        state.usage = chunk.usage.map(Into::into).or(state.usage);

        for choice in chunk.choices {
            if let Some(delta) = choice.delta.reasoning_content {
                if !state.reasoning_item_started {
                    state.reasoning_item_started = true;
                    if tx_event
                        .send(Ok(ResponseEvent::OutputItemAdded(
                            ResponseItem::Reasoning {
                                id: format!("chatcmpl-reasoning-{}", choice.index),
                                summary: Vec::new(),
                                content: Some(vec![ReasoningItemContent::ReasoningText {
                                    text: String::new(),
                                }]),
                                encrypted_content: None,
                            },
                        )))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                if tx_event
                    .send(Ok(ResponseEvent::ReasoningContentDelta {
                        delta,
                        content_index: choice.index,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            if let Some(delta) = choice.delta.content {
                if !state.message_item_started {
                    state.message_item_started = true;
                    if tx_event
                        .send(Ok(ResponseEvent::OutputItemAdded(ResponseItem::Message {
                            id: Some(format!("chatcmpl-message-{}", choice.index)),
                            role: "assistant".to_string(),
                            content: vec![ContentItem::OutputText {
                                text: String::new(),
                            }],
                            end_turn: None,
                            phase: None,
                        })))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                if tx_event
                    .send(Ok(ResponseEvent::OutputTextDelta(delta)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            if let Some(tool_calls) = choice.delta.tool_calls {
                for tool_call in tool_calls {
                    let entry = state.tool_calls.entry(tool_call.index).or_default();
                    if let Some(id) = tool_call.id {
                        entry.id = Some(id);
                    }
                    if let Some(function) = tool_call.function {
                        if let Some(name) = function.name {
                            entry.name = Some(name);
                        }
                        if let Some(arguments) = function.arguments {
                            entry.arguments.push_str(&arguments);
                            let item_id = entry
                                .id
                                .clone()
                                .unwrap_or_else(|| tool_call.index.to_string());
                            if tx_event
                                .send(Ok(ResponseEvent::ToolCallInputDelta {
                                    item_id,
                                    call_id: entry.id.clone(),
                                    delta: arguments,
                                }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
            }
            if choice.finish_reason.is_some() {
                let _ = emit_completed(&mut state, &tx_event).await;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use codex_client::TransportError;
    use futures::stream;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use tokio::sync::mpsc;

    async fn collect_events(lines: Vec<String>) -> Vec<Result<ResponseEvent, ApiError>> {
        let data = lines
            .into_iter()
            .map(|line| Ok::<_, TransportError>(Bytes::from(format!("data: {line}\n\n"))));
        let stream = Box::pin(stream::iter(data));
        let (tx, mut rx) = mpsc::channel(32);
        process_chat_completions_sse(stream, tx, Duration::from_secs(5), None).await;
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    }

    #[tokio::test]
    async fn parses_text_reasoning_tool_calls_and_usage() {
        let events = collect_events(vec![
            json!({
                "id": "chatcmpl-1",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "reasoning_content": "think",
                        "content": "hello",
                        "tool_calls": [{
                            "index": 0,
                            "id": "call-1",
                            "function": {"name": "shell_command", "arguments": "{\"cmd\""}
                        }]
                    }
                }]
            })
            .to_string(),
            json!({
                "id": "chatcmpl-1",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "tool_calls": [{
                            "index": 0,
                            "function": {"arguments": ":\"ls\"}"}
                        }]
                    },
                    "finish_reason": "tool_calls"
                }],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 4,
                    "total_tokens": 14,
                    "completion_tokens_details": {"reasoning_tokens": 2},
                    "prompt_tokens_details": {"cached_tokens": 3}
                }
            })
            .to_string(),
        ])
        .await;

        assert!(matches!(events[0], Ok(ResponseEvent::Created)));
        assert!(matches!(
            &events[2],
            Ok(ResponseEvent::ReasoningContentDelta { delta, .. }) if delta == "think"
        ));
        assert!(matches!(
            &events[4],
            Ok(ResponseEvent::OutputTextDelta(delta)) if delta == "hello"
        ));
        assert!(matches!(
            &events[7],
            Ok(ResponseEvent::OutputItemDone(ResponseItem::FunctionCall {
                name,
                arguments,
                call_id,
                ..
            })) if name == "shell_command" && arguments == "{\"cmd\":\"ls\"}" && call_id == "call-1"
        ));
        assert!(matches!(
            &events[8],
            Ok(ResponseEvent::Completed {
                response_id,
                token_usage: Some(TokenUsage {
                    input_tokens: 10,
                    cached_input_tokens: 3,
                    output_tokens: 4,
                    reasoning_output_tokens: 2,
                    total_tokens: 14,
                }),
            }) if response_id == "chatcmpl-1"
        ));
    }

    #[tokio::test]
    async fn malformed_chunk_returns_stream_error() {
        let events = collect_events(vec!["not-json".to_string()]).await;

        assert!(matches!(events[0], Err(ApiError::Stream(_))));
    }

    #[test]
    fn decodes_mcp_tool_names() {
        assert_eq!(
            decode_chat_tool_name("mcp__calendar__lookup".to_string()),
            (Some("mcp__calendar".to_string()), "lookup".to_string())
        );
        assert_eq!(
            decode_chat_tool_name("shell_command".to_string()),
            (None, "shell_command".to_string())
        );
    }
}
