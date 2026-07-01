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
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::time::timeout;

#[derive(Default)]
struct ToolCallState {
    id: Option<String>,
    name: String,
    arguments: String,
}

pub fn spawn_zai_chat_stream(
    stream_response: StreamResponse,
    idle_timeout: Duration,
    telemetry: Option<Arc<dyn SseTelemetry>>,
) -> ResponseStream {
    let (tx_event, rx_event) = mpsc::channel::<Result<ResponseEvent, ApiError>>(1600);
    tokio::spawn(process_zai_chat_sse(
        stream_response.bytes,
        tx_event,
        idle_timeout,
        telemetry,
    ));
    ResponseStream { rx_event }
}

async fn process_zai_chat_sse(
    stream: ByteStream,
    tx_event: mpsc::Sender<Result<ResponseEvent, ApiError>>,
    idle_timeout: Duration,
    telemetry: Option<Arc<dyn SseTelemetry>>,
) {
    let mut stream = stream.eventsource();
    let mut assistant_text = String::new();
    let mut reasoning_text = String::new();
    let mut tool_calls: BTreeMap<usize, ToolCallState> = BTreeMap::new();
    let mut response_id = "zai-chat-response".to_string();
    let mut usage: Option<TokenUsage> = None;

    let _ = tx_event.send(Ok(ResponseEvent::Created)).await;

    loop {
        let start = Instant::now();
        let next = timeout(idle_timeout, stream.next()).await;
        if let Some(telemetry) = telemetry.as_ref() {
            telemetry.on_sse_poll(&next, start.elapsed());
        }
        let item = match next {
            Ok(Some(Ok(event))) => event,
            Ok(Some(Err(err))) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(format!(
                        "error decoding SSE stream: {err}"
                    ))))
                    .await;
                return;
            }
            Ok(None) => break,
            Err(_) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(format!(
                        "stream disconnected after {} ms of inactivity",
                        idle_timeout.as_millis()
                    ))))
                    .await;
                return;
            }
        };
        let data = item.data.trim();
        if data.is_empty() {
            continue;
        }
        if data == "[DONE]" {
            break;
        }
        let value: Value = match serde_json::from_str(data) {
            Ok(value) => value,
            Err(err) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream(format!(
                        "failed to parse Z.AI SSE payload: {err}"
                    ))))
                    .await;
                return;
            }
        };

        if let Some(id) = value.get("id").and_then(Value::as_str) {
            response_id = id.to_string();
        }
        if let Some(parsed_usage) = parse_usage(value.get("usage")) {
            usage = Some(parsed_usage);
        }

        let Some(choices) = value.get("choices").and_then(Value::as_array) else {
            continue;
        };
        for choice in choices {
            if let Some(delta) = choice.get("delta") {
                if let Some(text) = delta.get("reasoning_content").and_then(Value::as_str)
                    && !text.is_empty()
                {
                    reasoning_text.push_str(text);
                    let _ = tx_event
                        .send(Ok(ResponseEvent::ReasoningContentDelta {
                            delta: text.to_string(),
                            content_index: 0,
                        }))
                        .await;
                }
                if let Some(text) = delta.get("content").and_then(Value::as_str)
                    && !text.is_empty()
                {
                    assistant_text.push_str(text);
                    let _ = tx_event
                        .send(Ok(ResponseEvent::OutputTextDelta(text.to_string())))
                        .await;
                }
                collect_tool_call_deltas(delta, &mut tool_calls, &tx_event).await;
            }

            if let Some(message) = choice.get("message") {
                if let Some(text) = message.get("reasoning_content").and_then(Value::as_str) {
                    reasoning_text.push_str(text);
                }
                if let Some(text) = message.get("content").and_then(Value::as_str) {
                    assistant_text.push_str(text);
                }
                collect_tool_call_deltas(message, &mut tool_calls, &tx_event).await;
            }
        }
    }

    if !reasoning_text.is_empty() {
        let _ = tx_event
            .send(Ok(ResponseEvent::OutputItemDone(ResponseItem::Reasoning {
                id: String::new(),
                summary: Vec::new(),
                content: Some(vec![ReasoningItemContent::ReasoningText {
                    text: reasoning_text,
                }]),
                encrypted_content: None,
            })))
            .await;
    }

    for (_, tool_call) in tool_calls {
        if let Some(call_id) = tool_call.id {
            let _ = tx_event
                .send(Ok(ResponseEvent::OutputItemDone(
                    ResponseItem::FunctionCall {
                        id: None,
                        name: tool_call.name,
                        namespace: None,
                        arguments: tool_call.arguments,
                        call_id,
                    },
                )))
                .await;
        }
    }

    if !assistant_text.is_empty() {
        let _ = tx_event
            .send(Ok(ResponseEvent::OutputItemDone(ResponseItem::Message {
                id: None,
                role: "assistant".to_string(),
                content: vec![ContentItem::OutputText {
                    text: assistant_text,
                }],
                end_turn: None,
                phase: None,
            })))
            .await;
    }

    let _ = tx_event
        .send(Ok(ResponseEvent::Completed {
            response_id,
            token_usage: usage,
        }))
        .await;
}

async fn collect_tool_call_deltas(
    value: &Value,
    tool_calls: &mut BTreeMap<usize, ToolCallState>,
    tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>,
) {
    let Some(items) = value.get("tool_calls").and_then(Value::as_array) else {
        return;
    };

    for item in items {
        let index = item
            .get("index")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(tool_calls.len());
        let state = tool_calls.entry(index).or_default();
        if let Some(id) = item.get("id").and_then(Value::as_str) {
            state.id = Some(id.to_string());
        }
        if let Some(function) = item.get("function") {
            if let Some(name) = function.get("name").and_then(Value::as_str) {
                state.name.push_str(name);
            }
            if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                state.arguments.push_str(arguments);
                let _ = tx_event
                    .send(Ok(ResponseEvent::ToolCallInputDelta {
                        item_id: state.id.clone().unwrap_or_else(|| index.to_string()),
                        call_id: state.id.clone(),
                        delta: arguments.to_string(),
                    }))
                    .await;
            }
        }
    }
}

fn parse_usage(value: Option<&Value>) -> Option<TokenUsage> {
    let usage = value?;
    let input_tokens = usage
        .get("prompt_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let output_tokens = usage
        .get("completion_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let total_tokens = usage
        .get("total_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let cached_input_tokens = usage
        .get("prompt_tokens_details")
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let reasoning_output_tokens = usage
        .get("completion_tokens_details")
        .and_then(|details| details.get("reasoning_tokens"))
        .and_then(Value::as_i64)
        .unwrap_or(0);

    Some(TokenUsage {
        input_tokens,
        cached_input_tokens,
        output_tokens,
        reasoning_output_tokens,
        total_tokens,
    })
}
