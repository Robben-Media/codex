use crate::auth::SharedAuthProvider;
use crate::common::ResponseStream;
use crate::common::ResponsesApiRequest;
use crate::common::TextControls;
use crate::endpoint::session::EndpointSession;
use crate::error::ApiError;
use crate::provider::Provider;
use crate::requests::Compression;
use crate::requests::headers::build_conversation_headers;
use crate::requests::headers::insert_header;
use crate::requests::headers::subagent_header;
use crate::sse::spawn_chat_completions_stream;
use crate::telemetry::SseTelemetry;
use codex_client::HttpTransport;
use codex_client::RequestCompression;
use codex_client::RequestTelemetry;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::SessionSource;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use tracing::instrument;

pub struct ChatCompletionsClient<T: HttpTransport> {
    session: EndpointSession<T>,
    sse_telemetry: Option<Arc<dyn SseTelemetry>>,
}

#[derive(Default)]
pub struct ChatCompletionsOptions {
    pub conversation_id: Option<String>,
    pub session_source: Option<SessionSource>,
    pub extra_headers: HeaderMap,
    pub compression: Compression,
}

impl<T: HttpTransport> ChatCompletionsClient<T> {
    pub fn new(transport: T, provider: Provider, auth: SharedAuthProvider) -> Self {
        Self {
            session: EndpointSession::new(transport, provider, auth),
            sse_telemetry: None,
        }
    }

    pub fn with_telemetry(
        self,
        request: Option<Arc<dyn RequestTelemetry>>,
        sse: Option<Arc<dyn SseTelemetry>>,
    ) -> Self {
        Self {
            session: self.session.with_request_telemetry(request),
            sse_telemetry: sse,
        }
    }

    #[instrument(
        name = "chat_completions.stream_request",
        level = "info",
        skip_all,
        fields(
            transport = "chat_completions_http",
            http.method = "POST",
            api.path = "chat/completions"
        )
    )]
    pub async fn stream_request(
        &self,
        request: ResponsesApiRequest,
        options: ChatCompletionsOptions,
    ) -> Result<ResponseStream, ApiError> {
        let ChatCompletionsOptions {
            conversation_id,
            session_source,
            extra_headers,
            compression,
        } = options;

        let body = serde_json::to_value(chat_completions_request_from_responses(request)?)?;

        let mut headers = extra_headers;
        if let Some(ref conv_id) = conversation_id {
            insert_header(&mut headers, "x-client-request-id", conv_id);
        }
        headers.extend(build_conversation_headers(conversation_id));
        if let Some(subagent) = subagent_header(&session_source) {
            insert_header(&mut headers, "x-openai-subagent", &subagent);
        }

        self.stream(body, headers, compression).await
    }

    fn path() -> &'static str {
        "chat/completions"
    }

    async fn stream(
        &self,
        body: Value,
        extra_headers: HeaderMap,
        compression: Compression,
    ) -> Result<ResponseStream, ApiError> {
        let request_compression = match compression {
            Compression::None => RequestCompression::None,
            Compression::Zstd => RequestCompression::Zstd,
        };

        let stream_response = self
            .session
            .stream_with(
                Method::POST,
                Self::path(),
                extra_headers,
                Some(body),
                |req| {
                    req.headers.insert(
                        http::header::ACCEPT,
                        HeaderValue::from_static("text/event-stream"),
                    );
                    req.compression = request_compression;
                },
            )
            .await?;

        Ok(spawn_chat_completions_stream(
            stream_response,
            self.session.provider().stream_idle_timeout,
            self.sse_telemetry.clone(),
        ))
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(err: serde_json::Error) -> Self {
        Self::InvalidRequest {
            message: format!("failed to encode chat completions request: {err}"),
        }
    }
}

#[derive(Debug, Serialize, PartialEq)]
struct ChatCompletionsRequest {
    model: String,
    messages: Vec<ChatMessage>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_stream: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ChatTool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<ChatThinking>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<ChatResponseFormat>,
}

#[derive(Debug, Serialize, PartialEq)]
struct ChatThinking {
    r#type: ChatThinkingType,
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ChatThinkingType {
    Enabled,
    Disabled,
}

#[derive(Debug, Serialize, PartialEq)]
struct ChatResponseFormat {
    r#type: &'static str,
}

#[derive(Debug, Serialize, PartialEq)]
struct ChatMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ChatToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Serialize, PartialEq)]
struct ChatToolCall {
    id: String,
    r#type: &'static str,
    function: ChatToolCallFunction,
}

#[derive(Debug, Serialize, PartialEq)]
struct ChatToolCallFunction {
    name: String,
    arguments: String,
}

#[derive(Debug, Serialize, PartialEq)]
struct ChatTool {
    r#type: &'static str,
    function: ChatToolFunction,
}

#[derive(Debug, Serialize, PartialEq)]
struct ChatToolFunction {
    name: String,
    description: String,
    parameters: Value,
}

fn chat_completions_request_from_responses(
    request: ResponsesApiRequest,
) -> Result<ChatCompletionsRequest, ApiError> {
    let mut messages = Vec::new();
    if !request.instructions.trim().is_empty() {
        messages.push(ChatMessage {
            role: "system".to_string(),
            content: Some(request.instructions),
            tool_calls: None,
            tool_call_id: None,
            name: None,
        });
    }
    for item in request.input {
        if let Some(message) = response_item_to_chat_message(item)? {
            messages.push(message);
        }
    }

    let tools = chat_tools_from_responses_tools(&request.tools);
    let has_tools = !tools.is_empty();
    Ok(ChatCompletionsRequest {
        model: request.model,
        messages,
        stream: true,
        tool_stream: has_tools.then_some(true),
        tools,
        tool_choice: has_tools.then_some("auto".to_string()),
        thinking: request.reasoning.map(|reasoning| ChatThinking {
            r#type: if reasoning.effort == Some(ReasoningEffort::None) {
                ChatThinkingType::Disabled
            } else {
                ChatThinkingType::Enabled
            },
        }),
        response_format: chat_response_format(request.text)?,
    })
}

fn chat_response_format(
    text: Option<TextControls>,
) -> Result<Option<ChatResponseFormat>, ApiError> {
    let Some(text) = text else {
        return Ok(None);
    };
    if text.format.is_some() {
        return Err(ApiError::InvalidRequest {
            message: "chat_completions providers do not support JSON schema response formats"
                .to_string(),
        });
    }
    Ok(None)
}

fn response_item_to_chat_message(item: ResponseItem) -> Result<Option<ChatMessage>, ApiError> {
    match item {
        ResponseItem::Message { role, content, .. } => Ok(Some(ChatMessage {
            role,
            content: Some(content_items_to_text(&content)?),
            tool_calls: None,
            tool_call_id: None,
            name: None,
        })),
        ResponseItem::FunctionCall {
            name,
            namespace,
            arguments,
            call_id,
            ..
        } => Ok(Some(ChatMessage {
            role: "assistant".to_string(),
            content: None,
            tool_calls: Some(vec![ChatToolCall {
                id: call_id,
                r#type: "function",
                function: ChatToolCallFunction {
                    name: encode_chat_tool_name(namespace.as_deref(), &name),
                    arguments,
                },
            }]),
            tool_call_id: None,
            name: None,
        })),
        ResponseItem::FunctionCallOutput { call_id, output } => Ok(Some(tool_output_message(
            call_id,
            None,
            function_output_to_text(&output),
        ))),
        ResponseItem::CustomToolCallOutput {
            call_id,
            name,
            output,
        } => Ok(Some(tool_output_message(
            call_id,
            name,
            function_output_to_text(&output),
        ))),
        ResponseItem::ToolSearchOutput {
            call_id: Some(call_id),
            tools,
            ..
        } => Ok(Some(tool_output_message(
            call_id,
            Some("tool_search".to_string()),
            serde_json::to_string(&tools).unwrap_or_default(),
        ))),
        _ => Ok(None),
    }
}

fn tool_output_message(call_id: String, name: Option<String>, content: String) -> ChatMessage {
    ChatMessage {
        role: "tool".to_string(),
        content: Some(content),
        tool_calls: None,
        tool_call_id: Some(call_id),
        name,
    }
}

fn content_items_to_text(content: &[ContentItem]) -> Result<String, ApiError> {
    let mut text = Vec::new();
    for item in content {
        match item {
            ContentItem::InputText { text: content }
            | ContentItem::OutputText { text: content } => {
                text.push(content.as_str());
            }
            ContentItem::InputImage { .. } => {
                return Err(ApiError::InvalidRequest {
                    message: "chat_completions providers do not support image input".to_string(),
                });
            }
        }
    }
    Ok(text.join("\n"))
}

fn function_output_to_text(output: &FunctionCallOutputPayload) -> String {
    output.body.to_text().unwrap_or_default()
}

fn chat_tools_from_responses_tools(tools: &[Value]) -> Vec<ChatTool> {
    let mut chat_tools = Vec::new();
    for tool in tools {
        let Some(tool_type) = tool.get("type").and_then(Value::as_str) else {
            continue;
        };
        match tool_type {
            "function" => {
                if let Some(chat_tool) = response_function_tool_to_chat_tool(None, tool) {
                    chat_tools.push(chat_tool);
                }
            }
            "namespace" => {
                let namespace = tool.get("name").and_then(Value::as_str);
                if let Some(children) = tool.get("tools").and_then(Value::as_array) {
                    for child in children {
                        if child.get("type").and_then(Value::as_str) == Some("function")
                            && let Some(chat_tool) =
                                response_function_tool_to_chat_tool(namespace, child)
                        {
                            chat_tools.push(chat_tool);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    chat_tools
}

fn response_function_tool_to_chat_tool(namespace: Option<&str>, tool: &Value) -> Option<ChatTool> {
    let name = tool.get("name").and_then(Value::as_str)?;
    let name = encode_chat_tool_name(namespace, name);
    if !is_valid_chat_tool_name(&name) {
        return None;
    }
    let description = tool
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let parameters =
        tool.get("parameters")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::from_iter([(
                "type".to_string(),
                Value::String("object".to_string()),
            )])));
    Some(ChatTool {
        r#type: "function",
        function: ChatToolFunction {
            name,
            description: description.to_string(),
            parameters,
        },
    })
}

pub(crate) fn encode_chat_tool_name(namespace: Option<&str>, name: &str) -> String {
    match namespace {
        Some(namespace) => format!("{namespace}__{name}"),
        None => name.to_string(),
    }
}

fn is_valid_chat_tool_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Reasoning;
    use crate::common::TextControls;
    use crate::common::TextFormat;
    use crate::common::TextFormatType;
    use codex_protocol::config_types::ReasoningSummary;
    use codex_protocol::models::FunctionCallOutputPayload;
    use codex_protocol::models::ResponseItem;
    use serde_json::json;

    fn base_request() -> ResponsesApiRequest {
        ResponsesApiRequest {
            model: "glm-5.1".to_string(),
            instructions: "system".to_string(),
            input: vec![ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: "hello".to_string(),
                }],
                end_turn: None,
                phase: None,
            }],
            tools: Vec::new(),
            tool_choice: "auto".to_string(),
            parallel_tool_calls: false,
            reasoning: None,
            store: false,
            stream: true,
            include: Vec::new(),
            service_tier: None,
            prompt_cache_key: None,
            text: None,
            client_metadata: None,
        }
    }

    #[test]
    fn translates_system_and_user_messages() {
        let request = chat_completions_request_from_responses(base_request()).unwrap();

        assert_eq!(request.model, "glm-5.1");
        assert_eq!(
            request.messages,
            vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: Some("system".to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: Some("hello".to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
            ]
        );
    }

    #[test]
    fn translates_tools_and_tool_outputs() {
        let mut request = base_request();
        request.tools = vec![json!({
            "type": "function",
            "name": "shell_command",
            "description": "Run a command",
            "parameters": {"type": "object", "properties": {}}
        })];
        request.input.push(ResponseItem::FunctionCallOutput {
            call_id: "call-1".to_string(),
            output: FunctionCallOutputPayload::from_text("ok".to_string()),
        });

        let request = chat_completions_request_from_responses(request).unwrap();

        assert_eq!(
            serde_json::to_value(&request.tools).unwrap(),
            json!([{
                "type": "function",
                "function": {
                    "name": "shell_command",
                    "description": "Run a command",
                    "parameters": {"type": "object", "properties": {}}
                }
            }])
        );
        assert_eq!(
            request.messages.last(),
            Some(&ChatMessage {
                role: "tool".to_string(),
                content: Some("ok".to_string()),
                tool_calls: None,
                tool_call_id: Some("call-1".to_string()),
                name: None,
            })
        );
        assert_eq!(request.tool_stream, Some(true));
    }

    #[test]
    fn translates_reasoning() {
        let mut request = base_request();
        request.reasoning = Some(Reasoning {
            effort: Some(ReasoningEffort::None),
            summary: Some(ReasoningSummary::Auto),
        });

        let request = chat_completions_request_from_responses(request).unwrap();

        assert_eq!(
            request.thinking,
            Some(ChatThinking {
                r#type: ChatThinkingType::Disabled,
            })
        );
        assert_eq!(request.response_format, None);
    }

    #[test]
    fn rejects_json_schema_response_format() {
        let mut request = base_request();
        request.text = Some(TextControls {
            verbosity: None,
            format: Some(TextFormat {
                r#type: TextFormatType::JsonSchema,
                strict: true,
                schema: json!({"type": "object"}),
                name: "schema".to_string(),
            }),
        });

        let err = chat_completions_request_from_responses(request).unwrap_err();

        assert!(matches!(err, ApiError::InvalidRequest { .. }));
    }

    #[test]
    fn rejects_image_input() {
        let mut request = base_request();
        request.input = vec![ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputImage {
                image_url: "file://image.png".to_string(),
                detail: None,
            }],
            end_turn: None,
            phase: None,
        }];

        let err = chat_completions_request_from_responses(request).unwrap_err();

        assert!(matches!(err, ApiError::InvalidRequest { .. }));
    }
}
