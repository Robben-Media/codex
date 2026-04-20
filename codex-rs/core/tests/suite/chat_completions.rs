use std::sync::Arc;
use std::sync::Mutex;

use codex_features::Feature;
use codex_login::CodexAuth;
use codex_model_provider_info::ZAI_PROVIDER_ID;
use codex_model_provider_info::built_in_model_providers;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use wiremock::Match;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::Request;
use wiremock::ResponseTemplate;
use wiremock::matchers::header;
use wiremock::matchers::method;
use wiremock::matchers::path;

const EXISTING_ENV_VAR_WITH_NON_EMPTY_VALUE: &str = "PATH";

fn create_dummy_codex_auth() -> CodexAuth {
    CodexAuth::create_dummy_chatgpt_auth_for_testing()
}

#[derive(Debug, Clone, Default)]
struct CaptureRequests {
    requests: Arc<Mutex<Vec<Request>>>,
}

impl CaptureRequests {
    fn requests(&self) -> Vec<Request> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl Match for CaptureRequests {
    fn matches(&self, request: &Request) -> bool {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request.clone());
        true
    }
}

fn chat_sse(chunks: Vec<Value>) -> String {
    let mut out = String::new();
    for chunk in chunks {
        out.push_str("data: ");
        out.push_str(&chunk.to_string());
        out.push_str("\n\n");
    }
    out.push_str("data: [DONE]\n\n");
    out
}

fn body_json(request: &Request) -> Value {
    match serde_json::from_slice(&request.body) {
        Ok(body) => body,
        Err(err) => panic!("request body should be JSON: {err}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zai_provider_streams_chat_completions_and_tool_follow_up() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let capture = CaptureRequests::default();
    let auth_env_value = match std::env::var(EXISTING_ENV_VAR_WITH_NON_EMPTY_VALUE) {
        Ok(value) => value,
        Err(err) => panic!("{EXISTING_ENV_VAR_WITH_NON_EMPTY_VALUE} should be set: {err}"),
    };
    let auth_value = format!("Bearer {auth_env_value}");

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .and(header("Authorization", auth_value.as_str()))
        .and(capture.clone())
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(
                    chat_sse(vec![json!({
                        "id": "chatcmpl-tool",
                        "choices": [{
                            "index": 0,
                            "delta": {
                                "tool_calls": [{
                                    "index": 0,
                                    "id": "call-shell",
                                    "function": {
                                    "name": "exec_command",
                                    "arguments": "{\"cmd\":\"echo chat-completions\",\"workdir\":\".\"}"
                                    }
                                }]
                            },
                            "finish_reason": "tool_calls"
                        }]
                    })]),
                    "text/event-stream",
                ),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .and(header("Authorization", auth_value.as_str()))
        .and(capture.clone())
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(
                    chat_sse(vec![json!({
                        "id": "chatcmpl-final",
                        "choices": [{
                            "index": 0,
                            "delta": {"content": "done"},
                            "finish_reason": "stop"
                        }],
                        "usage": {
                            "prompt_tokens": 1,
                            "completion_tokens": 1,
                            "total_tokens": 2
                        }
                    })]),
                    "text/event-stream",
                ),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    let mut provider = built_in_model_providers(/*openai_base_url*/ None)
        .remove(ZAI_PROVIDER_ID)
        .unwrap_or_else(|| panic!("zai provider exists"));
    provider.base_url = Some(server.uri());
    provider.env_key = Some(EXISTING_ENV_VAR_WITH_NON_EMPTY_VALUE.to_string());

    let test = test_codex()
        .with_auth(create_dummy_codex_auth())
        .with_config(move |config| {
            config.model = Some("glm-5.1".to_string());
            config.model_provider_id = ZAI_PROVIDER_ID.to_string();
            config.model_provider = provider;
            config.model_reasoning_effort = Some(ReasoningEffort::Medium);
            config.model_reasoning_summary = Some(ReasoningSummary::None);
            config.features.enable(Feature::ShellTool).unwrap();
        })
        .build(&server)
        .await?;
    let codex = test.codex;

    codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "run a command".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
        })
        .await?;

    wait_for_event(&codex, |event| matches!(event, EventMsg::TurnComplete(_))).await;

    let requests = capture.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].url.path(), "/chat/completions");
    assert_eq!(requests[1].url.path(), "/chat/completions");

    let first_body = body_json(&requests[0]);
    assert_eq!(first_body["model"], "glm-5.1");
    assert_eq!(first_body["stream"], true);
    assert_eq!(first_body["tool_stream"], true);
    assert_eq!(first_body["thinking"]["type"], "enabled");
    assert!(first_body.get("store").is_none());
    assert!(first_body.get("include").is_none());
    assert!(first_body.get("parallel_tool_calls").is_none());
    assert!(first_body.get("prompt_cache_key").is_none());
    let tools = match first_body["tools"].as_array() {
        Some(tools) => tools,
        None => panic!("tools should be an array: {first_body}"),
    };
    assert!(
        tools
            .iter()
            .any(|tool| tool["function"]["name"] == "exec_command"),
        "{first_body}"
    );

    let second_body = body_json(&requests[1]);
    let messages = match second_body["messages"].as_array() {
        Some(messages) => messages,
        None => panic!("messages should be an array: {second_body}"),
    };
    assert!(messages.iter().any(|message| {
        message["role"] == "tool"
            && message["tool_call_id"] == "call-shell"
            && message["content"]
                .as_str()
                .is_some_and(|content| content.contains("chat-completions"))
    }));

    Ok(())
}
