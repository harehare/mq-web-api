use axum::{
    body::to_bytes,
    extract::Request,
    http::header::{CONTENT_LENGTH, CONTENT_TYPE},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::any::Any;

use crate::{metrics::metrics, problem::ProblemDetails};

/// Longest framework-generated error text that is carried over into the problem body.
const MAX_ERROR_TEXT_BYTES: usize = 4096;

fn is_json(response: &Response) -> bool {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.starts_with("application/json") || v.starts_with("application/problem+json")
        })
}

/// Rewrites error responses that aren't JSON yet into `application/problem+json`.
///
/// Errors raised before or outside our handlers (unknown route, wrong method,
/// body too large, request timeout, malformed JSON body, ...) come from axum and
/// tower as empty or plain-text bodies. Wrapping them here gives every error
/// response the same shape. Headers such as `Allow` or `Retry-After` are kept.
pub async fn problem_json_middleware(request: Request, next: Next) -> Response {
    let response = next.run(request).await;
    let status = response.status();

    if !(status.is_client_error() || status.is_server_error()) || is_json(&response) {
        return response;
    }

    let (parts, body) = response.into_parts();
    let text = to_bytes(body, MAX_ERROR_TEXT_BYTES)
        .await
        .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_string())
        .unwrap_or_default();

    let mut problem = ProblemDetails::new(status);
    if !text.is_empty() {
        problem = problem.with_detail("error", &text);
    }

    let mut response = problem.into_response();
    for (name, value) in &parts.headers {
        if name != CONTENT_TYPE && name != CONTENT_LENGTH {
            response.headers_mut().append(name.clone(), value.clone());
        }
    }
    response
}

/// Builds the 500 response for a handler that panicked. The panic itself was
/// already logged and counted by the hook installed at startup.
pub fn panic_response(_payload: Box<dyn Any + Send + 'static>) -> Response {
    ProblemDetails::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        .with_title("Internal error")
        .with_detail(
            "error",
            "The request could not be completed because of an internal error",
        )
        .into_response()
}

/// Records status and latency of every request for `GET /metrics`.
pub async fn metrics_middleware(request: Request, next: Next) -> Response {
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    metrics().record_http(response.status().as_u16(), started.elapsed().as_secs_f64());
    response
}
