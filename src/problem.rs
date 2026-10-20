use axum::http::{
    HeaderValue, StatusCode,
    header::{CONTENT_TYPE, RETRY_AFTER},
};
use axum::{
    Json,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde::ser::SerializeMap;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;

/// An RFC 9457 `application/problem+json` error body.
pub struct ProblemDetails {
    status: StatusCode,
    details: HashMap<Cow<'static, str>, Value>,
    retry_after: Option<u64>,
}

impl Serialize for ProblemDetails {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.details.len() + 1))?;
        map.serialize_entry("status", &self.status.as_u16())?;
        for (k, v) in &self.details {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl ProblemDetails {
    /// Creates a problem with the given HTTP status. `type` defaults to
    /// `about:blank` and `title` to the status' reason phrase, as RFC 9457 specifies.
    pub fn new(status: StatusCode) -> Self {
        let mut details = HashMap::new();
        details.insert(Cow::Borrowed("type"), Value::from("about:blank"));
        if let Some(reason) = status.canonical_reason() {
            details.insert(Cow::Borrowed("title"), Value::from(reason));
        }
        Self {
            status,
            details,
            retry_after: None,
        }
    }

    /// Sets the `type` member.
    pub fn with_type(mut self, value: &str) -> Self {
        self.details
            .insert(Cow::Borrowed("type"), Value::from(value));
        self
    }

    /// Adds an extension member.
    pub fn with_detail(mut self, key: &str, value: &str) -> Self {
        self.details
            .insert(Cow::Owned(key.to_string()), Value::from(value));
        self
    }

    /// Adds a numeric extension member.
    pub fn with_number(mut self, key: &str, value: i64) -> Self {
        self.details
            .insert(Cow::Owned(key.to_string()), Value::from(value));
        self
    }

    /// Sets the `title` member.
    pub fn with_title(mut self, value: &str) -> Self {
        self.details
            .insert(Cow::Borrowed("title"), Value::from(value));
        self
    }

    /// Sets the `Retry-After` response header, in seconds.
    pub fn with_retry_after(mut self, seconds: u64) -> Self {
        self.retry_after = Some(seconds);
        self
    }

    /// Returns the HTTP status of the problem.
    pub fn status(&self) -> StatusCode {
        self.status
    }
}

impl IntoResponse for ProblemDetails {
    fn into_response(self) -> Response {
        let status = self.status;
        let retry_after = self.retry_after;
        let mut response = (status, Json(self)).into_response();
        let headers = response.headers_mut();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        if let Some(seconds) = retry_after {
            headers.insert(RETRY_AFTER, HeaderValue::from(seconds));
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults_follow_rfc_9457() {
        let body = serde_json::to_value(ProblemDetails::new(StatusCode::NOT_FOUND)).unwrap();
        assert_eq!(body["status"], 404);
        assert_eq!(body["type"], "about:blank");
        assert_eq!(body["title"], "Not Found");
    }

    #[test]
    fn test_members_can_be_overridden_and_extended() {
        let body = serde_json::to_value(
            ProblemDetails::new(StatusCode::TOO_MANY_REQUESTS)
                .with_title("Rate limit exceeded")
                .with_detail("error", "slow down")
                .with_number("limit", 100),
        )
        .unwrap();
        assert_eq!(body["title"], "Rate limit exceeded");
        assert_eq!(body["error"], "slow down");
        assert_eq!(body["limit"], 100);
    }

    #[test]
    fn test_response_content_type_is_problem_json() {
        let response = ProblemDetails::new(StatusCode::BAD_REQUEST).into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.headers()[CONTENT_TYPE], "application/problem+json");
    }
}
