use axum::{
    extract::{ConnectInfo, Request, State},
    http::{HeaderValue, StatusCode, header::RETRY_AFTER},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use tracing::{debug, warn};

use crate::{
    problem::ProblemDetails,
    rate_limiter::{RateLimitError, RateLimiter},
};

/// State shared by [`rate_limit_middleware`].
#[derive(Clone)]
pub struct RateLimitState {
    /// Limiter applied per client IP.
    pub rate_limiter: Arc<RateLimiter>,
    /// Number of trusted reverse proxies; see `Config::trusted_proxy_count`.
    pub trusted_proxy_count: usize,
}

/// Paths polled by orchestrators and scrapers, which must never be throttled.
fn is_exempt(path: &str) -> bool {
    matches!(path, "/health" | "/ready" | "/metrics")
}

/// Applies the rate limit per client and sets rate limit headers on the response.
pub async fn rate_limit_middleware(
    State(state): State<RateLimitState>,
    request: Request,
    next: Next,
) -> Response {
    if is_exempt(request.uri().path()) {
        return next.run(request).await;
    }

    let rate_limiter = &state.rate_limiter;
    let identifier = extract_identifier(&request, state.trusted_proxy_count);

    match rate_limiter.check_and_increment(&identifier).await {
        Ok(()) => {
            debug!("Rate limit check passed for identifier: {}", identifier);

            // Add rate limit headers
            let mut response = next.run(request).await;
            if let Ok(current_usage) = rate_limiter.get_current_usage(&identifier).await {
                add_rate_limit_headers(&mut response, current_usage.unwrap_or(1), rate_limiter);
            }
            response
        }
        Err(RateLimitError::LimitExceeded { requests, limit }) => {
            warn!(
                "Rate limit exceeded for identifier '{}': {}/{} requests",
                identifier, requests, limit
            );

            let mut response = create_rate_limit_exceeded_response(
                requests,
                limit,
                rate_limiter.retry_after_seconds(),
            );
            add_rate_limit_headers(&mut response, requests, rate_limiter);
            response
        }
        Err(err) => {
            warn!(
                "Rate limiter error for identifier '{}': {}",
                identifier, err
            );

            // On database errors, allow the request to proceed
            // This ensures the service remains available even if rate limiting fails
            debug!("Allowing request due to rate limiter error");
            next.run(request).await
        }
    }
}

/// Resolves the client the request is attributed to.
///
/// `X-Forwarded-For` is only as trustworthy as the proxies appending to it, and
/// a client can prepend arbitrary entries. So with `trusted_proxy_count = n`
/// the address is taken `n` entries from the right (the one our outermost
/// trusted proxy saw), never from the left. With `0`, or when the header is
/// missing, short, or unparsable, the TCP peer address is used.
fn extract_identifier(request: &Request, trusted_proxy_count: usize) -> String {
    if trusted_proxy_count > 0 {
        if let Some(ip) = forwarded_client_ip(request, trusted_proxy_count) {
            return ip.to_string();
        }

        if let Some(real_ip) = request.headers().get("x-real-ip")
            && let Ok(ip) = real_ip
                .to_str()
                .unwrap_or_default()
                .trim()
                .parse::<IpAddr>()
        {
            return ip.to_string();
        }
    }

    if let Some(ConnectInfo(addr)) = request.extensions().get::<ConnectInfo<SocketAddr>>() {
        return addr.ip().to_string();
    }

    "unknown".to_string()
}

fn forwarded_client_ip(request: &Request, trusted_proxy_count: usize) -> Option<IpAddr> {
    let entries: Vec<&str> = request
        .headers()
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .collect();

    entries
        .len()
        .checked_sub(trusted_proxy_count)
        .and_then(|i| entries.get(i))
        .and_then(|entry| entry.parse().ok())
}

fn create_rate_limit_exceeded_response(requests: i64, limit: i64, retry_after: i64) -> Response {
    let mut response = ProblemDetails::new(StatusCode::TOO_MANY_REQUESTS)
        .with_title("Rate limit exceeded")
        .with_detail(
            "error",
            &format!(
                "Too many requests: {}/{} requests in current window",
                requests, limit
            ),
        )
        .with_number("requests", requests)
        .with_number("limit", limit)
        .into_response();
    response
        .headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from(retry_after.max(1) as u64));
    response
}

fn add_rate_limit_headers(response: &mut Response, current_usage: i64, rate_limiter: &RateLimiter) {
    let headers = response.headers_mut();

    // Add current usage
    if let Ok(usage_header) = HeaderValue::from_str(&current_usage.to_string()) {
        headers.insert("X-RateLimit-Used", usage_header);
    }

    // Add limit
    if let Ok(limit_header) = HeaderValue::from_str(&rate_limiter.requests_per_window().to_string())
    {
        headers.insert("X-RateLimit-Limit", limit_header);
    }

    // Add remaining requests
    let remaining = (rate_limiter.requests_per_window() - current_usage).max(0);
    if let Ok(remaining_header) = HeaderValue::from_str(&remaining.to_string()) {
        headers.insert("X-RateLimit-Remaining", remaining_header);
    }

    // Add window size
    if let Ok(window_header) =
        HeaderValue::from_str(&rate_limiter.window_size_seconds().to_string())
    {
        headers.insert("X-RateLimit-Window", window_header);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limiter::RateLimitConfig;
    use axum::{body::Body, http::Request};

    fn request_with(headers: &[(&str, &str)]) -> Request<Body> {
        let mut builder = Request::builder();
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(Body::empty()).unwrap()
    }

    fn with_peer(mut request: Request<Body>, peer: &str) -> Request<Body> {
        request
            .extensions_mut()
            .insert(ConnectInfo(peer.parse::<SocketAddr>().unwrap()));
        request
    }

    #[test]
    fn test_identifier_uses_rightmost_trusted_forwarded_entry() {
        // The client claims 6.6.6.6; the proxy appended the address it really saw.
        let request = request_with(&[("x-forwarded-for", "6.6.6.6, 192.168.1.1")]);
        assert_eq!(extract_identifier(&request, 1), "192.168.1.1");
    }

    #[test]
    fn test_identifier_skips_trusted_hops() {
        let request = request_with(&[("x-forwarded-for", "6.6.6.6, 192.168.1.1, 10.0.0.1")]);
        assert_eq!(extract_identifier(&request, 2), "192.168.1.1");
    }

    #[test]
    fn test_identifier_joins_repeated_forwarded_headers() {
        let request = request_with(&[
            ("x-forwarded-for", "6.6.6.6"),
            ("x-forwarded-for", "192.168.1.1"),
        ]);
        assert_eq!(extract_identifier(&request, 1), "192.168.1.1");
    }

    #[test]
    fn test_identifier_ignores_forwarding_headers_without_trusted_proxies() {
        let request = with_peer(
            request_with(&[("x-forwarded-for", "6.6.6.6"), ("x-real-ip", "7.7.7.7")]),
            "203.0.113.9:4000",
        );
        assert_eq!(extract_identifier(&request, 0), "203.0.113.9");
    }

    #[test]
    fn test_identifier_falls_back_to_peer_when_header_is_short_or_invalid() {
        let short = with_peer(
            request_with(&[("x-forwarded-for", "10.0.0.1")]),
            "203.0.113.9:4000",
        );
        assert_eq!(extract_identifier(&short, 2), "203.0.113.9");

        let garbage = with_peer(
            request_with(&[("x-forwarded-for", "not-an-ip")]),
            "203.0.113.9:4000",
        );
        assert_eq!(extract_identifier(&garbage, 1), "203.0.113.9");
    }

    #[test]
    fn test_identifier_from_real_ip() {
        let request = request_with(&[("x-real-ip", "192.168.1.100")]);
        assert_eq!(extract_identifier(&request, 1), "192.168.1.100");
    }

    #[test]
    fn test_identifier_fallback() {
        let request = request_with(&[]);
        assert_eq!(extract_identifier(&request, 1), "unknown");
    }

    #[test]
    fn test_exempt_paths() {
        assert!(is_exempt("/health"));
        assert!(is_exempt("/ready"));
        assert!(is_exempt("/metrics"));
        assert!(!is_exempt("/api/v1/query"));
    }

    #[test]
    fn test_rate_limit_headers_added() {
        let config = RateLimitConfig {
            requests_per_window: 10,
            window_size_seconds: 3600,
            cleanup_interval_seconds: 3600,
        };
        let rate_limiter = RateLimiter::new(config);

        let mut response = Response::new(Body::empty());
        add_rate_limit_headers(&mut response, 3, &rate_limiter);

        let headers = response.headers();
        assert_eq!(headers.get("X-RateLimit-Used").unwrap(), "3");
        assert_eq!(headers.get("X-RateLimit-Limit").unwrap(), "10");
        assert_eq!(headers.get("X-RateLimit-Remaining").unwrap(), "7");
        assert_eq!(headers.get("X-RateLimit-Window").unwrap(), "3600");
    }

    #[test]
    fn test_exceeded_response_is_problem_json_with_retry_after() {
        let response = create_rate_limit_exceeded_response(101, 100, 42);
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers()["content-type"],
            "application/problem+json"
        );
        assert_eq!(response.headers()[RETRY_AFTER], "42");
    }
}
