use std::fmt::Write;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Upper bounds, in seconds, of the latency histogram buckets.
const BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Lowest and highest HTTP status code that is counted.
const STATUS_MIN: usize = 100;
const STATUS_MAX: usize = 599;

/// Fixed-bucket latency histogram.
#[derive(Default)]
struct Histogram {
    buckets: [AtomicU64; BUCKETS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Histogram {
    fn observe(&self, seconds: f64) {
        if let Some(i) = BUCKETS.iter().position(|upper| seconds <= *upper) {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add((seconds * 1e6) as u64, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut cumulative = 0;
        for (upper, bucket) in BUCKETS.iter().zip(&self.buckets) {
            cumulative += bucket.load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{upper}\"}} {cumulative}");
        }
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let _ = writeln!(
            out,
            "{name}_sum {}",
            self.sum_micros.load(Ordering::Relaxed) as f64 / 1e6
        );
        let _ = writeln!(out, "{name}_count {count}");
    }
}

/// Process-wide counters rendered by `GET /metrics`.
pub struct Metrics {
    http_requests: Vec<AtomicU64>,
    http_duration: Histogram,
    query_duration: Histogram,
    query_cache_hits: AtomicU64,
    query_cache_misses: AtomicU64,
    query_rejected: AtomicU64,
    panics: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            http_requests: (STATUS_MIN..=STATUS_MAX)
                .map(|_| AtomicU64::new(0))
                .collect(),
            http_duration: Histogram::default(),
            query_duration: Histogram::default(),
            query_cache_hits: AtomicU64::new(0),
            query_cache_misses: AtomicU64::new(0),
            query_rejected: AtomicU64::new(0),
            panics: AtomicU64::new(0),
        }
    }

    /// Records a finished HTTP request.
    pub fn record_http(&self, status: u16, seconds: f64) {
        if let Some(counter) = (status as usize)
            .checked_sub(STATUS_MIN)
            .and_then(|i| self.http_requests.get(i))
        {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        self.http_duration.observe(seconds);
    }

    /// Records the wall time of one query evaluation (cache misses only).
    pub fn record_query(&self, seconds: f64) {
        self.query_duration.observe(seconds);
    }

    /// Records a query result served from the cache.
    pub fn record_cache_hit(&self) {
        self.query_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a cacheable query that had to be evaluated.
    pub fn record_cache_miss(&self) {
        self.query_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a query turned away because all evaluation slots were busy.
    pub fn record_query_rejected(&self) {
        self.query_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a panic, wherever it happened.
    pub fn record_panic(&self) {
        self.panics.fetch_add(1, Ordering::Relaxed);
    }

    /// Renders all metrics in the Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut out = String::new();

        let _ = writeln!(
            out,
            "# HELP mq_http_requests_total HTTP requests by response status."
        );
        let _ = writeln!(out, "# TYPE mq_http_requests_total counter");
        for (i, counter) in self.http_requests.iter().enumerate() {
            let value = counter.load(Ordering::Relaxed);
            if value > 0 {
                let _ = writeln!(
                    out,
                    "mq_http_requests_total{{status=\"{}\"}} {value}",
                    STATUS_MIN + i
                );
            }
        }

        self.http_duration.render(
            &mut out,
            "mq_http_request_duration_seconds",
            "End-to-end HTTP request latency.",
        );
        self.query_duration.render(
            &mut out,
            "mq_query_duration_seconds",
            "Query evaluation time, excluding cache hits.",
        );

        let _ = writeln!(
            out,
            "# HELP mq_query_cache_total Query cache lookups by result."
        );
        let _ = writeln!(out, "# TYPE mq_query_cache_total counter");
        let _ = writeln!(
            out,
            "mq_query_cache_total{{result=\"hit\"}} {}",
            self.query_cache_hits.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "mq_query_cache_total{{result=\"miss\"}} {}",
            self.query_cache_misses.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP mq_query_rejected_total Queries rejected because the server was saturated."
        );
        let _ = writeln!(out, "# TYPE mq_query_rejected_total counter");
        let _ = writeln!(
            out,
            "mq_query_rejected_total {}",
            self.query_rejected.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP mq_panics_total Panics caught while serving requests."
        );
        let _ = writeln!(out, "# TYPE mq_panics_total counter");
        let _ = writeln!(
            out,
            "mq_panics_total {}",
            self.panics.load(Ordering::Relaxed)
        );

        out
    }
}

/// Returns the process-wide metrics registry.
pub fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(Metrics::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_counts_requests_by_status() {
        let m = Metrics::new();
        m.record_http(200, 0.002);
        m.record_http(200, 0.3);
        m.record_http(429, 0.001);

        let text = m.render();
        assert!(
            text.contains("mq_http_requests_total{status=\"200\"} 2"),
            "{text}"
        );
        assert!(
            text.contains("mq_http_requests_total{status=\"429\"} 1"),
            "{text}"
        );
        assert!(!text.contains("status=\"404\""), "{text}");
    }

    #[test]
    fn test_histogram_buckets_are_cumulative() {
        let m = Metrics::new();
        m.record_http(200, 0.002);
        m.record_http(200, 0.3);
        m.record_http(200, 100.0);

        let text = m.render();
        assert!(
            text.contains("mq_http_request_duration_seconds_bucket{le=\"0.005\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("mq_http_request_duration_seconds_bucket{le=\"0.5\"} 2"),
            "{text}"
        );
        assert!(
            text.contains("mq_http_request_duration_seconds_bucket{le=\"+Inf\"} 3"),
            "{text}"
        );
        assert!(
            text.contains("mq_http_request_duration_seconds_count 3"),
            "{text}"
        );
    }

    #[test]
    fn test_out_of_range_status_is_ignored() {
        let m = Metrics::new();
        m.record_http(0, 0.1);
        m.record_http(999, 0.1);
        assert!(!m.render().contains("mq_http_requests_total{"));
    }
}
