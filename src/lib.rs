#![forbid(unsafe_code)]

//! The header route technology — a technology of `xmip-core-route`.
//!
//! A Subscription's filter names properties, and each property is read from
//! one source. This source reads what the transport delivered beside the
//! bytes: `header:<protocol>.<name>` is the header called `<name>` that
//! `<protocol>` carried. The name's case counts exactly where the protocol
//! says it does: `header:http.content-type` and `header:http.Content-Type`
//! are one property because HTTP folds field names, while
//! `header:kafka.Trace-Id` and `header:kafka.trace-id` are two because Kafka
//! does not — `context::property::HEADER_CASE_FOLDING` is the one table.
//! `header:amqp.x-priority` is another protocol's. ADR-0046.
//!
//! **Whose header it is, is part of its name.** A transport writes a header
//! into the Message Context under `<protocol>.header.<name>`, built by
//! `context::property::header`, and this reads it through the same builder
//! (the owner, 2026-09-24; ADR-0019, amendment 2026-09-24). The protocol is
//! the text before the first dot, since a protocol's word has none and a
//! header's name may. A header value reads through `route::routable`, as
//! every context value a filter names does: as the text it was, a `Null`
//! absent and bytes refused (ADR-0046, amended 2026-09-24).
//!
//! A route technology does not decide anything: it reads.

use context::property;
use message::Message;
use path::Content;
use route::{Reading, Source};

/// The manifest leaf and the prefix a property carries.
pub const TECHNOLOGY: &str = "header";

/// Reads `header:<protocol>.<name>` from the `<protocol>.header.<name>`
/// context keys.
pub struct HeaderSource;

impl Source for HeaderSource {
    fn technology(&self) -> &'static str {
        TECHNOLOGY
    }

    /// The context key, built once: the builder folds what the protocol
    /// folds, so every Message after is one exact lookup, and case is kept
    /// wherever it counts.
    fn compile(&self, name: &str) -> Result<Box<dyn Reading>, String> {
        let Some((protocol, header)) = name
            .split_once('.')
            .filter(|(protocol, header)| !protocol.is_empty() && !header.is_empty())
        else {
            return Err(
                "a header is named by its protocol and its name: header:http.content-type"
                    .to_string(),
            );
        };
        Ok(Box::new(Key(property::header(protocol, header))))
    }
}

/// The context key a header is delivered under.
struct Key(String);

impl Reading for Key {
    fn read(&self, message: &Message, _: Option<&Content<'_>>) -> Result<Option<String>, String> {
        route::routable(&self.0, message.context().get(&self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use context::MessageContext;
    use message::MessageTreatment;
    use route::{Gathering, Promoted, SourceError};
    use xcore::MessageId;
    use xcore::ScalarValue;

    fn message() -> Message {
        let context = MessageContext::new()
            .with_value(
                property::header("http", "Content-Type"),
                ScalarValue::Text("application/json".into()),
            )
            .with_value(
                property::header("http", "Content-Length"),
                ScalarValue::Integer(42),
            )
            .with_value(property::header("http", "X-Empty"), ScalarValue::Null)
            .with_value(
                property::header("http", "X-Bytes"),
                ScalarValue::Binary(vec![1, 2]),
            )
            .with_value(
                property::header("amqp", "content-type"),
                ScalarValue::Text("text/plain".into()),
            )
            .with_value(
                property::header("kafka", "trace.id"),
                ScalarValue::Text("4bf92f35".into()),
            )
            .with_value(
                property::header("kafka", "Trace-Id"),
                ScalarValue::Text("upper".into()),
            )
            .with_value(
                property::header("kafka", "trace-id"),
                ScalarValue::Text("lower".into()),
            )
            .with_value("MessageType", ScalarValue::Text("Order".into()));
        Message::received(
            MessageId::new(1),
            Vec::new(),
            context,
            MessageTreatment::default(),
        )
    }

    fn promote(properties: &[&str]) -> Result<Promoted, SourceError> {
        Gathering::new(&[&HeaderSource], properties).promote(&message())
    }

    fn read(name: &str) -> Result<Option<String>, SourceError> {
        let property = format!("header:{name}");
        Ok(promote(&[property.as_str()])?
            .get(&property)
            .map(str::to_string))
    }

    #[test]
    fn a_header_reads_whatever_case_the_filter_spells_it_in() {
        let json = Some("application/json".to_string());
        assert_eq!(read("http.content-type").expect("lower"), json);
        assert_eq!(read("http.Content-Type").expect("canonical"), json);
        assert_eq!(read("HTTP.CONTENT-TYPE").expect("upper"), json);
        assert_eq!(
            read("http.Content-Length").expect("number"),
            Some("42".into())
        );
    }

    #[test]
    fn kafka_keeps_two_spellings_apart_where_http_folds_them() {
        assert_eq!(read("kafka.Trace-Id").expect("upper"), Some("upper".into()));
        assert_eq!(read("kafka.trace-id").expect("lower"), Some("lower".into()));
        assert_eq!(read("kafka.TRACE-ID").expect("neither"), None);
        assert_eq!(read("http.CONTENT-type"), read("http.content-type"));
    }

    #[test]
    fn the_protocol_named_is_the_protocol_read() {
        assert_eq!(
            read("amqp.content-type").expect("amqp"),
            Some("text/plain".into())
        );
        assert_eq!(read("kafka.content-type").expect("kafka"), None);
        // The protocol ends at the first dot; the header's name may hold one.
        assert_eq!(
            read("kafka.trace.id").expect("dotted"),
            Some("4bf92f35".into())
        );
    }

    #[test]
    fn a_header_that_was_not_delivered_reads_as_nothing_promoted() {
        assert_eq!(read("http.Authorization").expect("readable"), None);
        assert_eq!(read("http.X-Empty").expect("readable"), None);
        // A plain context key is not a header, whatever its name.
        assert_eq!(read("http.MessageType").expect("readable"), None);
    }

    #[test]
    fn bytes_and_a_name_without_its_protocol_are_refused_with_a_reason() {
        let refused = read("http.X-Bytes").expect_err("bytes");
        assert_eq!(refused.technology, "header");
        assert!(
            refused.reason.contains("http.header.x-bytes holds 2 bytes"),
            "{}",
            refused.reason
        );

        for unnamed in ["", "content-type", ".content-type", "http."] {
            let refused = read(unnamed).expect_err("no protocol");
            assert!(
                refused.reason.contains("its protocol"),
                "{}",
                refused.reason
            );
        }
    }

    #[test]
    fn the_technology_is_header_and_promote_reads_the_prefixed_property() {
        assert_eq!(HeaderSource.technology(), "header");

        let promoted =
            promote(&["header:http.Content-Type", "header:http.Authorization"]).expect("readable");

        assert_eq!(
            promoted.get("header:http.Content-Type"),
            Some("application/json")
        );
        assert_eq!(promoted.get("header:http.Authorization"), None);
        assert!(
            path::expression::Expression::parse("header:http.Content-Type like 'application/%'")
                .expect("compiles")
                .evaluate(&promoted)
                .holds()
        );
        assert_eq!(
            path::expression::Expression::parse("header:http.Authorization = 'x'")
                .expect("compiles")
                .evaluate(&promoted)
                .reason(),
            Some("nothing promoted header:http.Authorization")
        );
    }
}
