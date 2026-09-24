#![forbid(unsafe_code)]

//! The header route technology — a technology of `xmip-core-route`.
//!
//! A Subscription's filter names properties, and each property is read from
//! one source. This source reads what the transport delivered beside the
//! bytes: `header:<name>` is the header called `<name>`, whatever its case —
//! `header:content-type` and `header:Content-Type` are one property, because
//! HTTP, mail and every protocol that has headers say so. ADR-0046.
//!
//! **The convention this crate defines.** A header reaches the context under
//! the key `header.<name>`, the name in lower case: `header.content-type`,
//! `header.x-request-id`. Nothing in the estate wrote headers into the context
//! before this technology existed — the arrival carries them as transport
//! properties, which `xmip-core-identify` reads under `http.header.<name>`,
//! and those never reach a Message. So the reading here names the keys the
//! runtime's default promotion is to write, and this paragraph is the record
//! of that. A header value reads through `route::routable`, as every context
//! value a filter names does: as the text it was, a `Null` absent and bytes
//! refused (ADR-0046, amended 2026-09-24).
//!
//! A route technology does not decide anything: it reads.

use message::Message;
use route::{Source, SourceError};

/// The manifest leaf and the prefix a property carries.
pub const TECHNOLOGY: &str = "header";

/// The context key prefix a header is stored under, followed by the header's
/// name in lower case.
pub const KEY_PREFIX: &str = "header.";

/// The context key a header of this name is stored under.
#[must_use]
pub fn key_of(name: &str) -> String {
    format!("{KEY_PREFIX}{}", name.to_ascii_lowercase())
}

/// Reads `header:<name>` from the `header.<name>` context keys.
pub struct HeaderSource;

impl Source for HeaderSource {
    fn technology(&self) -> &'static str {
        TECHNOLOGY
    }

    fn read(&self, message: &Message, name: &str) -> Result<Option<String>, SourceError> {
        if name.is_empty() {
            return Err(SourceError::new(
                TECHNOLOGY,
                name,
                "a header name is needed after the prefix",
            ));
        }

        let wanted = key_of(name);
        let found = message
            .context()
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(&wanted))
            .map(|(_, value)| value);

        route::routable(&wanted, found).map_err(|reason| SourceError::new(TECHNOLOGY, name, reason))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use context::{ContextValue, MessageContext};
    use message::MessageTreatment;
    use route::{Predicate, Value};
    use xcore::MessageId;

    fn message() -> Message {
        let context = MessageContext::new()
            .with_value(
                key_of("Content-Type"),
                ContextValue::Text("application/json".into()),
            )
            .with_value(key_of("Content-Length"), ContextValue::Integer(42))
            .with_value(key_of("X-Empty"), ContextValue::Null)
            .with_value(key_of("X-Bytes"), ContextValue::Binary(vec![1, 2]))
            .with_value("MessageType", ContextValue::Text("Order".into()));
        Message::received(
            MessageId::new(1),
            Vec::new(),
            context,
            MessageTreatment::default(),
        )
    }

    fn read(name: &str) -> Result<Option<String>, SourceError> {
        HeaderSource.read(&message(), name)
    }

    #[test]
    fn a_header_is_stored_under_its_lower_case_name() {
        assert_eq!(key_of("Content-Type"), "header.content-type");
        assert_eq!(key_of("x-request-id"), "header.x-request-id");
    }

    #[test]
    fn a_header_reads_whatever_case_the_filter_spells_it_in() {
        let json = Some("application/json".to_string());
        assert_eq!(read("content-type").expect("lower"), json);
        assert_eq!(read("Content-Type").expect("canonical"), json);
        assert_eq!(read("CONTENT-TYPE").expect("upper"), json);
        assert_eq!(read("Content-Length").expect("number"), Some("42".into()));
    }

    #[test]
    fn a_header_that_was_not_delivered_reads_as_nothing_promoted() {
        assert_eq!(read("Authorization").expect("readable"), None);
        assert_eq!(read("X-Empty").expect("readable"), None);
        // A plain context key is not a header, whatever its name.
        assert_eq!(read("MessageType").expect("readable"), None);
    }

    #[test]
    fn bytes_and_an_empty_name_are_refused_with_a_reason() {
        let refused = read("X-Bytes").expect_err("bytes");
        assert_eq!(refused.technology, "header");
        assert!(refused.reason.contains("header.x-bytes holds 2 bytes"));

        let empty = read("").expect_err("no name");
        assert!(empty.reason.contains("header name"));
    }

    #[test]
    fn the_technology_is_header_and_promote_reads_the_prefixed_property() {
        assert_eq!(HeaderSource.technology(), "header");

        let sources: [&dyn Source; 1] = [&HeaderSource];
        let promoted = route::promote(
            &message(),
            &sources,
            &["header:Content-Type", "header:Authorization"],
        )
        .expect("readable");

        assert_eq!(
            promoted.get("header:Content-Type"),
            Some("application/json")
        );
        assert_eq!(promoted.get("header:Authorization"), None);
        assert!(
            Predicate::starts_with("header:Content-Type", "application/")
                .test(&promoted)
                .passed()
        );
        assert_eq!(
            Predicate::equals("header:Authorization", Value::Text("x".into()))
                .test(&promoted)
                .reason(),
            Some("nothing promoted header:Authorization")
        );
    }
}
