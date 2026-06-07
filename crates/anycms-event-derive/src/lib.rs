//! Procedural macros for the anycms-event crate.
//!
//! Provides:
//! - `#[derive(Event)]` — auto-implement the `Event` trait
//! - `event_bus! { ... }` — define a typed event bus with compile-time guarantees
//!
//! # `#[derive(Event)]`
//!
//! Generates an `impl Event for YourStruct` block with `event_name()` and `topic()`.
//!
//! ## Attributes
//!
//! - `#[event(name = "user.created")]` — set the event name explicitly
//! - `#[event(topic = "user")]` — set the topic explicitly
//!
//! If `name` is not specified, it is derived from the struct name using CamelCase convention:
//! `UserCreated` -> `"user.created"`, `OrderPlaced` -> `"order.placed"`.
//!
//! If `topic` is not specified, it defaults to the first segment of the event name
//! (e.g., `"user.created"` -> `"user"`).
//!
//! # `event_bus!`
//!
//! Define event structs, a topic enum, and a typed event bus in one declaration.
//!
//! ## Syntax
//!
//! ```ignore
//! event_bus! {
//!     bus AppEventBus {
//!         event UserCreated { user_id: String, username: String }
//!         event UserDeleted { user_id: String, reason: String }
//!
//!         // topic <method_name> => [EventType1, EventType2]
//!         topic user_events => [UserCreated, UserDeleted]
//!     }
//! }
//! ```
//!
//! This generates:
//! - Struct definitions with `#[derive(Debug, Clone, Serialize, Deserialize)]`
//! - `impl Event for ...` blocks
//! - A topic enum `AppEventBusTopicEvent`
//! - A typed `AppEventBus` newtype wrapping `anycms_event::EventBus`
//! - A `subscribe_topic_user_events()` method for the topic group

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    braced, parse, parse_macro_input, Data, DeriveInput, Expr, ExprLit, Ident, Lit,
    Meta, MetaNameValue, Path, Token, Type,
};

// ---------------------------------------------------------------------------
// CamelCase -> snake_case (dotted) conversion
// ---------------------------------------------------------------------------

/// Convert an UpperCamelCase identifier into a dotted lowercase string.
///
/// Rules:
/// - Each uppercase letter starts a new segment (unless it's part of a run
///   like `HTTPServer` -> `http_server` -> `http.server`).
/// - Segments are joined with `"."`.
/// - The entire result is lowercase.
///
/// Examples:
///   `UserCreated`  -> `"user.created"`
///   `OrderPlaced`  -> `"order.placed"`
///   `HTTPServer`   -> `"http.server"`
fn camel_to_dotted(name: &str) -> String {
    let mut result = String::with_capacity(name.len() + 8);
    let mut chars = name.chars().peekable();

    // Track whether the previous char was lowercase (or digit).
    let mut prev_lower = false;

    while let Some(ch) = chars.next() {
        if ch.is_uppercase() {
            // Insert a segment separator if:
            //   - we are not at the start, AND
            //   - the previous char was lowercase OR the next char is lowercase
            //     (handles "HTTPServer" -> "H-T-T-P-Server" with proper splits)
            let next_lower = chars.peek().map_or(false, |c| c.is_lowercase());
            if prev_lower || next_lower {
                if !result.is_empty() {
                    result.push('.');
                }
            } else if !result.is_empty() {
                // We're in an all-uppercase run (e.g., "HTTP").
                // Don't split — just append the lowercase char.
            } else {
                // first char, no separator
            }
            result.push(ch.to_ascii_lowercase());
            prev_lower = false;
        } else {
            // Lowercase or digit.
            if result.is_empty() {
                // first char, just push
            }
            result.push(ch);
            prev_lower = ch.is_lowercase() || ch.is_ascii_digit();
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Attribute parsing helpers
// ---------------------------------------------------------------------------

/// Parsed `#[event(..)]` attributes.
struct EventAttrs {
    /// Explicit event name, e.g. `#[event(name = "user.created")]`.
    name: Option<String>,
    /// Explicit topic, e.g. `#[event(topic = "user")]`.
    topic: Option<String>,
}

/// Parse all `#[event(...)]` attributes on the struct.
fn parse_event_attrs(attrs: &[syn::Attribute]) -> EventAttrs {
    let mut name = None;
    let mut topic = None;

    for attr in attrs {
        if !attr.path().is_ident("event") {
            continue;
        }

        // Parse the contents as a comma-separated list of `key = "value"`.
        let nested = attr.parse_args_with(
            syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
        );

        if let Ok(metas) = nested {
            for meta in metas {
                if let Meta::NameValue(MetaNameValue {
                    path,
                    value:
                        Expr::Lit(ExprLit {
                            lit: Lit::Str(lit_str),
                            ..
                        }),
                    ..
                }) = meta
                {
                    if path.is_ident("name") {
                        name = Some(lit_str.value());
                    } else if path.is_ident("topic") {
                        topic = Some(lit_str.value());
                    }
                }
            }
        }
    }

    EventAttrs { name, topic }
}

/// Extract the first dotted segment as the default topic.
///
/// `"user.created"` -> `"user"`
/// `"order.placed"` -> `"order"`
/// `"order"`        -> `"order"`
fn first_segment(name: &str) -> &str {
    name.split('.').next().unwrap_or(name)
}

// ---------------------------------------------------------------------------
// The derive macro
// ---------------------------------------------------------------------------

/// Derive macro for the `Event` trait.
///
/// See the crate-level documentation for usage.
#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // Only structs are supported.
    match &input.data {
        Data::Struct(_) => {}
        Data::Enum(_) => {
            return syn::Error::new_spanned(
                &input.ident,
                "Event can only be derived for structs, not enums",
            )
            .to_compile_error()
            .into();
        }
        Data::Union(_) => {
            return syn::Error::new_spanned(
                &input.ident,
                "Event can only be derived for structs, not unions",
            )
            .to_compile_error()
            .into();
        }
    }

    let ident = &input.ident;
    let attrs = parse_event_attrs(&input.attrs);

    // Determine event_name.
    let event_name = attrs.name.unwrap_or_else(|| camel_to_dotted(&ident.to_string()));

    // Determine topic.
    let topic = attrs
        .topic
        .unwrap_or_else(|| first_segment(&event_name).to_owned());

    let expanded = quote! {
        impl ::anycms_event::Event for #ident {
            fn event_name() -> &'static str {
                #event_name
            }

            fn topic() -> &'static str {
                #topic
            }
        }
    };

    expanded.into()
}

// ---------------------------------------------------------------------------
// event_bus! macro — parser
// ---------------------------------------------------------------------------

/// A single `event Name { fields }` declaration inside the macro.
struct EventDecl {
    name: Ident,
    fields: Vec<(Ident, Type)>,
}

/// A single `topic name => [EventType1, EventType2]` declaration.
struct TopicDecl {
    /// User-specified method name suffix (the identifier after `topic`).
    method_name: Ident,
    event_types: Vec<Ident>,
}

/// The full parsed `event_bus!` input.
struct EventBusDef {
    bus_name: Ident,
    events: Vec<EventDecl>,
    topics: Vec<TopicDecl>,
}

/// Helper: peek if the next token is a specific identifier (e.g. "bus", "event", "topic").
fn peek_keyword(input: &parse::ParseBuffer, keyword: &str) -> bool {
    input.peek(Ident)
        && input
            .cursor()
            .ident()
            .map_or(false, |(ident, _)| ident == keyword)
}

/// Helper: parse a specific identifier keyword or error.
fn parse_keyword(input: parse::ParseStream, keyword: &str) -> syn::Result<()> {
    let ident: Ident = input.parse()?;
    if ident != keyword {
        return Err(syn::Error::new(ident.span(), format!("expected `{}`", keyword)));
    }
    Ok(())
}

impl parse::Parse for EventBusDef {
    fn parse(input: parse::ParseStream) -> syn::Result<Self> {
        // Expect `bus Ident { ... }`
        parse_keyword(input, "bus")?;
        let bus_name: Ident = input.parse()?;
        let content;
        braced!(content in input);

        let mut events = Vec::new();
        let mut topics = Vec::new();

        while !content.is_empty() {
            if peek_keyword(&content, "event") {
                // Parse `event Ident { field: Type, ... }`
                parse_keyword(&content, "event")?;
                let name: Ident = content.parse()?;

                let fields_content;
                braced!(fields_content in content);

                let mut fields = Vec::new();
                while !fields_content.is_empty() {
                    let field_name: Ident = fields_content.parse()?;
                    fields_content.parse::<Token![:]>()?;
                    let field_type: Type = fields_content.parse()?;

                    fields.push((field_name, field_type));

                    // Optional trailing comma
                    if fields_content.peek(Token![,]) {
                        fields_content.parse::<Token![,]>()?;
                    }
                }

                events.push(EventDecl { name, fields });
            } else if peek_keyword(&content, "topic") {
                // Parse `topic name => [EventType1, EventType2]`
                parse_keyword(&content, "topic")?;
                let method_name: Ident = content.parse()?;
                content.parse::<Token![=>]>()?;

                let types_content;
                let _bracket = syn::bracketed!(types_content in content);

                let mut event_types = Vec::new();
                while !types_content.is_empty() {
                    let event_type: Path = types_content.parse()?;
                    // Extract the final ident from the path
                    if let Some(segment) = event_type.segments.last() {
                        event_types.push(segment.ident.clone());
                    }
                    // Optional trailing comma
                    if types_content.peek(Token![,]) {
                        types_content.parse::<Token![,]>()?;
                    }
                }

                topics.push(TopicDecl {
                    method_name,
                    event_types,
                });
            } else {
                return Err(content.error("expected `event` or `topic`"));
            }
        }

        Ok(EventBusDef {
            bus_name,
            events,
            topics,
        })
    }
}

// ---------------------------------------------------------------------------
// event_bus! macro — code generation
// ---------------------------------------------------------------------------

/// Define a typed event bus with events and topic groupings.
#[proc_macro]
pub fn event_bus(input: TokenStream) -> TokenStream {
    let def = match syn::parse::<EventBusDef>(input) {
        Ok(d) => d,
        Err(e) => return e.to_compile_error().into(),
    };

    let bus_name = &def.bus_name;
    let enum_name = quote::format_ident!("{}TopicEvent", def.bus_name);

    // ------------------------------------------------------------------
    // 1. Generate event structs + Event impls
    // ------------------------------------------------------------------
    let event_structs: Vec<proc_macro2::TokenStream> = def
        .events
        .iter()
        .map(|event| {
            let name = &event.name;
            let event_name_str = camel_to_dotted(&name.to_string());
            let topic_str = first_segment(&event_name_str).to_owned();

            let fields: Vec<proc_macro2::TokenStream> = event
                .fields
                .iter()
                .map(|(fname, ftype)| {
                    quote! { pub #fname: #ftype }
                })
                .collect();

            quote! {
                #[derive(::std::fmt::Debug, ::std::clone::Clone, ::serde::Serialize, ::serde::Deserialize)]
                pub struct #name {
                    #(#fields),*
                }

                impl ::anycms_event::Event for #name {
                    fn event_name() -> &'static str {
                        #event_name_str
                    }

                    fn topic() -> &'static str {
                        #topic_str
                    }
                }
            }
        })
        .collect();

    // ------------------------------------------------------------------
    // 2. Generate the topic enum (only if there are topics)
    // ------------------------------------------------------------------
    let topic_enum = if def.topics.is_empty() {
        quote! {}
    } else {
        let variants: Vec<proc_macro2::TokenStream> = def
            .topics
            .iter()
            .flat_map(|topic| &topic.event_types)
            .map(|event_type| {
                quote! { #event_type(#event_type) }
            })
            .collect();

        // Only generate the enum if we have variants
        if variants.is_empty() {
            quote! {}
        } else {
            quote! {
                #[derive(::std::fmt::Debug, ::std::clone::Clone, ::serde::Serialize, ::serde::Deserialize)]
                #[serde(tag = "event_type")]
                pub enum #enum_name {
                    #(#variants),*
                }
            }
        }
    };

    // ------------------------------------------------------------------
    // 3. Generate per-topic subscribe methods
    // ------------------------------------------------------------------
    let topic_subscribe_methods: Vec<proc_macro2::TokenStream> = if def.topics.is_empty() || def.topics.iter().all(|t| t.event_types.is_empty()) {
        Vec::new()
    } else {
        def.topics
            .iter()
            .map(|topic| {
                let method_name = quote::format_ident!("subscribe_topic_{}", topic.method_name);

                let subscribe_arms: Vec<proc_macro2::TokenStream> = topic
                    .event_types
                    .iter()
                    .map(|event_type| {
                        let variant = event_type;
                        quote! {
                            {
                                let h = handler.clone();
                                self.inner.subscribe::<#variant, _, _>(move |e| {
                                    let h = h.clone();
                                    async move { h(#enum_name::#variant(e)).await }
                                }).await
                            }
                        }
                    })
                    .collect();

                quote! {
                    pub async fn #method_name<F, Fut>(&self, handler: F) -> ::std::vec::Vec<::anycms_event::Result<::anycms_event::bus::Subscription>>
                    where
                        F: Fn(#enum_name) -> Fut + ::std::clone::Clone + ::std::marker::Send + ::std::marker::Sync + 'static,
                        Fut: ::std::future::Future<Output = ::anycms_event::Result<()>> + ::std::marker::Send + 'static,
                    {
                        let mut subs = ::std::vec::Vec::new();
                        #(
                            subs.push(#subscribe_arms);
                        )*
                        subs
                    }
                }
            })
            .collect()
    };

    // ------------------------------------------------------------------
    // 4. Generate the typed EventBus newtype
    // ------------------------------------------------------------------
    let bus_impl = quote! {
        pub struct #bus_name {
            inner: ::anycms_event::EventBus,
        }

        impl #bus_name {
            pub fn new() -> Self {
                Self {
                    inner: ::anycms_event::EventBus::new(),
                }
            }

            /// Get a reference to the underlying [`::anycms_event::EventBus`].
            pub fn inner(&self) -> &::anycms_event::EventBus {
                &self.inner
            }

            /// Consume this typed bus and return the underlying [`::anycms_event::EventBus`].
            pub fn into_inner(self) -> ::anycms_event::EventBus {
                self.inner
            }

            pub async fn publish<E: ::anycms_event::Event>(&self, event: E) -> ::anycms_event::Result<()> {
                self.inner.publish(event).await
            }

            pub async fn subscribe<E, F, Fut>(&self, handler: F) -> ::anycms_event::Result<::anycms_event::bus::Subscription>
            where
                E: ::anycms_event::Event,
                F: Fn(E) -> Fut + ::std::marker::Send + ::std::marker::Sync + 'static,
                Fut: ::std::future::Future<Output = ::anycms_event::Result<()>> + ::std::marker::Send + 'static,
            {
                self.inner.subscribe::<E, F, Fut>(handler).await
            }

            #(#topic_subscribe_methods)*
        }

        impl ::std::clone::Clone for #bus_name {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                }
            }
        }

        impl ::std::default::Default for #bus_name {
            fn default() -> Self {
                Self::new()
            }
        }
    };

    // ------------------------------------------------------------------
    // Assemble everything
    // ------------------------------------------------------------------
    let expanded = quote! {
        #(#event_structs)*
        #topic_enum
        #bus_impl
    };

    expanded.into()
}

// ---------------------------------------------------------------------------
// Tests (run with `cargo test -p anycms-event-derive`)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_camel_to_dotted_simple() {
        assert_eq!(camel_to_dotted("UserCreated"), "user.created");
    }

    #[test]
    fn test_camel_to_dotted_three_words() {
        assert_eq!(camel_to_dotted("UserProfileUpdated"), "user.profile.updated");
    }

    #[test]
    fn test_camel_to_dotted_single_word() {
        assert_eq!(camel_to_dotted("Order"), "order");
    }

    #[test]
    fn test_camel_to_dotted_acronym() {
        assert_eq!(camel_to_dotted("HTTPServer"), "http.server");
    }

    #[test]
    fn test_camel_to_dotted_order_placed() {
        assert_eq!(camel_to_dotted("OrderPlaced"), "order.placed");
    }

    #[test]
    fn test_first_segment_simple() {
        assert_eq!(first_segment("user.created"), "user");
    }

    #[test]
    fn test_first_segment_single() {
        assert_eq!(first_segment("order"), "order");
    }
}
