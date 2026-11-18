//! [`axum::extract::ws`] with type safe messages.
//!
//! # Example
//!
//! ```rust
//! use axum::{
//!     Router,
//!     response::IntoResponse,
//!     routing::get,
//! };
//! use axum_typed_websockets::{Message, WebSocket, WebSocketUpgrade};
//! use serde::{Serialize, Deserialize};
//! use std::time::Instant;
//!
//! // Make a regular axum router
//! let app = Router::new().route("/ws", get(handler));
//!
//! # async {
//! // Run it!
//! axum::serve(
//!     tokio::net::TcpListener::bind("0.0.0.0:3000")
//!         .await
//!         .unwrap(),
//!     app.into_make_service()
//! )
//! .await
//! .unwrap();
//! # };
//!
//! async fn handler(
//!     // Upgrade the request to a WebSocket connection where the server sends
//!     // messages of type `ServerMsg` and the clients sends `ClientMsg`
//!     ws: WebSocketUpgrade<ServerMsg, ClientMsg>,
//! ) -> impl IntoResponse {
//!     ws.on_upgrade(ping_pong_socket)
//! }
//!
//! // Send a ping and measure how long time it takes to get a pong back
//! async fn ping_pong_socket(mut socket: WebSocket<ServerMsg, ClientMsg>) {
//!     let start = Instant::now();
//!     socket.send(Message::Item(ServerMsg::Ping)).await.ok();
//!
//!     if let Some(msg) = socket.recv().await {
//!         match msg {
//!             Ok(Message::Item(ClientMsg::Pong)) => {
//!                 println!("ping: {:?}", start.elapsed());
//!             },
//!             Ok(_) => {},
//!             Err(err) => {
//!                 eprintln!("got error: {}", err);
//!             },
//!         }
//!     }
//! }
//!
//! #[derive(Debug, Serialize)]
//! enum ServerMsg {
//!     Ping,
//! }
//!
//! #[derive(Debug, Deserialize)]
//! enum ClientMsg {
//!     Pong,
//! }
//! ```
//!
//! # Feature flags
//!
//! The following features are available:
//!
//! - `json`: Enables [`TextJsonCodec`] and [`BinaryJsonCodec`] which encodes message as JSON using
//!   `serde_json`. Enabled by default.
//! - `msgpack`: Enables [`MessagePackCodec`] (compact, structs as arrays) and
//!   [`MessagePackNamedCodec`] (structs as maps with field names) using `rmp-serde`.
//! - `bitcode`: Enables [`BitcodeCodec`] which encodes message as bitcode using `bitcode`.
//! - `postcard`: Enables [`PostcardCodec`] which encodes message as postcard using `postcard`.
//! - `cbor`: Enables [`CborCodec`] which encodes message as CBOR using `ciborium`.
//! - `flexbuffers`: Enables [`FlexbuffersCodec`] which encodes message as FlexBuffers using
//!   `flexbuffers`.
//! - `bson`: Enables [`BsonCodec`] which encodes message as BSON using `bson`. Messages must
//!   serialize to a document (a struct or map).
//! - `ron`: Enables [`RonCodec`] which encodes message as RON text using `ron`.
//! - `xml`: Enables [`XmlCodec`] which encodes message as XML text using `quick-xml`.
//!   Messages must serialize to an element (e.g. a struct).

#![warn(
    clippy::all,
    clippy::dbg_macro,
    clippy::todo,
    clippy::empty_enums,
    clippy::enum_glob_use,
    clippy::mem_forget,
    clippy::unused_self,
    clippy::filter_map_next,
    clippy::needless_continue,
    clippy::needless_borrow,
    clippy::match_wildcard_for_single_variants,
    clippy::if_let_mutex,
    clippy::await_holding_lock,
    clippy::imprecise_flops,
    clippy::suboptimal_flops,
    clippy::lossy_float_literal,
    clippy::rest_pat_in_fully_bound_structs,
    clippy::fn_params_excessive_bools,
    clippy::exit,
    clippy::inefficient_to_string,
    clippy::linkedlist,
    clippy::macro_use_imports,
    clippy::option_option,
    clippy::verbose_file_reads,
    clippy::unnested_or_patterns,
    rust_2018_idioms,
    future_incompatible,
    nonstandard_style,
    missing_debug_implementations,
    missing_docs
)]
#![deny(unreachable_pub, private_interfaces, private_bounds)]
#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

use axum::{
    body::Bytes,
    extract::{FromRequestParts, ws},
    http::request::Parts,
    response::IntoResponse,
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    error::Error as StdError,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

macro_rules! with_and_without_json {
    (
        $(#[$m:meta])*
        pub struct $name:ident<S, R, C = TextJsonCodec> {
            $(
                $ident:ident : $ty:ty,
            )*
        }
    ) => {
        $(#[$m])*
        #[cfg(feature = "json")]
        pub struct $name<S, R, C = TextJsonCodec> {
            $(
                $ident : $ty,
            )*
        }

        $(#[$m])*
        #[cfg(not(feature = "json"))]
        pub struct $name<S, R, C> {
            $(
                $ident : $ty,
            )*
        }
    }
}

with_and_without_json! {
    /// A version of [`axum::extract::ws::WebSocketUpgrade`] with type safe
    /// messages.
    ///
    /// # Type parameters
    ///
    /// - `S` - The message sent from the server to the client.
    /// - `R` - The message sent from the client to the server.
    /// - `C` - The [`Codec`] used to encode and decode messages. Defaults to
    /// [`TextJsonCodec`] which serializes messages with `serde_json`.
    pub struct WebSocketUpgrade<S, R, C = TextJsonCodec> {
        upgrade: ws::WebSocketUpgrade,
        _marker: PhantomData<fn() -> (S, R, C)>,
    }
}

impl<S, R, C, B> FromRequestParts<B> for WebSocketUpgrade<S, R, C>
where
    B: Send + Sync,
{
    type Rejection = <ws::WebSocketUpgrade as FromRequestParts<B>>::Rejection;

    async fn from_request_parts(parts: &mut Parts, state: &B) -> Result<Self, Self::Rejection> {
        let upgrade = ws::WebSocketUpgrade::from_request_parts(parts, state).await?;
        Ok(Self {
            upgrade,
            _marker: PhantomData,
        })
    }
}

impl<S, R, C> WebSocketUpgrade<S, R, C> {
    /// Finalize upgrading the connection and call the provided callback with
    /// the stream.
    ///
    /// This is analogous to [`axum::extract::ws::WebSocketUpgrade::on_upgrade`].
    pub fn on_upgrade<F, Fut>(self, callback: F) -> impl IntoResponse
    where
        F: FnOnce(WebSocket<S, R, C>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
        S: Send,
        R: Send,
    {
        self.upgrade
            .on_upgrade(|socket| async move {
                let socket = WebSocket {
                    socket,
                    _marker: PhantomData,
                };
                callback(socket).await
            })
            .into_response()
    }

    /// Apply a transformation to the inner [`axum::extract::ws::WebSocketUpgrade`].
    ///
    /// This can be used to apply configuration.
    pub fn map<F>(mut self, f: F) -> Self
    where
        F: FnOnce(ws::WebSocketUpgrade) -> ws::WebSocketUpgrade,
    {
        self.upgrade = f(self.upgrade);
        self
    }

    /// Get the inner axum [`axum::extract::ws::WebSocketUpgrade`].
    pub fn into_inner(self) -> ws::WebSocketUpgrade {
        self.upgrade
    }
}

impl<S, R, C> fmt::Debug for WebSocketUpgrade<S, R, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketUpgrade")
            .field("upgrade", &self.upgrade)
            .finish()
    }
}

with_and_without_json! {
    /// A version of [`axum::extract::ws::WebSocket`] with type safe
    /// messages.
    pub struct WebSocket<S, R, C = TextJsonCodec> {
        socket: ws::WebSocket,
        _marker: PhantomData<fn() -> (S, R, C)>,
    }
}

impl<S, R, C> WebSocket<S, R, C> {
    /// Receive another message.
    ///
    /// Returns `None` if the stream has closed.
    ///
    /// This is analogous to [`axum::extract::ws::WebSocket::recv`] but with a
    /// statically typed message.
    pub async fn recv(&mut self) -> Option<Result<Message<R>, Error<C::DecodeError>>>
    where
        R: DeserializeOwned,
        C: Codec,
    {
        self.next().await
    }

    /// Send a message.
    ///
    /// This is analogous to [`axum::extract::ws::WebSocket::send`] but with a
    /// statically typed message.
    pub async fn send(&mut self, msg: Message<S>) -> Result<(), Error<C::EncodeError>>
    where
        S: Serialize,
        C: Codec,
    {
        SinkExt::send(self, msg).await
    }

    /// Gracefully close this WebSocket.
    ///
    /// This is analogous to calling [`SinkExt::close`] on the inner
    /// [`axum::extract::ws::WebSocket`].
    pub async fn close(mut self) -> Result<(), Error<()>>
    where
        C: Codec,
    {
        self.socket.close().await.map_err(Error::Ws)
    }

    /// Return the selected WebSocket subprotocol, if one has been chosen.
    ///
    /// This is analogous to [`axum::extract::ws::WebSocket::protocol`].
    pub fn protocol(&self) -> Option<&axum::http::HeaderValue> {
        self.socket.protocol()
    }

    /// Get the inner axum [`axum::extract::ws::WebSocket`].
    pub fn into_inner(self) -> ws::WebSocket {
        self.socket
    }
}

impl<S, R, C> fmt::Debug for WebSocket<S, R, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocket")
            .field("socket", &self.socket)
            .finish()
    }
}

impl<S, R, C> Stream for WebSocket<S, R, C>
where
    R: DeserializeOwned,
    C: Codec,
{
    type Item = Result<Message<R>, Error<C::DecodeError>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let msg = futures_util::ready!(
            Pin::new(&mut self.socket)
                .poll_next(cx)
                .map_err(Error::Ws)?
        );

        if let Some(msg) = msg {
            let msg = match msg {
                ws::Message::Text(msg) => TextOrBinary::Text(msg),
                ws::Message::Binary(bytes) => TextOrBinary::Binary(bytes),
                ws::Message::Close(frame) => {
                    return Poll::Ready(Some(Ok(Message::Close(frame))));
                }
                ws::Message::Ping(buf) => {
                    return Poll::Ready(Some(Ok(Message::Ping(buf))));
                }
                ws::Message::Pong(buf) => {
                    return Poll::Ready(Some(Ok(Message::Pong(buf))));
                }
            };

            let msg = C::decode(msg).map(Message::Item).map_err(Error::Codec);
            Poll::Ready(Some(msg))
        } else {
            Poll::Ready(None)
        }
    }
}

impl<S, R, C> Sink<Message<S>> for WebSocket<S, R, C>
where
    S: Serialize,
    C: Codec,
{
    type Error = Error<C::EncodeError>;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.socket).poll_ready(cx).map_err(Error::Ws)
    }

    fn start_send(mut self: Pin<&mut Self>, msg: Message<S>) -> Result<(), Self::Error> {
        let msg = match msg {
            Message::Item(buf) => C::encode(buf).map_err(Error::Codec)?.into(),
            Message::Ping(buf) => ws::Message::Ping(buf),
            Message::Pong(buf) => ws::Message::Pong(buf),
            Message::Close(frame) => ws::Message::Close(frame),
        };

        Pin::new(&mut self.socket)
            .start_send(msg)
            .map_err(Error::Ws)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.socket).poll_flush(cx).map_err(Error::Ws)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.socket).poll_close(cx).map_err(Error::Ws)
    }
}

/// Specifies if the message should be encoded/decoded as text or binary for transmission over the wire
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextOrBinary {
    /// Message should be transmitted as text
    Text(ws::Utf8Bytes),
    /// Message should be transmitted as Binary
    Binary(Bytes),
}

impl From<TextOrBinary> for ws::Message {
    fn from(value: TextOrBinary) -> Self {
        match value {
            TextOrBinary::Text(txt) => ws::Message::Text(txt),
            TextOrBinary::Binary(bin) => ws::Message::Binary(bin),
        }
    }
}

/// Trait for encoding and decoding WebSocket messages.
///
/// This allows you to customize how messages are encoded when sent over the
/// wire.
pub trait Codec {
    /// The errors that can happen when encoding using this codec.
    type EncodeError;
    /// The errors that can happen when decoding using this codec.
    type DecodeError;

    /// Encode a message.
    fn encode<S>(msg: S) -> Result<TextOrBinary, Self::EncodeError>
    where
        S: Serialize;

    /// Decode a message.
    fn decode<R>(msg: TextOrBinary) -> Result<R, Self::DecodeError>
    where
        R: DeserializeOwned;
}

/// A [`Codec`] that serializes messages as JSON using `serde_json` and transmits it as text.
/// Note that receiving messages works as both binary or text
#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
#[derive(Debug)]
#[non_exhaustive]
pub struct TextJsonCodec;

#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
impl Codec for TextJsonCodec {
    type EncodeError = serde_json::Error;
    type DecodeError = serde_json::Error;

    fn encode<S>(msg: S) -> Result<TextOrBinary, Self::EncodeError>
    where
        S: Serialize,
    {
        serde_json::to_string(&msg).map(|txt| TextOrBinary::Text(txt.into()))
    }

    fn decode<R>(msg: TextOrBinary) -> Result<R, Self::DecodeError>
    where
        R: DeserializeOwned,
    {
        match msg {
            TextOrBinary::Text(txt) => serde_json::from_str(&txt),
            TextOrBinary::Binary(bin) => serde_json::from_slice(&bin),
        }
    }
}

/// A [`Codec`] that serializes messages as JSON using `serde_json` and transmits it as binary.
/// Note that receiving messages works as both binary or text
#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
#[derive(Debug)]
#[non_exhaustive]
pub struct BinaryJsonCodec;

#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
impl Codec for BinaryJsonCodec {
    type EncodeError = serde_json::Error;
    type DecodeError = serde_json::Error;

    fn encode<S>(msg: S) -> Result<TextOrBinary, Self::EncodeError>
    where
        S: Serialize,
    {
        serde_json::to_vec(&msg).map(|bin| TextOrBinary::Binary(bin.into()))
    }

    fn decode<R>(msg: TextOrBinary) -> Result<R, Self::DecodeError>
    where
        R: DeserializeOwned,
    {
        match msg {
            TextOrBinary::Text(txt) => serde_json::from_str(&txt),
            TextOrBinary::Binary(bin) => serde_json::from_slice(&bin),
        }
    }
}

/// Defines a [`Codec`] that transmits binary and decodes both text and binary frames as bytes.
#[allow(
    unused_macros,
    reason = "unused when no binary codec feature is enabled"
)]
macro_rules! binary_codec {
    (
        $(#[$doc:meta])*
        $feature:literal, $name:ident,
        encode($msg:ident) -> $enc_err:ty $encode:block,
        decode($bytes:ident) -> $dec_err:ty $decode:block
    ) => {
        $(#[$doc])*
        #[cfg(feature = $feature)]
        #[cfg_attr(docsrs, doc(cfg(feature = $feature)))]
        #[derive(Debug)]
        #[non_exhaustive]
        pub struct $name;

        #[cfg(feature = $feature)]
        #[cfg_attr(docsrs, doc(cfg(feature = $feature)))]
        impl Codec for $name {
            type EncodeError = $enc_err;
            type DecodeError = $dec_err;

            fn encode<S>($msg: S) -> Result<TextOrBinary, Self::EncodeError>
            where
                S: Serialize,
            {
                let bin: Vec<u8> = $encode?;
                Ok(TextOrBinary::Binary(bin.into()))
            }

            fn decode<R>(msg: TextOrBinary) -> Result<R, Self::DecodeError>
            where
                R: DeserializeOwned,
            {
                let $bytes: &[u8] = match &msg {
                    TextOrBinary::Text(txt) => txt.as_bytes(),
                    TextOrBinary::Binary(bin) => bin,
                };
                $decode
            }
        }
    };
}

binary_codec! {
    /// A [`Codec`] that serializes messages as compact MessagePack using `rmp_serde` and
    /// transmits it as binary.
    ///
    /// Structs are encoded as arrays (no field names). Use [`MessagePackNamedCodec`] to include
    /// field names.
    "msgpack", MessagePackCodec,
    encode(msg) -> rmp_serde::encode::Error { rmp_serde::encode::to_vec(&msg) },
    decode(bytes) -> rmp_serde::decode::Error { rmp_serde::decode::from_slice(bytes) }
}

binary_codec! {
    /// A [`Codec`] that serializes messages as MessagePack using `rmp_serde` and transmits it
    /// as binary.
    ///
    /// Structs are encoded as maps keyed by field name, which is larger than
    /// [`MessagePackCodec`] but tolerant of field reordering.
    "msgpack", MessagePackNamedCodec,
    encode(msg) -> rmp_serde::encode::Error { rmp_serde::encode::to_vec_named(&msg) },
    decode(bytes) -> rmp_serde::decode::Error { rmp_serde::decode::from_slice(bytes) }
}

binary_codec! {
    /// A [`Codec`] that serializes messages with `bitcode` and transmits it as binary.
    "bitcode", BitcodeCodec,
    encode(msg) -> bitcode::Error { bitcode::serialize(&msg) },
    decode(bytes) -> bitcode::Error { bitcode::deserialize(bytes) }
}

binary_codec! {
    /// A [`Codec`] that serializes messages with `postcard` and transmits it as binary.
    "postcard", PostcardCodec,
    encode(msg) -> postcard::Error { postcard::to_allocvec(&msg) },
    decode(bytes) -> postcard::Error { postcard::from_bytes(bytes) }
}

binary_codec! {
    /// A [`Codec`] that serializes messages as CBOR using `ciborium` and transmits it as binary.
    "cbor", CborCodec,
    encode(msg) -> ciborium::ser::Error<std::io::Error> {
        let mut bin = Vec::new();
        ciborium::into_writer(&msg, &mut bin).map(|()| bin)
    },
    decode(bytes) -> ciborium::de::Error<std::io::Error> { ciborium::from_reader(bytes) }
}

binary_codec! {
    /// A [`Codec`] that serializes messages as FlexBuffers using `flexbuffers` and transmits it
    /// as binary.
    "flexbuffers", FlexbuffersCodec,
    encode(msg) -> flexbuffers::SerializationError { flexbuffers::to_vec(&msg) },
    decode(bytes) -> flexbuffers::DeserializationError { flexbuffers::from_slice(bytes) }
}

binary_codec! {
    /// A [`Codec`] that serializes messages as BSON using `bson` and transmits it as binary.
    ///
    /// Messages must serialize to a BSON document (a struct or map); anything else fails to
    /// encode.
    "bson", BsonCodec,
    encode(msg) -> bson::error::Error { bson::serialize_to_vec(&msg) },
    decode(bytes) -> bson::error::Error { bson::deserialize_from_slice(bytes) }
}

/// A [`Codec`] that serializes messages as RON using `ron` and transmits it as text.
/// Note that receiving messages works as both binary or text
#[cfg(feature = "ron")]
#[cfg_attr(docsrs, doc(cfg(feature = "ron")))]
#[derive(Debug)]
#[non_exhaustive]
pub struct RonCodec;

#[cfg(feature = "ron")]
#[cfg_attr(docsrs, doc(cfg(feature = "ron")))]
impl Codec for RonCodec {
    type EncodeError = ron::Error;
    type DecodeError = ron::error::SpannedError;

    fn encode<S>(msg: S) -> Result<TextOrBinary, Self::EncodeError>
    where
        S: Serialize,
    {
        ron::to_string(&msg).map(|txt| TextOrBinary::Text(txt.into()))
    }

    fn decode<R>(msg: TextOrBinary) -> Result<R, Self::DecodeError>
    where
        R: DeserializeOwned,
    {
        match msg {
            TextOrBinary::Text(txt) => ron::from_str(&txt),
            TextOrBinary::Binary(bin) => ron::de::from_bytes(&bin),
        }
    }
}

/// A [`Codec`] that serializes messages as XML using `quick_xml` and transmits it as text.
/// Note that receiving messages works as both binary or text
///
/// Messages must serialize to an XML element (e.g. a struct); the root element is named after
/// the type.
#[cfg(feature = "xml")]
#[cfg_attr(docsrs, doc(cfg(feature = "xml")))]
#[derive(Debug)]
#[non_exhaustive]
pub struct XmlCodec;

#[cfg(feature = "xml")]
#[cfg_attr(docsrs, doc(cfg(feature = "xml")))]
impl Codec for XmlCodec {
    type EncodeError = quick_xml::SeError;
    type DecodeError = quick_xml::DeError;

    fn encode<S>(msg: S) -> Result<TextOrBinary, Self::EncodeError>
    where
        S: Serialize,
    {
        quick_xml::se::to_string(&msg).map(|txt| TextOrBinary::Text(txt.into()))
    }

    fn decode<R>(msg: TextOrBinary) -> Result<R, Self::DecodeError>
    where
        R: DeserializeOwned,
    {
        match msg {
            TextOrBinary::Text(txt) => quick_xml::de::from_str(&txt),
            TextOrBinary::Binary(bin) => quick_xml::de::from_reader(&bin[..]),
        }
    }
}

/// Errors that can happen when using this library.
#[derive(Debug)]
pub enum Error<E> {
    /// Something went wrong with the WebSocket.
    Ws(axum::Error),
    /// Something went wrong with the [`Codec`].
    Codec(E),
}

impl<E> fmt::Display for Error<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Ws(inner) => inner.fmt(f),
            Error::Codec(inner) => inner.fmt(f),
        }
    }
}

impl<E> StdError for Error<E>
where
    E: StdError + 'static,
{
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::Ws(inner) => Some(inner),
            Error::Codec(inner) => Some(inner),
        }
    }
}

/// A WebSocket message containing a value of a known type.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Message<T> {
    /// An item of type `T`.
    Item(T),
    /// A ping message with the specified payload
    ///
    /// The payload here must be at most 125 bytes long
    Ping(Bytes),
    /// A pong message with the specified payload
    ///
    /// The payload here must be at most 125 bytes long
    Pong(Bytes),
    /// A close message with the optional close frame.
    Close(Option<ws::CloseFrame>),
}
