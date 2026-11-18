#![cfg(feature = "json")]

use axum::{Router, body::Bytes, response::IntoResponse, routing::get};
use axum_typed_websockets::{BinaryJsonCodec, Codec, Error, Message, WebSocket, WebSocketUpgrade};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message as TMessage,
};

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Ping {
    n: u32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Pong {
    n: u32,
}

async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn connect(addr: SocketAddr) -> Client {
    connect_async(format!("ws://{addr}/ws")).await.unwrap().0
}

/// Replies to every `Ping { n }` with `Pong { n + 1 }`; reports codec errors as a text frame.
async fn pong_loop<C>(mut socket: WebSocket<Pong, Ping, C>)
where
    C: Codec,
{
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(Message::Item(Ping { n })) => {
                if socket.send(Message::Item(Pong { n: n + 1 })).await.is_err() {
                    return;
                }
            }
            Err(Error::Codec(_)) => {
                let mut raw = socket.into_inner();
                raw.send(axum::extract::ws::Message::Text("codec error".into()))
                    .await
                    .ok();
                return;
            }
            Ok(_) => {}
            Err(Error::Ws(_)) => return,
        }
    }
}

#[tokio::test]
async fn text_json_codec_replies_with_text_frame() {
    async fn handler(ws: WebSocketUpgrade<Pong, Ping>) -> impl IntoResponse {
        ws.on_upgrade(pong_loop)
    }
    let mut client = connect(serve(Router::new().route("/ws", get(handler))).await).await;

    client.send(TMessage::text(r#"{"n":1}"#)).await.unwrap();

    match client.next().await.unwrap().unwrap() {
        TMessage::Text(txt) => {
            assert_eq!(serde_json::from_str::<Pong>(&txt).unwrap(), Pong { n: 2 });
        }
        other => panic!("expected text frame, got {other:?}"),
    }
}

#[tokio::test]
async fn binary_json_codec_replies_with_binary_frame_and_accepts_text() {
    async fn handler(ws: WebSocketUpgrade<Pong, Ping, BinaryJsonCodec>) -> impl IntoResponse {
        ws.on_upgrade(pong_loop)
    }
    let mut client = connect(serve(Router::new().route("/ws", get(handler))).await).await;

    // Text input must still decode for the binary JSON codec.
    client.send(TMessage::text(r#"{"n":41}"#)).await.unwrap();

    match client.next().await.unwrap().unwrap() {
        TMessage::Binary(bin) => {
            assert_eq!(
                serde_json::from_slice::<Pong>(&bin).unwrap(),
                Pong { n: 42 }
            );
        }
        other => panic!("expected binary frame, got {other:?}"),
    }
}

#[tokio::test]
async fn undecodable_message_surfaces_codec_error() {
    async fn handler(ws: WebSocketUpgrade<Pong, Ping>) -> impl IntoResponse {
        ws.on_upgrade(pong_loop)
    }
    let mut client = connect(serve(Router::new().route("/ws", get(handler))).await).await;

    client.send(TMessage::text("not json")).await.unwrap();

    assert_eq!(
        client.next().await.unwrap().unwrap(),
        TMessage::text("codec error")
    );
}

#[tokio::test]
async fn ping_payload_is_forwarded_untouched() {
    async fn handler(ws: WebSocketUpgrade<Pong, Ping>) -> impl IntoResponse {
        ws.on_upgrade(|mut socket: WebSocket<Pong, Ping>| async move {
            socket
                .send(Message::Ping(Bytes::from_static(b"hello")))
                .await
                .unwrap();
        })
    }
    let mut client = connect(serve(Router::new().route("/ws", get(handler))).await).await;

    match client.next().await.unwrap().unwrap() {
        TMessage::Ping(payload) => assert_eq!(&payload[..], b"hello"),
        other => panic!("expected ping frame, got {other:?}"),
    }
}

#[tokio::test]
async fn close_sends_close_frame_and_error_propagates_as_std_error() {
    async fn close_boxed(socket: WebSocket<Pong, Ping>) -> Result<(), Box<dyn std::error::Error>> {
        socket.close().await?;
        Ok(())
    }

    async fn handler(ws: WebSocketUpgrade<Pong, Ping>) -> impl IntoResponse {
        ws.on_upgrade(|socket: WebSocket<Pong, Ping>| async move {
            let _ = close_boxed(socket).await.map_err(|err| err.to_string());
        })
    }
    let mut client = connect(serve(Router::new().route("/ws", get(handler))).await).await;

    match client.next().await.unwrap().unwrap() {
        TMessage::Close(_) => {}
        other => panic!("expected close frame, got {other:?}"),
    }
}

#[tokio::test]
async fn negotiated_subprotocol_is_exposed_on_socket() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    async fn handler(ws: WebSocketUpgrade<String, Ping>) -> impl IntoResponse {
        ws.map(|upgrade| upgrade.protocols(["v2"])).on_upgrade(
            |mut socket: WebSocket<String, Ping>| async move {
                let protocol = socket
                    .protocol()
                    .map(|p| p.to_str().unwrap().to_owned())
                    .unwrap_or_default();
                socket.send(Message::Item(protocol)).await.unwrap();
            },
        )
    }
    let addr = serve(Router::new().route("/ws", get(handler))).await;

    let mut request = format!("ws://{addr}/ws").into_client_request().unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "v1, v2".parse().unwrap());
    let mut client = connect_async(request).await.unwrap().0;

    assert_eq!(
        client.next().await.unwrap().unwrap(),
        TMessage::text(r#""v2""#)
    );
}

/// Serves `pong_loop` with codec `C`, sends `request` and returns the reply frame.
#[cfg(any(
    feature = "msgpack",
    feature = "bitcode",
    feature = "postcard",
    feature = "cbor",
    feature = "ron",
    feature = "flexbuffers",
    feature = "bson",
    feature = "xml"
))]
async fn codec_roundtrip<C>(request: TMessage) -> TMessage
where
    C: Codec + Send + 'static,
    C::EncodeError: Send,
    C::DecodeError: Send,
{
    let app = Router::new().route(
        "/ws",
        get(|ws: WebSocketUpgrade<Pong, Ping, C>| async move { ws.on_upgrade(pong_loop::<C>) }),
    );
    let mut client = connect(serve(app).await).await;
    client.send(request).await.unwrap();
    client.next().await.unwrap().unwrap()
}

#[cfg(feature = "msgpack")]
#[tokio::test]
async fn message_pack_codec_encodes_structs_as_arrays() {
    let ping = rmp_serde::to_vec(&Ping { n: 7 }).unwrap();
    let reply =
        codec_roundtrip::<axum_typed_websockets::MessagePackCodec>(TMessage::binary(ping)).await;
    assert_eq!(
        reply,
        TMessage::binary(rmp_serde::to_vec(&Pong { n: 8 }).unwrap())
    );
}

#[cfg(feature = "msgpack")]
#[tokio::test]
async fn message_pack_named_codec_encodes_structs_as_maps() {
    let ping = rmp_serde::to_vec_named(&Ping { n: 7 }).unwrap();
    let reply =
        codec_roundtrip::<axum_typed_websockets::MessagePackNamedCodec>(TMessage::binary(ping))
            .await;
    assert_eq!(
        reply,
        TMessage::binary(rmp_serde::to_vec_named(&Pong { n: 8 }).unwrap())
    );
}

#[cfg(feature = "bitcode")]
#[tokio::test]
async fn bitcode_codec_roundtrips_binary() {
    let ping = bitcode::serialize(&Ping { n: 7 }).unwrap();
    match codec_roundtrip::<axum_typed_websockets::BitcodeCodec>(TMessage::binary(ping)).await {
        TMessage::Binary(bin) => {
            assert_eq!(bitcode::deserialize::<Pong>(&bin).unwrap(), Pong { n: 8 });
        }
        other => panic!("expected binary frame, got {other:?}"),
    }
}

#[cfg(feature = "postcard")]
#[tokio::test]
async fn postcard_codec_roundtrips_binary() {
    let ping = postcard::to_allocvec(&Ping { n: 7 }).unwrap();
    match codec_roundtrip::<axum_typed_websockets::PostcardCodec>(TMessage::binary(ping)).await {
        TMessage::Binary(bin) => {
            assert_eq!(postcard::from_bytes::<Pong>(&bin).unwrap(), Pong { n: 8 });
        }
        other => panic!("expected binary frame, got {other:?}"),
    }
}

#[cfg(feature = "cbor")]
#[tokio::test]
async fn cbor_codec_roundtrips_binary() {
    let mut ping = Vec::new();
    ciborium::into_writer(&Ping { n: 7 }, &mut ping).unwrap();
    match codec_roundtrip::<axum_typed_websockets::CborCodec>(TMessage::binary(ping)).await {
        TMessage::Binary(bin) => {
            assert_eq!(
                ciborium::from_reader::<Pong, _>(&bin[..]).unwrap(),
                Pong { n: 8 }
            );
        }
        other => panic!("expected binary frame, got {other:?}"),
    }
}

#[cfg(feature = "ron")]
#[tokio::test]
async fn ron_codec_roundtrips_text() {
    match codec_roundtrip::<axum_typed_websockets::RonCodec>(TMessage::text("(n: 7)")).await {
        TMessage::Text(txt) => {
            assert_eq!(ron::from_str::<Pong>(&txt).unwrap(), Pong { n: 8 });
        }
        other => panic!("expected text frame, got {other:?}"),
    }
}

#[cfg(feature = "flexbuffers")]
#[tokio::test]
async fn flexbuffers_codec_roundtrips_binary() {
    let ping = flexbuffers::to_vec(Ping { n: 7 }).unwrap();
    match codec_roundtrip::<axum_typed_websockets::FlexbuffersCodec>(TMessage::binary(ping)).await {
        TMessage::Binary(bin) => {
            assert_eq!(
                flexbuffers::from_slice::<Pong>(&bin).unwrap(),
                Pong { n: 8 }
            );
        }
        other => panic!("expected binary frame, got {other:?}"),
    }
}

#[cfg(feature = "bson")]
#[tokio::test]
async fn bson_codec_roundtrips_binary() {
    let ping = bson::serialize_to_vec(&Ping { n: 7 }).unwrap();
    match codec_roundtrip::<axum_typed_websockets::BsonCodec>(TMessage::binary(ping)).await {
        TMessage::Binary(bin) => {
            assert_eq!(
                bson::deserialize_from_slice::<Pong>(&bin).unwrap(),
                Pong { n: 8 }
            );
        }
        other => panic!("expected binary frame, got {other:?}"),
    }
}

#[cfg(feature = "xml")]
#[tokio::test]
async fn xml_codec_roundtrips_text() {
    let ping = quick_xml::se::to_string(&Ping { n: 7 }).unwrap();
    match codec_roundtrip::<axum_typed_websockets::XmlCodec>(TMessage::text(ping)).await {
        TMessage::Text(txt) => {
            assert_eq!(
                quick_xml::de::from_str::<Pong>(&txt).unwrap(),
                Pong { n: 8 }
            );
        }
        other => panic!("expected text frame, got {other:?}"),
    }
}
