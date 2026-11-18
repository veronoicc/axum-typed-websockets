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

#[cfg(feature = "msgpack")]
#[tokio::test]
async fn msgpack_codec_roundtrips_binary() {
    use axum_typed_websockets::MsgPackCodec;

    async fn handler(ws: WebSocketUpgrade<Pong, Ping, MsgPackCodec>) -> impl IntoResponse {
        ws.on_upgrade(pong_loop)
    }
    let mut client = connect(serve(Router::new().route("/ws", get(handler))).await).await;

    let ping = rmp_serde::to_vec(&Ping { n: 7 }).unwrap();
    client.send(TMessage::binary(ping)).await.unwrap();

    match client.next().await.unwrap().unwrap() {
        TMessage::Binary(bin) => {
            assert_eq!(rmp_serde::from_slice::<Pong>(&bin).unwrap(), Pong { n: 8 });
        }
        other => panic!("expected binary frame, got {other:?}"),
    }
}
