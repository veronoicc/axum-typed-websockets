# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

# Unreleased

- **breaking:** Update to axum 0.8.
- **breaking:** `JsonCodec` is replaced by `TextJsonCodec` (the default, sends **Text** frames)
  and `BinaryJsonCodec` (sends Binary frames like 0.6's `JsonCodec`). Clients that expect
  binary frames from the default codec should switch to `BinaryJsonCodec`.
- **breaking:** The `Codec` trait now has separate `EncodeError`/`DecodeError` types and
  encodes to / decodes from `TextOrBinary` (`Utf8Bytes` or `Bytes`) instead of `Vec<u8>`.
- **breaking:** `Message::Ping`/`Message::Pong` carry `Bytes`, `Message::Close` carries
  `Option<CloseFrame>` (no lifetime).
- **breaking:** Minimum supported Rust version is now 1.88 (edition 2024).
- **added:** `WebSocket::protocol` returning the negotiated subprotocol.
- **added:** `MessagePackCodec`/`MessagePackNamedCodec` (`msgpack` feature), `BitcodeCodec`,
  `PostcardCodec`, `CborCodec`, `FlexbuffersCodec`, `BsonCodec`, `RonCodec` and `XmlCodec`
  behind the `bitcode`, `postcard`, `cbor`, `flexbuffers`, `bson`, `ron` and `xml` features.
- **fixed:** docs.rs now documents all feature-gated codecs.

# 0.6.0 (31. December 2023)

- Update to axum 0.7 ([#6])

[#6]: https://github.com/davidpdrsn/axum-typed-websockets/pull/6

# 0.5.0 (15. April, 2023)

- Update to axum 0.6.

# 0.4.0 (08. May, 2022)

- Update to axum 0.5.

# 0.3.0 (02. December, 2021)

- Update to axum 0.4.

# 0.2.0 (02. November, 2021)

- Update to axum 0.3.

# 0.1.0 (31. October, 2021)

- Initial release.
