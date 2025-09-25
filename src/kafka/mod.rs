// Kafka protocol implementation modules
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//

mod admin;
pub mod broker;
pub mod counters;
mod fetch;
pub mod groups;

mod metadata;

mod produce;

pub mod shutdown;
pub mod storage;

use crate::kafka::broker::BROKER;
use crate::kafka::counters::{
    IDLE_TRACKER, RESPONSE_QUEUE_TIME_MS, RESPONSE_SEND_TIME_MS, TOTALTIME_MS,
};
use crate::kafka::shutdown::Shutdown;
use crate::metric_with_fetch_consumer;
use crate::request_kind_name;
use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, RequestKind, ResponseHeader};
use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::{Decodable, Encodable};
use std::io::Error;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tracing::error;

pub struct Kafka;

const HTTP_GET_BYTES: i32 = 1195725856; // "GET " in ASCII
const REQUEST_TOO_LARGE: i32 = 100000000;

impl Kafka {
    pub(crate) async fn handle_request(
        mut client: TcpStream,
        shutdown: Arc<Shutdown>,
    ) -> anyhow::Result<()> {
        let (mut rd, mut wr) = client.split();

        loop {
            let mut current_stage_start_time = Instant::now();
            let request_start_time = Instant::now();
            // 1. Read the size of the request and handle size related errors.
            let n = Self::log_io_call(rd.read_i32().await)?;

            // Track request start for idle time measurement
            if let Ok(mut tracker) = IDLE_TRACKER.lock() {
                tracker.on_request_start();
            }
            if n == HTTP_GET_BYTES {
                // Some clients may think this is an HTTP server. Send a forbidden response.
                Self::log_io_call(
                    wr.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                        .await,
                )?;
                return Err(anyhow::Error::msg("GET request on Kafka port"));
            }
            if n > REQUEST_TOO_LARGE {
                return Err(anyhow::Error::msg("Request size too large"));
            }

            // 2. Read the request and handle decoding errors.
            let mut buf = BytesMut::with_capacity(n as usize);
            buf.resize(n as usize, 0);
            if shutdown.is_shutdown() {
                return Ok(());
            }
            Self::log_io_call(rd.read_exact(&mut buf).await)?;
            let mut buf = buf.freeze();

            let api_key = ApiKey::try_from(buf.peek_bytes(0..2).get_i16())
                .map_err(|_| anyhow::Error::msg("Unknown API key"))?;
            //info!("Received request for API key: {:?}", api_key);
            let api_version = buf.peek_bytes(2..4).get_i16();
            let header_version = api_key.request_header_version(api_version);
            let correlation_id = RequestHeader::decode(&mut buf, header_version)?.correlation_id;
            let request = RequestKind::decode(api_key, &mut buf, api_version)?;

            // 3. invoke the broker to handle the request and generate a response.
            let mut response = BytesMut::new();
            response.put_i32(0); // placeholder for total length
            ResponseHeader::default()
                .with_correlation_id(correlation_id)
                .encode(&mut response, api_key.response_header_version(api_version))?;

            let request_name = request_kind_name!(request);
            BROKER
                .reply(&mut current_stage_start_time, &request_name, request)
                .await?
                .encode(&mut response, api_version)?;
            let length = (response.len() - 4) as i32;
            response[0..4].copy_from_slice(length.to_be_bytes().as_ref());

            let elapsed = current_stage_start_time.elapsed().as_millis() as f64;
            let current_stage_start_time = Instant::now();
            metric_with_fetch_consumer!(RESPONSE_QUEUE_TIME_MS, request_name, elapsed);

            // 4. Write the response and handle write errors.
            Self::log_io_call(wr.write_all(&response).await)?;

            let elapsed = current_stage_start_time.elapsed().as_millis() as f64;
            metric_with_fetch_consumer!(RESPONSE_SEND_TIME_MS, request_name, elapsed);

            // Track request end for idle time measurement
            if let Ok(mut tracker) = IDLE_TRACKER.lock() {
                tracker.on_request_end();
            }

            let elapsed = request_start_time.elapsed().as_millis() as f64;
            metric_with_fetch_consumer!(TOTALTIME_MS, request_name, elapsed);
        }
    }

    fn log_io_call<T>(result: Result<T, Error>) -> Result<T, Error> {
        if result.is_err() {
            let err = &result.as_ref().err();
            match err.unwrap().kind() {
                // for the following errors due to client disconnect, we just avoid logging them.
                std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted => {}
                // for all other errors, we log them.
                _ => {
                    error!("Error reading from client: {:?}", err);
                }
            }
        }
        result
    }
}
