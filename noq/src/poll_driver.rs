//! Poll-based driver for noq connections without async runtime dependency
//!
//! This module provides [`PollDriver`], a synchronous driver that wraps `noq-proto`'s
//! sans-IO endpoint and connections. It's designed for FFI bridges that have their own
//! event loop (e.g. Go's netpoller, Java's NIO, or a custom reactor like Aster's).
//!
//! # Design
//!
//! The async `noq` crate wraps `noq-proto` in Tokio tasks (`EndpointDriver`,
//! `ConnectionDriver`) connected by channels. `PollDriver` replaces that with a single
//! synchronous [`drive`](PollDriver::drive) method that the caller invokes from their own
//! event loop after a socket becomes readable or a timer fires.
//!
//! # Usage
//!
//! ```ignore
//! let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
//! socket.set_nonblocking(true)?;
//! let endpoint = proto::Endpoint::new(endpoint_config, Some(server_config), true, None);
//! let mut driver = PollDriver::new(socket, endpoint);
//!
//! loop {
//!     // Wait for socket readable or timer (epoll/kqueue/io_uring)
//!     let now = Instant::now();
//!     for event in driver.drive(now) {
//!         match event {
//!             PollEvent::NewConnection(incoming) => {
//!                 driver.accept(incoming, now, None);
//!             }
//!             PollEvent::StreamReadable { connection, stream } => {
//!                 let mut buf = [0u8; 4096];
//!                 let n = driver.stream_recv(connection, stream, &mut buf)?;
//!                 // process buf[..n]
//!             }
//!             // ...
//!         }
//!     }
//!     // Send any outgoing packets
//!     driver.flush_transmits(now);
//!     // Sleep until next deadline
//!     let timeout = driver.poll_timeout();
//! }
//! ```

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::num::NonZeroUsize;
use std::time::Instant;

use bytes::BytesMut;
use proto::{
    self, ClientConfig, ConnectError, ConnectionError, ConnectionHandle, DatagramEvent,
    EndpointEvent, StreamId, Transmit,
};
use thiserror::Error;
use tracing::{debug, trace, warn};

/// Maximum UDP datagrams to receive per `drive()` call before yielding.
const RECV_BATCH_SIZE: usize = 64;

/// Maximum outgoing datagrams per connection per `flush_transmits()` call.
const SEND_BATCH_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(64) };

/// A synchronous, poll-based driver for a noq endpoint and its connections.
///
/// This replaces the async `Endpoint` + `ConnectionDriver` + `EndpointDriver` with a
/// single struct that can be driven from any event loop without requiring Tokio or Smol.
pub struct PollDriver {
    endpoint: proto::Endpoint,
    connections: HashMap<ConnectionHandle, ConnectionState>,
    socket: UdpSocket,
    recv_buf: BytesMut,
    send_buf: Vec<u8>,
    response_buf: Vec<u8>,
}

struct ConnectionState {
    conn: proto::Connection,
}

/// Events produced by [`PollDriver::drive`].
#[derive(Debug)]
pub enum PollEvent {
    /// A new incoming connection attempt. Call [`PollDriver::accept`] or drop it.
    NewConnection(proto::Incoming),
    /// The connection handshake completed.
    Connected {
        /// The connection handle.
        connection: ConnectionHandle,
    },
    /// A stream has data available to read.
    StreamReadable {
        /// The connection handle.
        connection: ConnectionHandle,
        /// The stream with available data.
        stream: StreamId,
    },
    /// A stream is ready to accept more data.
    StreamWritable {
        /// The connection handle.
        connection: ConnectionHandle,
        /// The writable stream.
        stream: StreamId,
    },
    /// New streams of the given direction may be opened.
    StreamsAvailable {
        /// The connection handle.
        connection: ConnectionHandle,
        /// Direction of available streams.
        dir: proto::Dir,
    },
    /// A connection was lost.
    ConnectionLost {
        /// The connection handle.
        connection: ConnectionHandle,
        /// The reason for the loss.
        reason: ConnectionError,
    },
    /// An unreliable datagram was received on a connection.
    DatagramReceived {
        /// The connection handle.
        connection: ConnectionHandle,
    },
}

/// Errors from [`PollDriver`] operations.
#[derive(Debug, Error)]
pub enum PollDriverError {
    /// I/O error from the UDP socket.
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// The specified connection does not exist.
    #[error("unknown connection")]
    UnknownConnection,
    /// Stream read error from the protocol layer.
    #[error("stream read: {0}")]
    Read(#[from] proto::ReadError),
    /// Stream read could not be started.
    #[error("stream not readable: {0}")]
    Readable(#[from] proto::ReadableError),
    /// Stream write error.
    #[error("stream write: {0}")]
    Write(#[from] proto::WriteError),
    /// Stream finish error.
    #[error("stream finish: {0}")]
    Finish(#[from] proto::FinishError),
    /// Stream was closed.
    #[error("closed stream")]
    ClosedStream,
}

impl From<proto::ClosedStream> for PollDriverError {
    fn from(_: proto::ClosedStream) -> Self {
        Self::ClosedStream
    }
}

impl PollDriver {
    /// Create a new poll-based driver.
    ///
    /// The socket must be set to non-blocking mode.
    pub fn new(socket: UdpSocket, endpoint: proto::Endpoint) -> Self {
        Self {
            endpoint,
            connections: HashMap::new(),
            socket,
            recv_buf: BytesMut::zeroed(65536),
            send_buf: Vec::new(),
            response_buf: Vec::new(),
        }
    }

    /// Drive the endpoint forward.
    ///
    /// Call this after:
    /// - The socket becomes readable (new UDP datagrams arrived)
    /// - A timer from [`Self::poll_timeout`] has fired
    ///
    /// Returns events for the application to process.
    pub fn drive(&mut self, now: Instant) -> Vec<PollEvent> {
        let mut events = Vec::new();

        // 1. Receive incoming datagrams and route to endpoint
        self.drive_recv(now, &mut events);

        // 2. Handle timeouts for all connections
        self.drive_timers(now);

        // 3. Route endpoint events between connections and endpoint
        self.route_endpoint_events();

        // 4. Collect application events from all connections
        self.collect_app_events(&mut events);

        events
    }

    /// Flush pending outgoing packets for all connections.
    ///
    /// Call this after [`drive`](Self::drive) to send any outgoing data.
    /// Returns the number of datagrams sent, or an I/O error.
    pub fn flush_transmits(&mut self, now: Instant) -> Result<usize, io::Error> {
        let mut total = 0;
        let handles: Vec<_> = self.connections.keys().copied().collect();
        for handle in handles {
            total += self.flush_connection_transmits(handle, now)?;
        }
        Ok(total)
    }

    /// Read from a stream directly into a caller-provided buffer.
    ///
    /// Returns the number of bytes read, or 0 if the stream is finished.
    /// Uses the zero-copy `read_into` path that bypasses `Bytes` allocation.
    pub fn stream_recv(
        &mut self,
        connection: ConnectionHandle,
        stream: StreamId,
        buf: &mut [u8],
    ) -> Result<usize, PollDriverError> {
        let state = self
            .connections
            .get_mut(&connection)
            .ok_or(PollDriverError::UnknownConnection)?;

        let mut recv = state.conn.recv_stream(stream);
        let mut chunks = recv.read(true)?;
        let n = chunks.read_into(buf)?;
        let should_transmit = chunks.finalize();
        if should_transmit.should_transmit() {
            // Endpoint events will be picked up in the next drive() cycle
        }
        Ok(n)
    }

    /// Write data to a stream.
    ///
    /// Returns the number of bytes written (may be less than `data.len()` if the
    /// stream's flow control window is full).
    pub fn stream_send(
        &mut self,
        connection: ConnectionHandle,
        stream: StreamId,
        data: &[u8],
    ) -> Result<usize, PollDriverError> {
        let state = self
            .connections
            .get_mut(&connection)
            .ok_or(PollDriverError::UnknownConnection)?;

        let mut send = state.conn.send_stream(stream);
        let written = send.write(data)?;
        Ok(written)
    }

    /// Finish a send stream, signaling no more data will be written.
    pub fn stream_finish(
        &mut self,
        connection: ConnectionHandle,
        stream: StreamId,
    ) -> Result<(), PollDriverError> {
        let state = self
            .connections
            .get_mut(&connection)
            .ok_or(PollDriverError::UnknownConnection)?;
        state.conn.send_stream(stream).finish()?;
        Ok(())
    }

    /// Open a new bidirectional stream, returning the stream ID.
    ///
    /// Returns `None` if the peer's stream limit has been reached.
    pub fn open_bi(
        &mut self,
        connection: ConnectionHandle,
    ) -> Result<Option<StreamId>, PollDriverError> {
        let state = self
            .connections
            .get_mut(&connection)
            .ok_or(PollDriverError::UnknownConnection)?;
        Ok(state.conn.streams().open(proto::Dir::Bi))
    }

    /// Open a new unidirectional stream, returning the stream ID.
    ///
    /// Returns `None` if the peer's stream limit has been reached.
    pub fn open_uni(
        &mut self,
        connection: ConnectionHandle,
    ) -> Result<Option<StreamId>, PollDriverError> {
        let state = self
            .connections
            .get_mut(&connection)
            .ok_or(PollDriverError::UnknownConnection)?;
        Ok(state.conn.streams().open(proto::Dir::Uni))
    }

    /// Accept an incoming connection.
    ///
    /// Returns the connection handle on success.
    pub fn accept(
        &mut self,
        incoming: proto::Incoming,
        now: Instant,
        server_config: Option<std::sync::Arc<proto::ServerConfig>>,
    ) -> Result<ConnectionHandle, ConnectionError> {
        match self
            .endpoint
            .accept(incoming, now, &mut self.response_buf, server_config)
        {
            Ok((handle, conn)) => {
                self.connections.insert(
                    handle,
                    ConnectionState { conn },
                );
                Ok(handle)
            }
            Err(error) => {
                if let Some(transmit) = error.response {
                    let _ = self.send_transmit(&transmit, &self.response_buf.clone());
                }
                Err(error.cause)
            }
        }
    }

    /// Initiate an outgoing connection.
    pub fn connect(
        &mut self,
        now: Instant,
        config: ClientConfig,
        remote: SocketAddr,
        server_name: &str,
    ) -> Result<ConnectionHandle, ConnectError> {
        let (handle, conn) = self.endpoint.connect(now, config, remote, server_name)?;
        self.connections.insert(handle, ConnectionState { conn });
        Ok(handle)
    }

    /// Close a connection.
    pub fn close(
        &mut self,
        connection: ConnectionHandle,
        now: Instant,
        error_code: proto::VarInt,
        reason: bytes::Bytes,
    ) {
        if let Some(state) = self.connections.get_mut(&connection) {
            state.conn.close(now, error_code, reason);
        }
    }

    /// Get the next timer deadline across all connections.
    ///
    /// Returns `None` if no timers are active. The caller should arrange to call
    /// [`drive`](Self::drive) at or after this instant.
    pub fn poll_timeout(&mut self) -> Option<Instant> {
        self.connections
            .values_mut()
            .filter_map(|s| s.conn.poll_timeout())
            .min()
    }

    /// Get connection statistics.
    pub fn connection_stats(
        &mut self,
        connection: ConnectionHandle,
    ) -> Option<proto::ConnectionStats> {
        self.connections.get_mut(&connection).map(|s| s.conn.stats())
    }

    // --- internal ---

    fn drive_recv(&mut self, now: Instant, events: &mut Vec<PollEvent>) {
        for _ in 0..RECV_BATCH_SIZE {
            self.recv_buf.resize(65536, 0);
            let (len, src) = match self.socket.recv_from(&mut self.recv_buf) {
                Ok(x) => x,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    debug!("UDP recv error: {e}");
                    break;
                }
            };

            let local_addr = match self.socket.local_addr() {
                Ok(addr) => addr,
                Err(_) => continue,
            };

            let data = self.recv_buf.split_to(len);
            let four_tuple = proto::FourTuple::new(src, Some(local_addr.ip()));

            let event = self
                .endpoint
                .handle(now, four_tuple, None, data, &mut self.response_buf);

            match event {
                Some(DatagramEvent::NewConnection(incoming)) => {
                    events.push(PollEvent::NewConnection(incoming));
                }
                Some(DatagramEvent::ConnectionEvent(handle, conn_event)) => {
                    if let Some(state) = self.connections.get_mut(&handle) {
                        state.conn.handle_event(conn_event);
                    } else {
                        trace!("received packet for unknown connection {handle:?}");
                    }
                }
                Some(DatagramEvent::Response(transmit)) => {
                    let _ = self.send_transmit(&transmit, &self.response_buf.clone());
                }
                None => {}
            }
        }
    }

    fn drive_timers(&mut self, now: Instant) {
        for state in self.connections.values_mut() {
            if state
                .conn
                .poll_timeout()
                .is_some_and(|deadline| now >= deadline)
            {
                state.conn.handle_timeout(now);
            }
        }
    }

    fn route_endpoint_events(&mut self) {
        // Drain endpoint events from connections and route them back
        let mut events_to_route: Vec<(ConnectionHandle, EndpointEvent)> = Vec::new();
        for (&handle, state) in self.connections.iter_mut() {
            while let Some(event) = state.conn.poll_endpoint_events() {
                events_to_route.push((handle, event));
            }
        }

        for (handle, event) in events_to_route {
            let is_drained = event.is_drained();
            if let Some(conn_event) = self.endpoint.handle_event(handle, event) {
                if let Some(state) = self.connections.get_mut(&handle) {
                    state.conn.handle_event(conn_event);
                }
            }
            if is_drained {
                self.connections.remove(&handle);
            }
        }
    }

    fn collect_app_events(&mut self, events: &mut Vec<PollEvent>) {
        let handles: Vec<_> = self.connections.keys().copied().collect();
        for handle in handles {
            let Some(state) = self.connections.get_mut(&handle) else {
                continue;
            };
            while let Some(event) = state.conn.poll() {
                match event {
                    proto::Event::Connected => {
                        events.push(PollEvent::Connected {
                            connection: handle,
                        });
                    }
                    proto::Event::Stream(proto::StreamEvent::Readable { id }) => {
                        events.push(PollEvent::StreamReadable {
                            connection: handle,
                            stream: id,
                        });
                    }
                    proto::Event::Stream(proto::StreamEvent::Writable { id }) => {
                        events.push(PollEvent::StreamWritable {
                            connection: handle,
                            stream: id,
                        });
                    }
                    proto::Event::Stream(proto::StreamEvent::Available { dir }) => {
                        events.push(PollEvent::StreamsAvailable {
                            connection: handle,
                            dir,
                        });
                    }
                    proto::Event::ConnectionLost { reason } => {
                        events.push(PollEvent::ConnectionLost {
                            connection: handle,
                            reason,
                        });
                    }
                    proto::Event::DatagramReceived => {
                        events.push(PollEvent::DatagramReceived {
                            connection: handle,
                        });
                    }
                    _ => {
                        trace!("unhandled proto event: {event:?}");
                    }
                }
            }
        }
    }

    fn flush_connection_transmits(
        &mut self,
        handle: ConnectionHandle,
        now: Instant,
    ) -> Result<usize, io::Error> {
        let mut sent = 0;
        let state = match self.connections.get_mut(&handle) {
            Some(s) => s,
            None => return Ok(0),
        };
        while let Some(transmit) =
            state
                .conn
                .poll_transmit(now, SEND_BATCH_SIZE, &mut self.send_buf)
        {
            let data = &self.send_buf[..transmit.size];
            match self.socket.send_to(data, transmit.destination) {
                Ok(_) => sent += 1,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    warn!("UDP send error: {e}");
                    return Err(e);
                }
            }
        }
        Ok(sent)
    }

    fn send_transmit(&self, transmit: &Transmit, buf: &[u8]) -> Result<(), io::Error> {
        let data = &buf[..transmit.size];
        self.socket
            .send_to(data, transmit.destination)
            .map(|_| ())
    }
}
