/// The protocol version.
///
/// Version table:
/// Protocols | App (Official implementation)
/// ----------|------------------------------
/// 1.0       | 1.0.0 - 1.8.0
/// 1.0, 2.0  | 1.9.0 - 1.14.0
/// 1.0, 2.1  | 1.15.0 - 1.17.0
/// 2.1       | 1.18.0
const protocolVersion = '2.1';

/// The optional swarm (HopSwift, v3) protocol version.
/// Used when both sides agree to do chunk-based multi-receiver transfers.
/// Negotiated via probing `/v3/prepare-swarm`; falls back to v2 unicast on 404.
const swarmProtocolVersion = '3.0';

/// Default chunk size for swarm transfers (4 MiB).
/// Sender splits each file into fixed-size chunks (last chunk may be smaller).
const defaultChunkSize = 4 * 1024 * 1024;

/// Swarm small-file bundling (Direction A). Files smaller than the chunk size
/// are concatenated into bundle units so a folder of many small files becomes a
/// handful of chunk requests instead of one request per file. Bundles are
/// capped so a single manifest/unit stays reasonable and the relay tree can
/// balance load across several bundles.
const bundleMaxBytes = 64 * 1024 * 1024; // ≤ 64 MiB of payload per bundle
const bundleMaxEntries = 1024; // ≤ 1024 member files per bundle

/// Assumed protocol version of peers for first handshake.
/// Generally this should be slightly lower than the current protocol version.
const peerProtocolVersion = '1.0';

/// The protocol version when no version is specified.
/// Prior v2, the protocol version was not specified.
const fallbackProtocolVersion = '1.0';

/// The default http server port and
/// and multicast port.
const defaultPort = 53317;

/// The default discovery timeout in milliseconds.
/// This is the time the discovery server waits for responses.
/// If no response is received within this time, the target server is unavailable.
const defaultDiscoveryTimeout = 500;

/// The default multicast group should be 224.0.0.0/24
/// because on some Android devices this is the only IP range
/// that can receive UDP multicast messages.
const defaultMulticastGroup = '224.0.0.167';
