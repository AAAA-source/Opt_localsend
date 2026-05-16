/// A single peer (receiver) participating in a swarm session.
/// Distributed by the sender to all receivers so they can pull chunks from each other.
class PeerInfo {
  final String fingerprint;
  final String ip;
  final int port;
  final bool https;

  const PeerInfo({
    required this.fingerprint,
    required this.ip,
    required this.port,
    required this.https,
  });

  Map<String, dynamic> toJson() => {
        'fingerprint': fingerprint,
        'ip': ip,
        'port': port,
        'https': https,
      };

  static PeerInfo fromJson(Map<String, dynamic> map) => PeerInfo(
        fingerprint: map['fingerprint'] as String,
        ip: map['ip'] as String,
        port: map['port'] as int,
        https: map['https'] as bool,
      );
}
