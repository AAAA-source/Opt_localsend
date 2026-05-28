/// Per-peer topology assignment for a single swarm session.
///
/// Computed by the sender via [RelayPlanner.computeTree] and distributed to
/// each receiver inside [PrepareSwarmRequestDto] so every node knows its
/// parent (data source) and children (push targets) without further
/// coordination.
class RelayPlanDto {
  final String sessionId;

  /// Fingerprint of this peer's parent node.
  /// Null for the root node(s), which receive data directly from the sender.
  final String? parentFingerprint;

  /// Fingerprints of the children this peer must push chunks to after
  /// receiving them.
  final List<String> childrenFingerprints;

  /// True if this node only forwards chunks and does not keep the final file.
  /// In a typical LocalSend LAN scenario every device is both a relay and a
  /// receiver, so this is false for all nodes. Reserved for future use cases
  /// where a dedicated forwarding device participates in the swarm.
  final bool isPureRelay;

  RelayPlanDto({
    required this.sessionId,
    this.parentFingerprint,
    required this.childrenFingerprints,
    this.isPureRelay = false,
  });

  Map<String, dynamic> toJson() => {
        'sessionId': sessionId,
        'parentFingerprint': parentFingerprint,
        'childrenFingerprints': childrenFingerprints,
        'isPureRelay': isPureRelay,
      };

  factory RelayPlanDto.fromJson(Map<String, dynamic> json) => RelayPlanDto(
        sessionId: json['sessionId'] as String,
        parentFingerprint: json['parentFingerprint'] as String?,
        childrenFingerprints: (json['childrenFingerprints'] as List<dynamic>)
            .map((e) => e as String)
            .toList(),
        isPureRelay: json['isPureRelay'] as bool? ?? false,
      );
}