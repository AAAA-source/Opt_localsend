import 'package:common/model/dto/swarm/relay_plan_dto.dart';

/// Bandwidth hint derived from Algorithm 1's EWMA throughput estimate.
class PeerBandwidthHint {
  final String fingerprint;

  /// Estimated throughput in bytes/ms, taken from the EWMA tracker in
  /// [_AdaptiveChunkScheduler]. Zero if no prior measurement is available.
  final double ewmaThroughput;

  PeerBandwidthHint({
    required this.fingerprint,
    required this.ewmaThroughput,
  });
}

/// Builds a bandwidth-aware overlay tree for a swarm session.
///
/// The tree is a complete k-ary tree (k = [maxFanOut]) laid out in BFS order
/// over peers sorted by descending throughput. High-bandwidth peers sit near
/// the root so they carry more of the forwarding load, while low-bandwidth
/// peers become leaves and only need to receive, not forward.
class RelayPlanner {
  /// Computes a [RelayPlanDto] for every peer in [peerFingerprints].
  ///
  /// [sessionId]         Session identifier shared across all peers.
  /// [peerFingerprints]  All receivers participating in this swarm session.
  /// [bandwidthHints]    EWMA throughput estimates from a prior or ongoing
  ///                     session. When empty (first transfer), all peers are
  ///                     treated as equal and the tree degenerates to insertion
  ///                     order.
  /// [maxFanOut]         Maximum number of children per relay node. Lower values
  ///                     produce deeper trees with less per-node fan-out;
  ///                     higher values produce shallower trees where each relay
  ///                     pushes to more children simultaneously. Default is 4,
  ///                     balancing tree depth against per-node upload pressure.
  static Map<String, RelayPlanDto> computeTree({
    required String sessionId,
    required List<String> peerFingerprints,
    required List<PeerBandwidthHint> bandwidthHints,
    int maxFanOut = 4,
  }) {
    if (peerFingerprints.isEmpty) return const {};

    // Build a throughput lookup from the provided hints.
    final bwLookup = {
      for (final hint in bandwidthHints) hint.fingerprint: hint.ewmaThroughput,
    };

    // Sort peers by descending throughput so faster nodes become parents.
    // Peers with no hint (bwLookup miss) default to 0.0 and sort to the end.
    final sorted = List<String>.from(peerFingerprints)
      ..sort((a, b) => (bwLookup[b] ?? 0.0).compareTo(bwLookup[a] ?? 0.0));

    // Assign parent and children using the BFS index relationships of a
    // complete k-ary tree:
    //   parent of node i  : (i - 1) ~/ maxFanOut   (undefined for i == 0)
    //   children of node i: i * maxFanOut + 1  ..  i * maxFanOut + maxFanOut
    final planMap = <String, RelayPlanDto>{};
    for (var i = 0; i < sorted.length; i++) {
      final fingerprint = sorted[i];

      final parentFp = i == 0 ? null : sorted[(i - 1) ~/ maxFanOut];

      final children = <String>[];
      for (var c = 1; c <= maxFanOut; c++) {
        final childIndex = i * maxFanOut + c;
        if (childIndex < sorted.length) children.add(sorted[childIndex]);
      }

      planMap[fingerprint] = RelayPlanDto(
        sessionId: sessionId,
        parentFingerprint: parentFp,
        childrenFingerprints: children,
        // All LocalSend peers are both relays and receivers by default.
        isPureRelay: false,
      );
    }

    return planMap;
  }
}