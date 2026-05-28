import 'package:common/model/dto/file_dto.dart';
import 'package:common/model/dto/info_register_dto.dart';
import 'package:common/model/dto/swarm/chunk_plan_dto.dart';
import 'package:common/model/dto/swarm/peer_info.dart';
import 'package:common/model/dto/swarm/relay_plan_dto.dart';

/// Sender → every receiver: open a swarm session.
///
/// The same payload is broadcast to all targets; `myIndex` tells each receiver
/// which slot they occupy in the round-robin schedule (chunk k → peer[k mod M]).
class PrepareSwarmRequestDto {
  final InfoRegisterDto info;
  final String sessionId; // sender-chosen, shared by all peers
  final Map<String, FileDto> files;
  final Map<String, ChunkPlanDto> plans; // fileId → plan
  final List<PeerInfo> peers; // all M receivers, deterministic order
  final int myIndex; // index of THIS receiver in [peers]
  final RelayPlanDto? relayPlan; // optional per-peer topology assignment

  const PrepareSwarmRequestDto({
    required this.info,
    required this.sessionId,
    required this.files,
    required this.plans,
    required this.peers,
    required this.myIndex,
    this.relayPlan,
  });

  Map<String, dynamic> toJson() => {
    'info': info.toJson(),
    'sessionId': sessionId,
    'files': {
      for (final entry in files.entries)
        entry.key: FileDtoMapper().encode(entry.value),
    },
    'plans': {
      for (final entry in plans.entries) entry.key: entry.value.toJson(),
    },
    'peers': peers.map((p) => p.toJson()).toList(),
    'myIndex': myIndex,
    'relayPlan': relayPlan?.toJson(),
  };

  static PrepareSwarmRequestDto fromJson(Map<String, dynamic> map) =>
      PrepareSwarmRequestDto(
        info: InfoRegisterDto.fromJson(map['info'] as Map<String, dynamic>),
        sessionId: map['sessionId'] as String,
        files: {
          for (final entry in (map['files'] as Map<String, dynamic>).entries)
            entry.key: FileDtoMapper().decode(entry.value),
        },
        plans: {
          for (final entry in (map['plans'] as Map<String, dynamic>).entries)
            entry.key: ChunkPlanDto.fromJson(
              entry.value as Map<String, dynamic>,
            ),
        },
        peers: (map['peers'] as List)
            .map((p) => PeerInfo.fromJson(p as Map<String, dynamic>))
            .toList(),
        myIndex: map['myIndex'] as int,
        relayPlan: map['relayPlan'] != null
            ? RelayPlanDto.fromJson(map['relayPlan'] as Map<String, dynamic>)
            : null,
      );
}
