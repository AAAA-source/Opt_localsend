import 'package:common/model/device.dart';
import 'package:common/model/dto/file_dto.dart';
import 'package:common/model/dto/swarm/chunk_plan_dto.dart';
import 'package:common/model/session_status.dart';

/// In-memory state of an outbound swarm session held by the sender.
class SwarmSendState {
  final String sessionId;
  final SessionStatus status;
  final List<Device> targets; // index in this list == peer index used in scheduling
  final Map<String, FileDto> files;
  final Map<String, ChunkPlanDto> plans; // fileId -> plan
  // Tokens issued by each receiver to authorize chunk uploads + peer pulls.
  // fingerprint -> (fileId -> token)
  final Map<String, Map<String, String>> tokens;
  // Per (fileId, chunkIndex) tracking: which peer (fingerprint) has acknowledged having it.
  // Cleared once the chunk is confirmed at every receiver.
  final Map<String, Set<int>> sentToPrimary; // fileId -> indices the sender has uploaded directly
  final int startTime;
  final int? endTime;
  final String? errorMessage;
  // Preparation progress (SHA256 pre-hashing). 0..1.
  final double prepareProgress;
  // Timing instrumentation (epoch millis) — for the proposal's last-receiver completion metric.
  final int? prepareStartTime;
  final int? prepareEndTime;
  final int? firstChunkSentAt;
  final int? lastChunkSentAt;
  // fingerprint -> epoch millis when that peer's bitmap first reported all chunks for all files.
  final Map<String, int> peerCompleteTime;

  const SwarmSendState({
    required this.sessionId,
    required this.status,
    required this.targets,
    required this.files,
    required this.plans,
    required this.tokens,
    required this.sentToPrimary,
    required this.startTime,
    required this.endTime,
    required this.errorMessage,
    required this.prepareProgress,
    required this.prepareStartTime,
    required this.prepareEndTime,
    required this.firstChunkSentAt,
    required this.lastChunkSentAt,
    required this.peerCompleteTime,
  });

  SwarmSendState copyWith({
    SessionStatus? status,
    Map<String, ChunkPlanDto>? plans,
    Map<String, Map<String, String>>? tokens,
    Map<String, Set<int>>? sentToPrimary,
    int? endTime,
    String? errorMessage,
    double? prepareProgress,
    int? prepareStartTime,
    int? prepareEndTime,
    int? firstChunkSentAt,
    int? lastChunkSentAt,
    Map<String, int>? peerCompleteTime,
  }) {
    return SwarmSendState(
      sessionId: sessionId,
      status: status ?? this.status,
      targets: targets,
      files: files,
      plans: plans ?? this.plans,
      tokens: tokens ?? this.tokens,
      sentToPrimary: sentToPrimary ?? this.sentToPrimary,
      startTime: startTime,
      endTime: endTime ?? this.endTime,
      errorMessage: errorMessage ?? this.errorMessage,
      prepareProgress: prepareProgress ?? this.prepareProgress,
      prepareStartTime: prepareStartTime ?? this.prepareStartTime,
      prepareEndTime: prepareEndTime ?? this.prepareEndTime,
      firstChunkSentAt: firstChunkSentAt ?? this.firstChunkSentAt,
      lastChunkSentAt: lastChunkSentAt ?? this.lastChunkSentAt,
      peerCompleteTime: peerCompleteTime ?? this.peerCompleteTime,
    );
  }

  /// Total bytes across all files (cached as a getter; cheap because files map is small).
  int get totalBytes => files.values.fold<int>(0, (a, f) => a + f.size);

  /// Total chunks across all files (0 before plans are filled).
  int get totalChunks => plans.values.fold<int>(0, (a, p) => a + p.totalChunks);

  /// Direct uploads completed so far across all files.
  int get sentChunks => sentToPrimary.values.fold<int>(0, (a, s) => a + s.length);
}
