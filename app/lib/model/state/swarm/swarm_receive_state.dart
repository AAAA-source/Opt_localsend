import 'dart:async';

import 'package:common/model/device.dart';
import 'package:common/model/dto/swarm/bitmap_dto.dart';
import 'package:common/model/dto/swarm/chunk_plan_dto.dart';
import 'package:common/model/dto/swarm/peer_info.dart';
import 'package:common/model/dto/swarm/relay_plan_dto.dart';
import 'package:common/model/file_type.dart';
import 'package:common/model/session_status.dart';

/// One destination file within a swarm unit. A standalone file is a unit with a
/// single member at offset 0; a bundle has several members concatenated.
class SwarmMember {
  final String fileId; // original FileDto.id
  final String fileName;
  final FileType fileType;
  final int size;
  final int offset; // byte offset of this member within the unit's byte space
  final String? path; // absolute destination path, set after accept

  const SwarmMember({
    required this.fileId,
    required this.fileName,
    required this.fileType,
    required this.size,
    required this.offset,
    this.path,
  });

  SwarmMember withPath(String p) => SwarmMember(
        fileId: fileId,
        fileName: fileName,
        fileType: fileType,
        size: size,
        offset: offset,
        path: p,
      );
}

/// Runtime state of one swarm **unit** on the receiver side — either a single
/// standalone file or a bundle of small files concatenated into one byte space.
/// All swarm machinery (bitmap, plan, token) is keyed by the unit id.
class SwarmReceivingFile {
  final String unitId; // == plan.fileId; the key used by the swarm protocol
  final ChunkPlanDto plan;
  final String token; // self-issued, returned to sender via prepare-swarm response
  final List<SwarmMember> members; // ≥1 destination files (standalone ⇒ 1)
  final BitmapDto bitmap; // mutable: in-place flipped as chunks arrive
  // How many chunks were obtained from each source. For demo / proposal verification.
  final Map<String, int> chunksFromSource; // 'sender' or peer fingerprint -> count
  final String? errorMessage;

  const SwarmReceivingFile({
    required this.unitId,
    required this.plan,
    required this.token,
    required this.members,
    required this.bitmap,
    required this.chunksFromSource,
    required this.errorMessage,
  });

  /// Total payload bytes of this unit (sum of member sizes).
  int get unitSize => members.fold<int>(0, (a, m) => a + m.size);

  /// Display label for logs / single-unit UI fallbacks.
  String get displayName => members.length == 1 ? members.first.fileName : '${members.length} files';

  SwarmReceivingFile copyWith({
    List<SwarmMember>? members,
    String? errorMessage,
  }) {
    return SwarmReceivingFile(
      unitId: unitId,
      plan: plan,
      token: token,
      members: members ?? this.members,
      bitmap: bitmap,
      chunksFromSource: chunksFromSource,
      errorMessage: errorMessage ?? this.errorMessage,
    );
  }
}

class SwarmReceiveState {
  final String sessionId;
  final SessionStatus status;
  final Device sender;
  final String senderAlias;
  final List<PeerInfo> peers; // all receivers including self
  final int myIndex;
  final Map<String, SwarmReceivingFile> files;
  final int? startTime;
  final int? endTime;
  final String destinationDirectory;
  final String cacheDirectory;
  final Set<String> createdDirectories;
  // null while waiting for accept/decline; emits the user's selection map (fileId -> filename) or null on decline.
  final StreamController<Map<String, String>?>? responseHandler;
  // Most-recent bitmap announced by each peer (excluding self). fingerprint -> fileId -> bitmap
  final Map<String, Map<String, BitmapDto>> peerBitmaps;
  final String? errorMessage;
  // Timing instrumentation (epoch millis).
  final int? firstChunkReceivedAt;
  final int? lastChunkReceivedAt;

  final RelayPlanDto? relayPlan; // optional per-peer topology assignment

  /// Map key: fileId, value: 
  /// Set of chunk indices already pushed to children
  final Map<String, Set<int>> pushedToChildren;

  const SwarmReceiveState({
    required this.sessionId,
    required this.status,
    required this.sender,
    required this.senderAlias,
    required this.peers,
    required this.myIndex,
    required this.files,
    required this.startTime,
    required this.endTime,
    required this.destinationDirectory,
    required this.cacheDirectory,
    required this.createdDirectories,
    required this.responseHandler,
    required this.peerBitmaps,
    required this.errorMessage,
    this.firstChunkReceivedAt,
    this.lastChunkReceivedAt,
    this.relayPlan,
    this.pushedToChildren = const {},
  });

  SwarmReceiveState copyWith({
    SessionStatus? status,
    Map<String, SwarmReceivingFile>? files,
    int? startTime,
    int? endTime,
    StreamController<Map<String, String>?>? responseHandler,
    bool clearResponseHandler = false,
    Map<String, Map<String, BitmapDto>>? peerBitmaps,
    String? errorMessage,
    int? firstChunkReceivedAt,
    int? lastChunkReceivedAt,
    RelayPlanDto? relayPlan,
    Map<String, Set<int>>? pushedToChildren,
  }) {
    return SwarmReceiveState(
      sessionId: sessionId,
      status: status ?? this.status,
      sender: sender,
      senderAlias: senderAlias,
      peers: peers,
      myIndex: myIndex,
      files: files ?? this.files,
      startTime: startTime ?? this.startTime,
      endTime: endTime ?? this.endTime,
      destinationDirectory: destinationDirectory,
      cacheDirectory: cacheDirectory,
      createdDirectories: createdDirectories,
      responseHandler: clearResponseHandler ? null : (responseHandler ?? this.responseHandler),
      peerBitmaps: peerBitmaps ?? this.peerBitmaps,
      errorMessage: errorMessage ?? this.errorMessage,
      firstChunkReceivedAt: firstChunkReceivedAt ?? this.firstChunkReceivedAt,
      lastChunkReceivedAt: lastChunkReceivedAt ?? this.lastChunkReceivedAt,
      relayPlan: relayPlan ?? this.relayPlan,
      pushedToChildren: pushedToChildren ?? this.pushedToChildren,
    );
  }

  /// Total bytes across all files in this session.
  int get totalBytes => files.values.fold<int>(0, (a, f) => a + f.unitSize);

  /// Bytes received so far (counted by bitmap, using per-chunk length).
  int get receivedBytes {
    var sum = 0;
    for (final rf in files.values) {
      for (var k = 0; k < rf.plan.totalChunks; k++) {
        if (rf.bitmap.has(k)) sum += rf.plan.chunkLength(k);
      }
    }
    return sum;
  }

  /// Bytes of [member] (inside [unit]) already received, derived from the unit
  /// bitmap: sum the received chunks that overlap the member's byte range,
  /// clipped to that range. Enables per-original-file progress under bundling.
  int receivedBytesOfMember(SwarmReceivingFile unit, SwarmMember member) {
    if (member.size == 0) return 0;
    final cs = unit.plan.chunkSize;
    final mStart = member.offset;
    final mEnd = member.offset + member.size;
    final first = mStart ~/ cs;
    final last = (mEnd - 1) ~/ cs;
    var sum = 0;
    for (var c = first; c <= last; c++) {
      if (!unit.bitmap.has(c)) continue;
      final cStart = c * cs;
      final cEnd = cStart + unit.plan.chunkLength(c);
      final oStart = mStart > cStart ? mStart : cStart;
      final oEnd = mEnd < cEnd ? mEnd : cEnd;
      if (oStart < oEnd) sum += oEnd - oStart;
    }
    return sum;
  }
}
