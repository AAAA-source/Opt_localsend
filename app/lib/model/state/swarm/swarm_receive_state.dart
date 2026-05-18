import 'dart:async';
import 'dart:io';

import 'package:common/model/device.dart';
import 'package:common/model/dto/file_dto.dart';
import 'package:common/model/dto/swarm/bitmap_dto.dart';
import 'package:common/model/dto/swarm/chunk_plan_dto.dart';
import 'package:common/model/dto/swarm/peer_info.dart';
import 'package:common/model/session_status.dart';

/// Per-file runtime state on the receiver side of a swarm session.
class SwarmReceivingFile {
  final FileDto file;
  final ChunkPlanDto plan;
  final String token; // self-issued, returned to sender via prepare-swarm response
  final String desiredName; // local filename (may differ from FileDto.fileName)
  final String? path; // absolute path on disk, set after open
  final BitmapDto bitmap; // mutable: in-place flipped as chunks arrive
  // How many chunks were obtained from each source. For demo / proposal verification.
  final Map<String, int> chunksFromSource; // 'sender' or peer fingerprint -> count
  final RandomAccessFile? raf; // open handle for offset writes
  final String? errorMessage;

  const SwarmReceivingFile({
    required this.file,
    required this.plan,
    required this.token,
    required this.desiredName,
    required this.path,
    required this.bitmap,
    required this.chunksFromSource,
    required this.raf,
    required this.errorMessage,
  });

  SwarmReceivingFile copyWith({
    String? path,
    String? errorMessage,
    RandomAccessFile? raf,
  }) {
    return SwarmReceivingFile(
      file: file,
      plan: plan,
      token: token,
      desiredName: desiredName,
      path: path ?? this.path,
      bitmap: bitmap,
      chunksFromSource: chunksFromSource,
      raf: raf ?? this.raf,
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
    );
  }

  /// Total bytes across all files in this session.
  int get totalBytes => files.values.fold<int>(0, (a, f) => a + f.file.size);

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
}
