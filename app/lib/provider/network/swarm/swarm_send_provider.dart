import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:common/api_route_builder.dart';
import 'package:common/constants.dart';
import 'package:common/model/device.dart';
import 'package:common/model/dto/file_dto.dart';
import 'package:common/model/dto/info_register_dto.dart';
import 'package:common/model/dto/multicast_dto.dart';
import 'package:common/model/dto/swarm/announce_dto.dart';
import 'package:common/model/dto/swarm/bitmap_dto.dart';
import 'package:common/model/dto/swarm/chunk_plan_dto.dart';
import 'package:common/model/dto/swarm/peer_info.dart';
import 'package:common/model/dto/swarm/prepare_swarm_request_dto.dart';
import 'package:common/model/dto/swarm/prepare_swarm_response_dto.dart';
import 'package:common/model/dto/swarm/relay_plan_dto.dart';
import 'package:common/model/file_type.dart';
import 'package:common/model/session_status.dart';
import 'package:common/src/task/swarm/chunk_planner.dart';
import 'package:common/src/task/swarm/relay_planner.dart';
import 'package:localsend_app/model/cross_file.dart';
import 'package:localsend_app/model/state/swarm/swarm_send_state.dart';
import 'package:localsend_app/provider/device_info_provider.dart';
import 'package:localsend_app/provider/network/swarm/swarm_http_client.dart';
import 'package:localsend_app/provider/security_provider.dart';
import 'package:localsend_app/provider/settings_provider.dart';
import 'package:logging/logging.dart';
import 'package:refena_flutter/refena_flutter.dart';
import 'package:uuid/uuid.dart';

const _uuid = Uuid();
final _logger = Logger('SwarmSend');

/// Time after which the sender will re-issue a chunk that no peer has yet acked.
const _altruisticTimeout = Duration(seconds: 5);

/// Per-target concurrency for chunk uploads. Keeping it small avoids HOL blocking.
const _perTargetConcurrency = 2;

/// ---------------------------------------------------------------------------
// EWMA-based adaptive chunk scheduler
// ----------------------------------------------------------------------------

class _PeerPerformance {
  final String fingerprint;
  double ewmaThroughput = 0.0; // Bytes/ms
  bool _isInitialized = false;
  final double alpha = 0.3; // Smoothing factor, raise it toward 1.0 to react faster to link changes

  _PeerPerformance(this.fingerprint);

  void update(int bytes, int elapsedMs) {
    final currentMs = max(elapsedMs, 1);
    final currentThroughput = bytes / currentMs;

    if (!_isInitialized) {
      ewmaThroughput = currentThroughput;
      _isInitialized = true;
    } else {
      ewmaThroughput = alpha * currentThroughput + (1 - alpha) * ewmaThroughput;
    }
  }
}

class _AdaptiveChunkScheduler {
  /// Unassigned chunks waiting to be claimed by any peer worker.
  final List<_ChunkRef> _globalQueue = [];

  /// Chunks that have been claimed but not yet acknowledged. 
  /// Composite key '$fileId:$chunkIndex' to avoid collisions across multiple files.
  final Map<String, _ChunkRef> _inflightChunks = {};

  /// Per-peer throughput estimates, keyed by fingerprint.
  final Map<String, _PeerPerformance> _peers = {};

  _AdaptiveChunkScheduler(List<_ChunkRef> allChunks) {
    _globalQueue.addAll(allChunks);
  }

  /// Runtime work-stealing: any peer can call nextChunk() to claim the next available chunk.
  /// Claims the next available chunk for [fingerprint]
  /// The fingerprint parameter is retained for future priority ordering
  /// (e.g. prefer larger chunks for higher throughput peers)
  _ChunkRef? nextChunk(String fingerprint) {
    if (_globalQueue.isEmpty) return null;
    final task = _globalQueue.removeAt(0);
    _inflightChunks['${task.fileId}:${task.chunkIndex}'] = task;
    return task;
  }

  /// Recode a successful ACK from [fingerprint] and update its EWMA estimate.
  void recordAck(String fingerprint, _ChunkRef task, int bytes, int elapsedMs) {
    _inflightChunks.remove('${task.fileId}:${task.chunkIndex}');
    _peers.putIfAbsent(fingerprint, () => _PeerPerformance(fingerprint)).update(bytes, elapsedMs);
  }

  /// If a chunk is reported as failed by a peer, re-queue it for others to claim.
  void reportFailure(_ChunkRef task) {
    final key = '${task.fileId}:${task.chunkIndex}';
    if (_inflightChunks.containsKey(key)) {
      _inflightChunks.remove(key);
      _globalQueue.insert(0, task);
    }
  }

  /// Returns the current EWMA throughput estimate for [fingerprint]
  /// or null if no ACK has been recorded for that peer yet.
  double? throughputOf(String fingerprint) {
    final p = _peers[fingerprint];
    return (p != null && p._isInitialized) ? p.ewmaThroughput : null;
  }

  /// True when every chunk has been successfully ACKed by some peer.
  bool isDone => _globalQueue.isEmpty && _inflightChunks.isEmpty;
}


/// Sender-side coordinator for the swarm (HopSwift) transfer.
///
/// Lifecycle:
///   1. `startSwarmSession(targets, files)` — pre-hash all files (Phase A)
///   2. POST `/v3/prepare-swarm` to every target, collect tokens
///   3. Run round-robin chunk dispatcher (chunk k -> peers[k mod M])
///   4. Listen for `/v3/announce` from receivers; on timeout fallback-send to laggards
final swarmSendProvider = NotifierProvider<SwarmSendNotifier, Map<String, SwarmSendState>>((ref) {
  return SwarmSendNotifier();
});

class SwarmSendNotifier extends Notifier<Map<String, SwarmSendState>> {
  /// Per-session: fingerprint -> (fileId -> BitmapDto)
  /// Populated as receivers POST /v3/announce.
  final Map<String, Map<String, Map<String, BitmapDto>>> _peerBitmaps = {};

  /// Per-session HTTP client (Rhttp).
  final Map<String, SwarmHttpClient> _clients = {};

  /// Per-session: timestamps of last upload attempt per (fileId, chunkIndex).
  final Map<String, Map<String, Map<int, DateTime>>> _lastAttempt = {};

  /// Per-session controller used to stop the fallback loop.
  final Map<String, Completer<void>> _stopCompleters = {};

  @override
  Map<String, SwarmSendState> init() => {};

  /// Starts a swarm session and returns its id. The id is registered into
  /// [state] synchronously before any awaits, so callers can read it
  /// immediately after this call resolves on the next microtask.
  Future<String?> startSwarmSession({
    required List<Device> targets,
    required List<CrossFile> files,
  }) async {
    if (targets.isEmpty || files.isEmpty) return null;
    final sessionId = _uuid.v4();
    _logger.info('Starting swarm session $sessionId to ${targets.length} targets, ${files.length} files');

    final originDevice = ref.read(deviceFullInfoProvider);
    final settings = ref.read(settingsProvider);
    final chunkSize = settings.swarmChunkSize;

    // Build FileDto map. Files without on-disk path (web bytes) are unsupported in v1.
    final fileDtos = <String, FileDto>{};
    final filePaths = <String, String>{};
    for (final f in files) {
      final id = _uuid.v4();
      if (f.path == null) {
        _logger.warning('Skipping ${f.name}: swarm requires a file path');
        continue;
      }
      fileDtos[id] = FileDto(
        id: id,
        fileName: f.name,
        size: f.size,
        fileType: f.fileType,
        hash: null,
        preview: f.fileType == FileType.text && f.bytes != null ? utf8.decode(f.bytes!) : null,
        metadata: f.lastModified != null || f.lastAccessed != null ? FileMetadata(lastModified: f.lastModified, lastAccessed: f.lastAccessed) : null,
      );
      filePaths[id] = f.path!;
    }
    if (fileDtos.isEmpty) {
      _logger.warning('Swarm session aborted: no eligible files');
      return null;
    }

    final nowMs = DateTime.now().millisecondsSinceEpoch;
    final initialState = SwarmSendState(
      sessionId: sessionId,
      status: SessionStatus.waiting,
      targets: targets,
      files: fileDtos,
      plans: const {},
      tokens: const {},
      sentToPrimary: {for (final id in fileDtos.keys) id: <int>{}},
      startTime: nowMs,
      endTime: null,
      errorMessage: null,
      prepareProgress: 0,
      prepareStartTime: nowMs,
      prepareEndTime: null,
      firstChunkSentAt: null,
      lastChunkSentAt: null,
      peerCompleteTime: const {},
    );
    state = {...state, sessionId: initialState};

    // Phase A: pre-hash all files
    final plans = <String, ChunkPlanDto>{};
    final totalBytes = fileDtos.values.fold<int>(0, (a, f) => a + f.size);
    var hashedBytes = 0;
    try {
      for (final entry in fileDtos.entries) {
        final fid = entry.key;
        final plan = await planChunks(
          fileId: fid,
          filePath: filePaths[fid]!,
          chunkSize: chunkSize,
          onProgress: (p) {
            final overall = totalBytes == 0 ? 1.0 : (hashedBytes + p * entry.value.size) / totalBytes;
            state = _patch(sessionId, (s) => s.copyWith(prepareProgress: overall));
          },
        );
        plans[fid] = plan;
        hashedBytes += entry.value.size;
        _logger.info('Hashed ${entry.value.fileName}: ${plan.totalChunks} chunks');
      }
    } catch (e, st) {
      _logger.severe('Pre-hashing failed', e, st);
      state = _patch(
        sessionId,
        (s) => s.copyWith(
          status: SessionStatus.finishedWithErrors,
          errorMessage: 'Hashing failed: $e',
        ),
      );
      return sessionId;
    }
    state = _patch(
      sessionId,
      (s) => s.copyWith(
        plans: plans,
        prepareProgress: 1,
        prepareEndTime: DateTime.now().millisecondsSinceEpoch,
      ),
    );

    // Compute bandwidth-aware overlay tree topology
    final List<PeerBandwidthHint> bwHints = [];
    final peerFingerprints = targets.map((t) => t.fingerprint).toList();
    final relayPlanMap = RelayPlanner.computeTree(
      sessionId: sessionId,
      peerFingerprints: peerFingerprints,
      bandwidthHints: bwHints,
      maxFanOut: _perTargetConcurrency,
    );

    // Phase B: POST prepare-swarm to every target in parallel
    final security = ref.read(securityProvider);
    final http = SwarmHttpClient.create(security);
    _clients[sessionId] = http;

    final peers = <PeerInfo>[
      for (final t in targets)
        PeerInfo(
          fingerprint: t.fingerprint,
          ip: t.ip ?? '',
          port: t.port,
          https: t.https,
        ),
    ];

    final info = InfoRegisterDto(
      // Use the regular protocol version here. Swarm capability is implicit by
      // hitting /v3/prepare-swarm; we don't want to push receivers' target()
      // helper onto the v3 path for unrelated routes (cancel/info/register).
      alias: originDevice.alias,
      version: protocolVersion,
      deviceModel: originDevice.deviceModel,
      deviceType: originDevice.deviceType,
      fingerprint: originDevice.fingerprint,
      port: originDevice.port,
      protocol: originDevice.https ? ProtocolType.https : ProtocolType.http,
      download: originDevice.download,
    );

    final tokens = <String, Map<String, String>>{};
    final acceptFutures = <Future<bool>>[];
    for (var i = 0; i < targets.length; i++) {
      final idx = i;
      final t = targets[i];
      final relayPlan = relayPlanMap[t.fingerprint];
      acceptFutures.add(
        _prepareOnTarget(
              http: http,
              target: t,
              request: PrepareSwarmRequestDto(
                info: info,
                sessionId: sessionId,
                files: fileDtos,
                plans: plans,
                peers: peers,
                myIndex: idx,
                relayPlan: relayPlan,
              ),
            )
            .then((tokensForTarget) {
              if (tokensForTarget != null && tokensForTarget.isNotEmpty) {
                tokens[t.fingerprint] = tokensForTarget;
                return true;
              }
              return false;
            })
            .catchError((e, st) {
              _logger.warning('prepare-swarm failed for ${t.alias} (${t.ip})', e, st);
              return false;
            }),
      );
    }
    final accepts = await Future.wait(acceptFutures);
    final acceptedTargets = <Device>[
      for (var i = 0; i < targets.length; i++)
        if (accepts[i]) targets[i],
    ];
    if (acceptedTargets.isEmpty) {
      state = _patch(
        sessionId,
        (s) => s.copyWith(
          status: SessionStatus.declined,
          endTime: DateTime.now().millisecondsSinceEpoch,
        ),
      );
      return sessionId;
    }
    state = _patch(
      sessionId,
      (s) => s.copyWith(
        status: SessionStatus.sending,
        tokens: tokens,
      ),
    );

    // Phase C: dispatch chunks round-robin and run altruistic fallback loop
    final stop = Completer<void>();
    _stopCompleters[sessionId] = stop;
    _peerBitmaps.putIfAbsent(sessionId, () => {});
    _lastAttempt.putIfAbsent(sessionId, () => {});

    await _dispatchAndWatch(
      sessionId: sessionId,
      http: http,
      acceptedTargets: acceptedTargets,
      filePaths: filePaths,
      stop: stop,
    );

    final endMs = DateTime.now().millisecondsSinceEpoch;
    // Final pass: any target without a recorded completion gets stamped now.
    _recordPeerCompletions(sessionId: sessionId, acceptedTargets: acceptedTargets);
    state = _patch(sessionId, (s) {
      final pct = {...s.peerCompleteTime};
      for (final t in acceptedTargets) {
        pct.putIfAbsent(t.fingerprint, () => endMs);
      }
      return s.copyWith(
        status: SessionStatus.finished,
        endTime: endMs,
        peerCompleteTime: pct,
      );
    });
    _emitSenderBenchmarkLog(sessionId);
    _logger.info('Swarm session $sessionId completed');
    return sessionId;
  }

  /// Emit a single-line CSV-friendly summary at the `SENDBENCH` tag so
  /// `flutter logs | grep SENDBENCH` is enough to grab the proposal's metric.
  /// Schema: SENDBENCH,sessionId,mode,M,totalBytes,totalChunks,prepareMs,sendMs,lastReceiverMs,peerTimings(fp@ms;..)
  void _emitSenderBenchmarkLog(String sessionId) {
    final ss = state[sessionId];
    if (ss == null) return;
    final prepareMs = (ss.prepareStartTime != null && ss.prepareEndTime != null) ? ss.prepareEndTime! - ss.prepareStartTime! : -1;
    final sendMs = (ss.firstChunkSentAt != null && ss.lastChunkSentAt != null) ? ss.lastChunkSentAt! - ss.firstChunkSentAt! : -1;
    final lastReceiverMs = ss.peerCompleteTime.values.isEmpty ? -1 : ss.peerCompleteTime.values.reduce((a, b) => a > b ? a : b) - ss.startTime;
    final peerTimings = ss.peerCompleteTime.entries
        .map((e) => '${e.key.substring(0, e.key.length < 8 ? e.key.length : 8)}@${e.value - ss.startTime}')
        .join(';');
    _logger.info(
      'SENDBENCH,$sessionId,swarm,${ss.targets.length},${ss.totalBytes},${ss.totalChunks},'
      '$prepareMs,$sendMs,$lastReceiverMs,$peerTimings',
    );
  }

  Future<Map<String, String>?> _prepareOnTarget({
    required SwarmHttpClient http,
    required Device target,
    required PrepareSwarmRequestDto request,
  }) async {
    final url = 'https://${target.ip}:${target.port}${ApiRoute.prepareSwarm.v3}';
    final fallbackUrl = 'http://${target.ip}:${target.port}${ApiRoute.prepareSwarm.v3}';
    final body = await http.postJson(
      url: target.https ? url : fallbackUrl,
      body: request.toJson(),
    );
    final decoded = decodeJsonBody(body);
    final resp = PrepareSwarmResponseDto.fromJson(decoded);
    return resp.tokens;
  }

  /// Reports a peer bitmap announcement. Called by the v3 receive controller
  /// when an /v3/announce POST lands.
  void onAnnounce({required String sessionId, required AnnounceDto dto}) {
    final byPeer = _peerBitmaps[sessionId] ??= {};
    final byFile = byPeer[dto.fingerprint] ??= {};
    byFile[dto.bitmap.fileId] = dto.bitmap;
  }

  Future<void> _dispatchAndWatch({
    required String sessionId,
    required SwarmHttpClient http,
    required List<Device> acceptedTargets,
    required Map<String, String> filePaths,
    required Completer<void> stop,
  }) async {
    final ss = state[sessionId];
    if (ss == null) return;
    final M = acceptedTargets.length;
    final lastAttempt = _lastAttempt[sessionId]!;

    /// Build a global task pool from all chunks across all files. 
    final allChunks = <_ChunkRef>[];
    for (final entry in ss.plans.entries) {
      final plan = entry.value;
      for (var k = 0; k < plan.totalChunks; k++) {
        allChunks.add(_ChunkRef(fileId: plan.fileId, chunkIndex: k));
      }
    }

    final scheduler = _AdaptiveChunkScheduler(allChunks);
    _logger.info('Initilized AdaptiveChunkScheduler with ${allChunks.length} total tasks');

    /// Filter and only upload to primary relay nodes
    /// Senders only feeds parents instead of dispatching to all targets
    final primaryRelayTargets = acceptedTargets.where((target) {
      final peerPlan = relayPlanMap[target.fingerprint];
      return peerPlan != null && peerPlan.parentFingerprint == null;
    }).toList();
    _logger.info('Overlay tree filters active: feeding ${primaryRelayTargets.length}/${acceptedTargets.length} root targets directly');
    
    /// Dynamic heterogeneous chunk planning
    /// Spawn _perTargetConcurrency workers per peer; all share the same scheduler. 
    /// This allows parallel uploads without head-of-line (HOL) blocking on slow peers
    final futures = <Future<void>>[];
    for (final target in primaryRelayTargets) {
      for (var w = 0; w < _perTargetConcurrency; w++) {
        futures.add(
          _uploadWorker(
            sessionId: sessionId, 
            http: http, 
            target: target, 
            scheduler: scheduler, 
            filePaths: filePaths, 
            lastAttempt: lastAttempt, 
          ), 
        );
      }
    }

    /// Simple round-robin assignment
    // final futures = <Future<void>>[];
    // for (var i = 0; i < acceptedTargets.length; i++) {
    //   final target = acceptedTargets[i];
    //   final assignedChunks = <_ChunkRef>[];
    //   for (final entry in ss.plans.entries) {
    //     final plan = entry.value;
    //     for (var k = 0; k < plan.totalChunks; k++) {
    //       if (k % M == i) {
    //         assignedChunks.add(_ChunkRef(fileId: plan.fileId, chunkIndex: k));
    //       }
    //     }
    //   }
    //   _logger.info('Target ${target.alias}: ${assignedChunks.length} primary chunks');

    //   // spawn _perTargetConcurrency workers per target.
    //   // Dart is single-threaded, so removeAt(0) is safe without an explicit mutex.
    //   for (var w = 0; w < _perTargetConcurrency; w++) {
    //     futures.add(
    //       _uploadWorker(
    //         sessionId: sessionId,
    //         http: http,
    //         target: target,
    //         queue: assignedChunks,
    //         filePaths: filePaths,
    //         lastAttempt: lastAttempt,
    //       ),
    //     );
    //   }
    // }

    // Altruistic fallback + completion ticker (single loop, runs until all peers full)
    final watcher = _fallbackLoop(
      sessionId: sessionId,
      http: http,
      acceptedTargets: acceptedTargets,
      filePaths: filePaths,
      lastAttempt: lastAttempt,
      stop: stop,
    );

    // Wait for all primary uploads to finish first.
    await Future.wait(futures);

    // Primary uploads done — signal the fallback loop to finish after its
    // current tick, rather than waiting indefinitely for announce packets.
    final stopper = _stopCompleters[sessionId];
    if (stopper != null && !stopper.isCompleted) stopper.complete();

    // Then wait for the fallback/completion watcher to declare done.
    await watcher;
  }

  Future<void> _uploadWorker({
    required String sessionId,
    required SwarmHttpClient http,
    required Device target,
    required _AdaptiveChunkScheduler scheduler, 
    required Map<String, String> filePaths,
    required Map<String, Map<int, DateTime>> lastAttempt,
  }) async {
    while (true) {
      final next = scheduler.nextChunk(target.fingerprint);
      if (next == null) break; // no more work

      // if (queue.isEmpty) break;
      // final next = queue.removeAt(0);

      final stopwatch = Stopwatch()..start();
      int transmittedBytes = 0;
      
      try {
        transmittedBytes = await _uploadChunk(
          sessionId: sessionId,
          http: http,
          target: target,
          chunkRef: next,
          filePath: filePaths[next.fileId]!,
          lastAttempt: lastAttempt,
        );
        stopwatch.stop();

        scheduler.recordAck(
          target.fingerprint,
          next,
          transmittedBytes,
          stopwatch.elapsedMilliseconds,
        );
      } catch (e, st) {
        _logger.warning('Upload failed for ${target.alias} ${next.fileId}#${next.chunkIndex}', e, st);
        /// On failure, re-queue the chunk for others to claim.
        scheduler.reportFailure(next);
        await Future.delayed(const Duration(milliseconds: 200)); // brief backoff before retrying
      }
    }
  }

  /// Returns the number of bytes uploaded for this chunk for EWMA calculation
  Future<int> _uploadChunk({
    required String sessionId,
    required SwarmHttpClient http,
    required Device target,
    required _ChunkRef chunkRef,
    required String filePath,
    required Map<String, Map<int, DateTime>> lastAttempt,
  }) async {
    final ss = state[sessionId];
    if (ss == null) return 0;
    final plan = ss.plans[chunkRef.fileId];
    if (plan == null) return 0;
    final token = ss.tokens[target.fingerprint]?[chunkRef.fileId];
    if (token == null) return 0;

    // Read slice from disk
    final raf = await File(filePath).open();
    Uint8List bytes;
    try {
      await raf.setPosition(plan.chunkOffset(chunkRef.chunkIndex));
      final length = plan.chunkLength(chunkRef.chunkIndex);
      bytes = await raf.read(length);
      if (bytes.length != length) {
        throw StateError('Short read at chunk ${chunkRef.chunkIndex}');
      }
    } finally {
      await raf.close();
    }

    final url = '${target.https ? 'https' : 'http'}://${target.ip}:${target.port}${ApiRoute.uploadChunk.v3}';
    await http.postBytes(
      url: url,
      query: {
        'sessionId': sessionId,
        'fileId': chunkRef.fileId,
        'chunkIndex': '${chunkRef.chunkIndex}',
        'token': token,
      },
      bytes: bytes,
    );
    (lastAttempt[chunkRef.fileId] ??= {})[chunkRef.chunkIndex] = DateTime.now();
    state = _patch(sessionId, (s) {
      final next = {...s.sentToPrimary};
      next[chunkRef.fileId] = {...(next[chunkRef.fileId] ?? const <int>{}), chunkRef.chunkIndex};
      final nowMs = DateTime.now().millisecondsSinceEpoch;
      return s.copyWith(
        sentToPrimary: next,
        firstChunkSentAt: s.firstChunkSentAt ?? nowMs,
        lastChunkSentAt: nowMs,
      );
    });
    return bytes.length;
  }

  /// Periodically scans peer bitmaps; if a chunk's primary upload was issued
  /// more than [_altruisticTimeout] ago AND no peer reports having it, the
  /// sender uploads it to whichever receiver is most-laggard for that file.
  Future<void> _fallbackLoop({
    required String sessionId,
    required SwarmHttpClient http,
    required List<Device> acceptedTargets,
    required Map<String, String> filePaths,
    required Map<String, Map<int, DateTime>> lastAttempt,
    required Completer<void> stop,
  }) async {
    while (!stop.isCompleted) {
      try {
        await _fallbackTick(
          sessionId: sessionId,
          http: http,
          acceptedTargets: acceptedTargets,
          filePaths: filePaths,
          lastAttempt: lastAttempt,
        );
      } catch (e, st) {
        _logger.warning('Fallback tick error', e, st);
      }
      // Record per-peer completion timestamps as bitmaps fill up.
      _recordPeerCompletions(sessionId: sessionId, acceptedTargets: acceptedTargets);
      if (_allPeersComplete(sessionId: sessionId, acceptedTargets: acceptedTargets)) {
        if (!stop.isCompleted) stop.complete();
        return;
      }
      await Future.any([
        Future.delayed(const Duration(seconds: 2)),
        stop.future,
      ]);
    }
  }

  /// For each accepted target whose latest reported bitmaps cover all chunks
  /// of all files, stamp the completion time once (first observation only).
  void _recordPeerCompletions({
    required String sessionId,
    required List<Device> acceptedTargets,
  }) {
    final ss = state[sessionId];
    if (ss == null) return;
    final byPeer = _peerBitmaps[sessionId] ?? const {};
    final next = {...ss.peerCompleteTime};
    var changed = false;
    for (final t in acceptedTargets) {
      if (next.containsKey(t.fingerprint)) continue;
      final perFile = byPeer[t.fingerprint];
      if (perFile == null) continue;
      var fullCount = 0;
      for (final plan in ss.plans.values) {
        final bm = perFile[plan.fileId];
        if (bm != null && bm.receivedCount >= plan.totalChunks) fullCount++;
      }
      if (fullCount == ss.plans.length && ss.plans.isNotEmpty) {
        next[t.fingerprint] = DateTime.now().millisecondsSinceEpoch;
        changed = true;
      }
    }
    if (changed) {
      state = _patch(sessionId, (s) => s.copyWith(peerCompleteTime: next));
    }
  }

  /// True iff every accepted target's bitmap covers every chunk of every file.
  bool _allPeersComplete({
    required String sessionId,
    required List<Device> acceptedTargets,
  }) {
    final ss = state[sessionId];
    if (ss == null) return false;
    final byPeer = _peerBitmaps[sessionId] ?? const {};
    for (final t in acceptedTargets) {
      final perFile = byPeer[t.fingerprint];
      if (perFile == null) return false;
      for (final plan in ss.plans.values) {
        final bm = perFile[plan.fileId];
        if (bm == null) return false;
        if (bm.receivedCount < plan.totalChunks) return false;
      }
    }
    return true;
  }

  Future<void> _fallbackTick({
    required String sessionId,
    required SwarmHttpClient http,
    required List<Device> acceptedTargets,
    required Map<String, String> filePaths,
    required Map<String, Map<int, DateTime>> lastAttempt,
  }) async {
    final ss = state[sessionId];
    if (ss == null) return;
    final byPeer = _peerBitmaps[sessionId] ?? const {};

    for (final plan in ss.plans.values) {
      final fileId = plan.fileId;
      for (var k = 0; k < plan.totalChunks; k++) {
        // 1) Already uploaded by sender directly?
        final sentSet = ss.sentToPrimary[fileId] ?? const <int>{};
        final sentDirect = sentSet.contains(k);
        // 2) Does any peer report having it?
        var anyPeerHas = false;
        for (final byFile in byPeer.values) {
          final bm = byFile[fileId];
          if (bm != null && bm.has(k)) {
            anyPeerHas = true;
            break;
          }
        }
        if (anyPeerHas) continue;

        // 3) If sent directly already, check if it's been long enough that we
        //    should assume the receiver never got it.
        final attempt = lastAttempt[fileId]?[k];
        if (sentDirect && attempt != null && DateTime.now().difference(attempt) < _altruisticTimeout) {
          continue; // still within grace period
        }

        // 4) Pick the target most lacking this file (fewest received chunks).
        Device? laggard;
        var laggardCount = -1;
        for (final t in acceptedTargets) {
          final bm = byPeer[t.fingerprint]?[fileId];
          final count = bm?.receivedCount ?? 0;
          if (laggardCount < 0 || count < laggardCount) {
            laggard = t;
            laggardCount = count;
          }
        }
        if (laggard == null) continue;
        _logger.fine('Fallback resend $fileId#$k → ${laggard.alias}');
        await _uploadChunk(
          sessionId: sessionId,
          http: http,
          target: laggard,
          chunkRef: _ChunkRef(fileId: fileId, chunkIndex: k),
          filePath: filePaths[fileId]!,
          lastAttempt: lastAttempt,
        );
      }
    }
  }

  Map<String, SwarmSendState> _patch(String sessionId, SwarmSendState Function(SwarmSendState) f) {
    final cur = state[sessionId];
    if (cur == null) return state;
    return {...state, sessionId: f(cur)};
  }

  void closeSession(String sessionId) {
    _clients.remove(sessionId)?.dispose();
    _stopCompleters.remove(sessionId)?.complete();
    _peerBitmaps.remove(sessionId);
    _lastAttempt.remove(sessionId);
    state = {...state}..remove(sessionId);
  }
}

class _ChunkRef {
  final String fileId;
  final int chunkIndex;
  const _ChunkRef({required this.fileId, required this.chunkIndex});
}
