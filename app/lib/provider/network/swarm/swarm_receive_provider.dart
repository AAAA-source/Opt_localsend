import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:collection/collection.dart';
import 'package:common/api_route_builder.dart';
import 'package:common/model/dto/swarm/announce_dto.dart';
import 'package:common/model/dto/swarm/bitmap_dto.dart';
import 'package:common/model/dto/swarm/peer_info.dart';
import 'package:common/model/file_status.dart';
import 'package:common/model/session_status.dart';
import 'package:crypto/crypto.dart';
import 'package:localsend_app/model/state/swarm/swarm_receive_state.dart';
import 'package:localsend_app/provider/network/swarm/swarm_http_client.dart';
import 'package:localsend_app/provider/security_provider.dart';
import 'package:localsend_app/util/native/file_saver.dart';
import 'package:localsend_app/util/native/raf_pool.dart';
import 'package:logging/logging.dart';
import 'package:refena_flutter/refena_flutter.dart';

final _logger = Logger('SwarmReceive');

/// Holds the *currently active* incoming swarm session.
/// Mirrors the v2 receive flow: at most one inbound session at a time.
final swarmReceiveProvider = NotifierProvider<SwarmReceiveNotifier, SwarmReceiveState?>((ref) {
  return SwarmReceiveNotifier();
});

class SwarmReceiveNotifier extends Notifier<SwarmReceiveState?> {
  SwarmHttpClient? _client;
  Completer<void>? _pullStop;
  // Track which sources contributed which chunk indices, used for the UI breakdown.
  final Map<String, Map<int, String>> _chunkSource = {}; // fileId -> idx -> source label

  // --- Phase 1: throttled UI notification --------------------------------
  // Chunk data (bitmap bits) is mutated in place; we only need to *notify*
  // listeners. Doing a copyWith per chunk is O(files) per write → O(N²) for
  // many small files. Instead we coalesce notifications to ~10 Hz.
  Timer? _uiFlushTimer;
  static const _uiFlushInterval = Duration(milliseconds: 100);
  int? _firstChunkReceivedAt;
  int? _lastChunkReceivedAt;

  // --- Phase 2: coalesced bitmap announces -------------------------------
  // Per-chunk broadcastBitmap() POSTed one file's bitmap to sender + every
  // peer. With N small files that is N·M serialized POSTs. We mark files dirty
  // and flush all of them in a single multi-bitmap AnnounceDto per target.
  final Set<String> _dirtyBitmaps = {};
  Timer? _broadcastTimer;
  static const _broadcastInterval = Duration(milliseconds: 250);

  // --- Phase 1: relay-push dedup kept off the UI state -------------------
  // fileId -> set of chunk indices already pushed to children. Read only by
  // pushToChildren(); no reason to round-trip it through Riverpod state.
  final Map<String, Set<int>> _pushedToChildren = {};

  // --- Phase 3: bounded RAF lifecycle ------------------------------------
  // We no longer open a descriptor for every file up front (that exhausts fds
  // for a folder of thousands of files). Instead each file's write handle is
  // opened lazily on its first chunk and closed the moment it is complete; the
  // FileMode.write handle (O_RDWR) also serves relay reads while open. Once a
  // file is closed, relay reads of it go through a bounded read pool.
  final Map<String, RandomAccessFile> _writeRafs = {}; // fileId -> open write handle
  final Map<String, int> _recvCount = {}; // fileId -> unique chunks written
  final Map<String, Future<void>> _fileLocks = {}; // fileId -> serialization lock
  final RafReadPool _relayReadPool = RafReadPool();

  @override
  SwarmReceiveState? init() => null;

  /// Serializes seek+read/write on a single file's handle so concurrent chunk
  /// writes and relay reads don't clobber each other's position.
  Future<T> _withFileLock<T>(String fileId, Future<T> Function() fn) async {
    final prev = _fileLocks[fileId] ?? Future<void>.value();
    final completer = Completer<void>();
    _fileLocks[fileId] = completer.future;
    await prev;
    try {
      return await fn();
    } finally {
      completer.complete();
      // Awaiting the already-complete future just discards it cleanly.
      if (identical(_fileLocks[fileId], completer.future)) await _fileLocks.remove(fileId);
    }
  }

  bool get hasActiveSession {
    final s = state;
    if (s == null) return false;
    return s.status == SessionStatus.waiting || s.status == SessionStatus.sending;
  }

  /// Replaces the current swarm session. Caller must guarantee no active session.
  void setSession(SwarmReceiveState s) {
    // Reset throttle/announce bookkeeping for the fresh session.
    _uiFlushTimer?.cancel();
    _uiFlushTimer = null;
    _broadcastTimer?.cancel();
    _broadcastTimer = null;
    _dirtyBitmaps.clear();
    _pushedToChildren.clear();
    _writeRafs.clear();
    _recvCount.clear();
    _fileLocks.clear();
    _firstChunkReceivedAt = null;
    _lastChunkReceivedAt = null;
    state = s;
  }

  // --- Phase 1: throttled UI flush ---------------------------------------
  void _scheduleUiFlush() {
    if (_uiFlushTimer != null) return;
    _uiFlushTimer = Timer(_uiFlushInterval, () {
      _uiFlushTimer = null;
      _flushUiNow();
    });
  }

  /// Publishes a fresh state snapshot so progress widgets rebuild. Bitmaps are
  /// already up to date (mutated in place); this only triggers notification.
  void _flushUiNow() {
    final s = state;
    if (s == null) return;
    state = s.copyWith(
      files: {...s.files},
      firstChunkReceivedAt: _firstChunkReceivedAt,
      lastChunkReceivedAt: _lastChunkReceivedAt,
    );
  }

  void mutate(SwarmReceiveState Function(SwarmReceiveState s) f) {
    final cur = state;
    if (cur == null) return;
    state = f(cur);
  }

  /// User accept callback. [selection] maps fileId -> desired local filename;
  /// null = decline.
  void acceptOrDecline(Map<String, String>? selection) {
    final controller = state?.responseHandler;
    if (controller == null || controller.isClosed) return;
    controller.add(selection);
    controller.close(); // ignore: discarded_futures
  }

  /// Called by the v3 controller when a peer POSTs /v3/announce.
  void onAnnounce(AnnounceDto announce) {
    final s = state;
    if (s == null) return;
    final byPeer = {...s.peerBitmaps};
    final perFile = {...?byPeer[announce.fingerprint]};
    for (final bm in announce.bitmaps) {
      perFile[bm.fileId] = bm;
    }
    byPeer[announce.fingerprint] = perFile;
    state = s.copyWith(peerBitmaps: byPeer);
  }

  /// Persists a chunk to disk and updates bitmap.
  /// Returns true on success, false on hash mismatch or write error.
  Future<bool> writeChunk({
    required String fileId,
    required int chunkIndex,
    required Uint8List bytes,
    required String source, // 'sender' or fingerprint
  }) async {
    final s = state;
    if (s == null) return false;
    final rf = s.files[fileId];
    if (rf == null) return false;
    if (rf.bitmap.has(chunkIndex)) return true; // duplicate, ignore
    final expectedHash = rf.plan.sha256PerChunk[chunkIndex];
    final actual = sha256.convert(bytes).toString();
    if (actual != expectedHash) {
      _logger.warning('Hash mismatch ${rf.file.fileName}#$chunkIndex from $source');
      return false;
    }
    final ok = await _withFileLock(fileId, () async {
      // Re-check under the lock: a concurrent writer may have just landed it.
      if (rf.bitmap.has(chunkIndex)) return true;
      final RandomAccessFile raf;
      try {
        raf = await _openWriteRaf(rf);
      } catch (e, st) {
        _logger.severe('Open write handle failed for ${rf.file.fileName}', e, st);
        return false;
      }
      try {
        await raf.setPosition(rf.plan.chunkOffset(chunkIndex));
        await raf.writeFrom(bytes);
      } catch (e, st) {
        _logger.severe('Write failed at $fileId#$chunkIndex', e, st);
        return false;
      }
      // Update bitmap in-place
      final byte = chunkIndex >> 3;
      rf.bitmap.bits[byte] |= (1 << (chunkIndex & 7));
      rf.chunksFromSource[source] = (rf.chunksFromSource[source] ?? 0) + 1;
      (_chunkSource[fileId] ??= {})[chunkIndex] = source;
      // Close the write handle the moment the file is complete, so the open-fd
      // count tracks the in-progress working set, not the total file count.
      final count = (_recvCount[fileId] = (_recvCount[fileId] ?? 0) + 1);
      if (count >= rf.plan.totalChunks) {
        final w = _writeRafs.remove(fileId);
        try {
          await w?.flush();
          await w?.close();
        } catch (_) {}
      }
      return true;
    });
    if (!ok) return false;
    // Record timing in plain fields and notify the UI on a throttled tick
    // instead of copying the whole files map per chunk.
    final nowMs = DateTime.now().millisecondsSinceEpoch;
    _firstChunkReceivedAt ??= nowMs;
    _lastChunkReceivedAt = nowMs;
    _scheduleUiFlush();
    return true;
  }

  /// Lazily opens (and preallocates) the write handle for [rf], reusing it for
  /// the life of the incomplete file. FileMode.write is O_RDWR, so the same
  /// handle also serves relay reads while the file is being written.
  Future<RandomAccessFile> _openWriteRaf(SwarmReceivingFile rf) async {
    final existing = _writeRafs[rf.file.id];
    if (existing != null) return existing;
    final path = rf.path;
    if (path == null) {
      throw StateError('No destination path for ${rf.file.fileName}');
    }
    final raf = await File(path).open(mode: FileMode.write);
    if (rf.file.size > 0) {
      await raf.truncate(rf.file.size);
    }
    _writeRafs[rf.file.id] = raf;
    return raf;
  }

  /// Returns the bytes of a chunk we already hold (peer→peer GET).
  Future<Uint8List?> readChunk({required String fileId, required int chunkIndex}) async {
    final s = state;
    if (s == null) return null;
    final rf = s.files[fileId];
    if (rf == null) return null;
    if (!rf.bitmap.has(chunkIndex)) return null;
    final path = rf.path;
    if (path == null) return null;
    final offset = rf.plan.chunkOffset(chunkIndex);
    final n = rf.plan.chunkLength(chunkIndex);
    return _withFileLock(fileId, () async {
      final w = _writeRafs[fileId];
      if (w != null) {
        // File still being written; read from the same O_RDWR handle.
        await w.setPosition(offset);
        final out = await w.read(n);
        return out.length == n ? out : null;
      }
      // File complete and closed; read through the bounded read pool.
      final out = await _relayReadPool.read(path, offset, n);
      return out.length == n ? out : null;
    });
  }

  /// Spawns the peer-pull worker; runs until all chunks of all files held locally.
  Future<void> startPullWorker() async {
    final s = state;
    if (s == null) return;
    if (_pullStop != null && !_pullStop!.isCompleted) return;
    final security = ref.read(securityProvider);
    _client = SwarmHttpClient.create(security);
    _pullStop = Completer<void>();
    // Kick off broadcast of our (likely empty) bitmap so the swarm knows we're here.
    await _broadcastAllBitmaps();
    // Loop
    while (!_pullStop!.isCompleted) {
      try {
        final done = await _pullTick();
        if (done) {
          await _finishAndVerify();
          if (!_pullStop!.isCompleted) _pullStop!.complete();
          return;
        }
      } catch (e, st) {
        _logger.warning('Pull tick error', e, st);
      }
      await Future.any([
        Future.delayed(const Duration(milliseconds: 800)),
        _pullStop!.future,
      ]);
    }
  }

  /// One pass: for each missing chunk, find a peer that has it and pull.
  /// Returns true if everything we wanted is now held locally.
  Future<bool> _pullTick() async {
    final s = state;
    if (s == null) return true;
    var allComplete = true;

    for (final rf in s.files.values) {
      final plan = rf.plan;
      for (var k = 0; k < plan.totalChunks; k++) {
        if (rf.bitmap.has(k)) continue;
        allComplete = false;

        // Topologically aware parent-first routing
        PeerInfo? source;
        final myPlan = s.relayPlan;

        // 1st priority: try to pull from assigned parent node to respect overlay tree structure
        if (myPlan != null && myPlan.parentFingerprint != null) {
          final parentPeer = s.peers.firstWhereOrNull((p) => p.fingerprint == myPlan.parentFingerprint);
          if (parentPeer != null) {
            final parentBitmap = s.peerBitmaps[parentPeer.fingerprint]?[rf.file.id];
            if (parentBitmap != null && parentBitmap.has(k)) {
              source = parentPeer;
            }
          }
        }

        // 2nd priority: if parent doesn't have it, fall back to greedy round-robin scanning
        if (source == null) {
          for (final peer in s.peers) {
            if (peer.fingerprint == _myFingerprint()) continue;
            if (myPlan != null && peer.fingerprint == myPlan.parentFingerprint) continue; // already checked parent

            final bm = s.peerBitmaps[peer.fingerprint]?[rf.file.id];
            if (bm != null && bm.has(k)) {
              source = peer;
              break;
            }
          }
        }

        // Find a peer (not me) whose latest bitmap has this chunk.
        // PeerInfo? source;
        // for (final peer in s.peers) {
        //   if (peer.fingerprint == _myFingerprint()) continue;
        //   final bm = s.peerBitmaps[peer.fingerprint]?[rf.file.id];
        //   if (bm != null && bm.has(k)) {
        //     source = peer;
        //     break;
        //   }
        // }
        if (source == null) continue; // wait for sender or another peer to fill it
        try {
          final url = '${source.https ? 'https' : 'http'}://${source.ip}:${source.port}${ApiRoute.downloadChunk.v3}';
          final bytes = await _client!.getBytes(
            url: url,
            query: {
              'sessionId': s.sessionId,
              'fileId': rf.file.id,
              'chunkIndex': '$k',
              'token': rf.token,
            },
          );
          final ok = await writeChunk(
            fileId: rf.file.id,
            chunkIndex: k,
            bytes: bytes,
            source: source.fingerprint,
          );
          if (ok) {
            markBitmapDirty(rf.file.id);
          }
        } catch (e) {
          // _logger.fine('peer pull failed ${rf.file.id}#$k from ${source.alias}: $e');
          _logger.fine('peer pull failed ${rf.file.id}#$k from ${source.fingerprint}: $e');
          // mark the peer bitmap as "doesn't have it" so we don't loop hot
          final s2 = state;
          if (s2 != null) {
            final byPeer = {...s2.peerBitmaps};
            byPeer.remove(source.fingerprint);
            state = s2.copyWith(peerBitmaps: byPeer);
          }
        }
      }
    }
    return allComplete;
  }

  String? _myFingerprint() {
    try {
      return ref.read(securityProvider).certificateHash;
    } catch (_) {
      return null;
    }
  }

  /// Marks every file dirty and flushes immediately so the swarm learns we are
  /// here (even with empty bitmaps) at session start.
  Future<void> _broadcastAllBitmaps() async {
    final s = state;
    if (s == null) return;
    for (final rf in s.files.values) {
      _dirtyBitmaps.add(rf.file.id);
    }
    await _flushBitmapBroadcasts();
  }

  /// Phase 2: marks [fileId]'s bitmap dirty and schedules a coalesced flush.
  /// Replaces the old per-chunk broadcastBitmap() so N files collapse into one
  /// multi-bitmap announce per target.
  void markBitmapDirty(String fileId) {
    _dirtyBitmaps.add(fileId);
    if (_broadcastTimer != null) return;
    _broadcastTimer = Timer(_broadcastInterval, () {
      _broadcastTimer = null;
      // ignore: discarded_futures
      _flushBitmapBroadcasts();
    });
  }

  /// Sends the current bitmaps of all dirty files as a single AnnounceDto to
  /// the sender + every other peer, with the per-target POSTs issued in
  /// parallel.
  Future<void> _flushBitmapBroadcasts() async {
    final s = state;
    if (s == null) return;
    final client = _client;
    if (client == null) return;
    final me = _myFingerprint();
    if (me == null) return;
    if (_dirtyBitmaps.isEmpty) return;
    final dirty = _dirtyBitmaps.toList();
    _dirtyBitmaps.clear();
    final bitmaps = <BitmapDto>[
      for (final fid in dirty)
        if (s.files[fid] != null) s.files[fid]!.bitmap,
    ];
    if (bitmaps.isEmpty) return;
    final body = AnnounceDto(fingerprint: me, bitmaps: bitmaps).toJson();
    // POST to sender + every other peer, concurrently.
    final targets = <_AnnounceTarget>[
      _AnnounceTarget(ip: s.sender.ip ?? '', port: s.sender.port, https: s.sender.https),
      for (final peer in s.peers)
        if (peer.fingerprint != me) _AnnounceTarget(ip: peer.ip, port: peer.port, https: peer.https),
    ];
    await Future.wait([
      for (final t in targets)
        if (t.ip.isNotEmpty)
          client
              .postJson(
                url: '${t.https ? 'https' : 'http'}://${t.ip}:${t.port}${ApiRoute.announce.v3}',
                query: {'sessionId': s.sessionId},
                body: body,
              )
              .catchError((Object e) {
                _logger.fine('announce to ${t.ip} failed: $e');
                return '';
              }),
    ]);
  }

  Future<void> _finishAndVerify() async {
    final s = state;
    if (s == null) return;
    final files = {...s.files};
    // Phase 4: no redundant full-file re-read. Every chunk was SHA256-verified
    // against plan.sha256PerChunk at write time, and we only reach here once the
    // bitmap is fully set (all chunks present), so the chunk set provably covers
    // the whole file and the file is already intact.
    // Phase 3: flush/close any write handles still open (most were closed on
    // completion) and drop pooled relay-read descriptors.
    for (final fileId in _writeRafs.keys.toList()) {
      final w = _writeRafs.remove(fileId);
      try {
        await w?.flush();
        await w?.close();
      } catch (e) {
        _logger.warning('flush/close failed for $fileId: $e');
      }
    }
    await _relayReadPool.closeAll();
    final anyError = files.values.any((f) => f.errorMessage != null);
    final endMs = DateTime.now().millisecondsSinceEpoch;
    state = s.copyWith(
      status: anyError ? SessionStatus.finishedWithErrors : SessionStatus.finished,
      endTime: endMs,
      files: files,
      firstChunkReceivedAt: _firstChunkReceivedAt,
      lastChunkReceivedAt: _lastChunkReceivedAt,
    );
    _emitReceiverBenchmarkLog(state!);
    _logger.info('Swarm receive session ${s.sessionId} ${anyError ? 'finishedWithErrors' : 'finished'}');
  }

  /// Schema: RECVBENCH,sessionId,totalBytes,totalChunks,startToFirstMs,startToLastMs,sourceBreakdown(label=count;..)
  void _emitReceiverBenchmarkLog(SwarmReceiveState s) {
    final startMs = s.startTime;
    final totalBytes = s.totalBytes;
    final totalChunks = s.files.values.fold<int>(0, (a, f) => a + f.plan.totalChunks);
    final firstMs = (startMs != null && s.firstChunkReceivedAt != null) ? s.firstChunkReceivedAt! - startMs : -1;
    final lastMs = (startMs != null && s.lastChunkReceivedAt != null) ? s.lastChunkReceivedAt! - startMs : -1;
    final breakdown = <String, int>{};
    for (final rf in s.files.values) {
      rf.chunksFromSource.forEach((k, v) {
        final label = k == 'sender' ? 'sender' : (k.length < 8 ? k : k.substring(0, 8));
        breakdown[label] = (breakdown[label] ?? 0) + v;
      });
    }
    final breakdownStr = breakdown.entries.map((e) => '${e.key}=${e.value}').join(';');
    _logger.info(
      'RECVBENCH,${s.sessionId},$totalBytes,$totalChunks,$firstMs,$lastMs,$breakdownStr',
    );
  }

  /// Active push-to-children relay routing mechanism
  /// Automatically invoked by SwarmConstroller upon successful file write completion
  Future<void> pushToChildren({
    required String fileId, 
    required int chunkIndex, 
    required Uint8List bytes, 
  }) async {
    final s = state;
    if (s == null || s.relayPlan == null) return;
    final myPlan = s.relayPlan!;
    final client = _client;
    if (client == null || myPlan.childrenFingerprints.isEmpty) return;

    /// 1. Deduplication check (kept in a plain notifier field, not UI state):
    /// Only push if this chunk is newly acquired from sender or peer
    final currentPushedSet = _pushedToChildren[fileId] ??= <int>{};
    if (currentPushedSet.contains(chunkIndex)) return; // already pushed this chunk

    /// 2. Mark as pushed in-place before issuing requests.
    currentPushedSet.add(chunkIndex);

    final pushFutures = <Future<void>>[];

    /// 3. Parallel pipelined dispatches: 
    /// Route bytes to all registerd child nodes inside the overlay tree
    for (final childFp in myPlan.childrenFingerprints) {
      final childPeer = s.peers.firstWhereOrNull((p) => p.fingerprint == childFp);
      if (childPeer == null) continue; // should not happen

      // Find target's upload verification token associated with this specific file stream
      final targetFileRecord = s.files[fileId];
      if (targetFileRecord == null) continue; // should not happen

      final url = '${childPeer.https ? 'https' : 'http'}://${childPeer.ip}:${childPeer.port}${ApiRoute.uploadChunk.v3}';
      
      _logger.finest('Tree Routing: Pushing chunk $fileId#$chunkIndex downstream to child node [${childPeer.fingerprint}] at $url');
      
      pushFutures.add(client.postBytes(
        url: url,
        query: {
          'sessionId': s.sessionId,
          'fileId': fileId,
          'chunkIndex': '$chunkIndex',
          'token': targetFileRecord.token, // Verification token authorization pass
        },
        bytes: bytes,
      ).catchError((e) {
        _logger.fine('push to child ${childPeer.fingerprint} failed: $e');
      }));
    }
    /// Await all parallel push attempts to finish before allowing next batch
    await Future.wait(pushFutures);
  }

  /// Close current session (user or sender cancel).
  Future<void> closeSession() async {
    _pullStop?.complete();
    _uiFlushTimer?.cancel();
    _uiFlushTimer = null;
    _broadcastTimer?.cancel();
    _broadcastTimer = null;
    _dirtyBitmaps.clear();
    _pushedToChildren.clear();
    for (final fileId in _writeRafs.keys.toList()) {
      final w = _writeRafs.remove(fileId);
      try {
        await w?.close();
      } catch (_) {}
    }
    await _relayReadPool.closeAll();
    _recvCount.clear();
    _fileLocks.clear();
    _client?.dispose();
    _client = null;
    state = null;
    _chunkSource.clear();
  }
}

class _AnnounceTarget {
  final String ip;
  final int port;
  final bool https;
  const _AnnounceTarget({required this.ip, required this.port, required this.https});
}

/// Helper exported for the v3 controller — resolves a unique destination path
/// and reserves the name (so concurrent files don't collide) WITHOUT holding an
/// open descriptor. The actual write handle is opened lazily on the first chunk
/// (see [SwarmReceiveNotifier._openWriteRaf]), keeping the open-fd count bounded
/// for sessions with thousands of small files.
Future<String> reserveDestinationPath({
  required String destinationDirectory,
  required String fileName,
  required Set<String> createdDirectories,
}) async {
  final (path, _, _) = await digestFilePathAndPrepareDirectory(
    parentDirectory: destinationDirectory,
    fileName: fileName,
    createdDirectories: createdDirectories,
  );
  // Touch the file to reserve the name; the lazy write handle truncates and
  // preallocates it on first use.
  try {
    await File(path).create(recursive: true);
  } catch (_) {}
  return path;
}

/// FileStatus extension used by the UI: collapse swarm bitmap to FileStatus
/// for compatibility with progress widgets that expect the v2 enum.
extension SwarmFileStatusExt on SwarmReceivingFile {
  FileStatus get derivedStatus {
    if (errorMessage != null) return FileStatus.failed;
    if (bitmap.receivedCount >= plan.totalChunks) return FileStatus.finished;
    if (bitmap.receivedCount > 0) return FileStatus.sending;
    return FileStatus.queue;
  }
}

/// Helper for converting raw HTTP body strings on the server side.
Map<String, dynamic> jsonDecodeMap(String body) => jsonDecode(body) as Map<String, dynamic>;
