import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:common/api_route_builder.dart';
import 'package:common/model/dto/swarm/announce_dto.dart';
import 'package:common/model/dto/swarm/peer_info.dart';
import 'package:common/model/file_status.dart';
import 'package:common/model/session_status.dart';
import 'package:crypto/crypto.dart';
import 'package:localsend_app/model/state/swarm/swarm_receive_state.dart';
import 'package:localsend_app/provider/network/swarm/swarm_http_client.dart';
import 'package:localsend_app/provider/security_provider.dart';
import 'package:localsend_app/util/native/file_saver.dart';
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

  @override
  SwarmReceiveState? init() => null;

  bool get hasActiveSession {
    final s = state;
    if (s == null) return false;
    return s.status == SessionStatus.waiting || s.status == SessionStatus.sending;
  }

  /// Replaces the current swarm session. Caller must guarantee no active session.
  void setSession(SwarmReceiveState s) {
    state = s;
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
    perFile[announce.bitmap.fileId] = announce.bitmap;
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
    final raf = rf.raf;
    if (raf == null) {
      _logger.warning('No open RAF for ${rf.file.fileName}');
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
    // Notify listeners with a fresh state snapshot + timing.
    final nowMs = DateTime.now().millisecondsSinceEpoch;
    state = s.copyWith(
      files: {...s.files},
      firstChunkReceivedAt: s.firstChunkReceivedAt ?? nowMs,
      lastChunkReceivedAt: nowMs,
    );
    return true;
  }

  /// Returns the bytes of a chunk we already hold (peer→peer GET).
  Future<Uint8List?> readChunk({required String fileId, required int chunkIndex}) async {
    final s = state;
    if (s == null) return null;
    final rf = s.files[fileId];
    if (rf == null) return null;
    if (!rf.bitmap.has(chunkIndex)) return null;
    final raf = rf.raf;
    if (raf == null) return null;
    await raf.setPosition(rf.plan.chunkOffset(chunkIndex));
    final n = rf.plan.chunkLength(chunkIndex);
    final out = await raf.read(n);
    if (out.length != n) return null;
    return out;
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

        // Find a peer (not me) whose latest bitmap has this chunk.
        PeerInfo? source;
        for (final peer in s.peers) {
          if (peer.fingerprint == _myFingerprint()) continue;
          final bm = s.peerBitmaps[peer.fingerprint]?[rf.file.id];
          if (bm != null && bm.has(k)) {
            source = peer;
            break;
          }
        }
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
            await broadcastBitmap(rf.file.id);
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

  Future<void> _broadcastAllBitmaps() async {
    final s = state;
    if (s == null) return;
    for (final rf in s.files.values) {
      await broadcastBitmap(rf.file.id);
    }
  }

  Future<void> broadcastBitmap(String fileId) async {
    final s = state;
    if (s == null) return;
    final rf = s.files[fileId];
    if (rf == null) return;
    final me = _myFingerprint();
    if (me == null) return;
    final announce = AnnounceDto(fingerprint: me, bitmap: rf.bitmap);
    final body = announce.toJson();
    // POST to sender + every other peer.
    final targets = <_AnnounceTarget>[
      _AnnounceTarget(ip: s.sender.ip ?? '', port: s.sender.port, https: s.sender.https),
      ...[
        for (final peer in s.peers)
          if (peer.fingerprint != me) _AnnounceTarget(ip: peer.ip, port: peer.port, https: peer.https),
      ],
    ];
    final client = _client;
    if (client == null) return;
    for (final t in targets) {
      if (t.ip.isEmpty) continue;
      final url = '${t.https ? 'https' : 'http'}://${t.ip}:${t.port}${ApiRoute.announce.v3}';
      try {
        await client.postJson(
          url: url,
          query: {'sessionId': s.sessionId},
          body: body,
        );
      } catch (e) {
        _logger.fine('announce to ${t.ip} failed: $e');
      }
    }
  }

  Future<void> _finishAndVerify() async {
    final s = state;
    if (s == null) return;
    final files = {...s.files};
    for (final entry in files.entries) {
      final rf = entry.value;
      try {
        await rf.raf?.flush();
      } catch (e) {
        _logger.warning('flush failed for ${rf.file.fileName}: $e');
      }
      try {
        // Verify full-file hash
        if (rf.path != null) {
          final f = File(rf.path!);
          final actual = await sha256.bind(f.openRead()).first;
          if (actual.toString() != rf.plan.fileSha256) {
            files[entry.key] = rf.copyWith(errorMessage: 'SHA256 mismatch on full file');
          }
        }
      } catch (e, st) {
        _logger.warning('verify failed for ${rf.file.fileName}', e, st);
        files[entry.key] = rf.copyWith(errorMessage: 'verify error: $e');
      }
      try {
        await rf.raf?.close();
      } catch (_) {}
    }
    final anyError = files.values.any((f) => f.errorMessage != null);
    final endMs = DateTime.now().millisecondsSinceEpoch;
    state = s.copyWith(
      status: anyError ? SessionStatus.finishedWithErrors : SessionStatus.finished,
      endTime: endMs,
      files: files,
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

  /// Close current session (user or sender cancel).
  Future<void> closeSession() async {
    _pullStop?.complete();
    final s = state;
    if (s != null) {
      for (final rf in s.files.values) {
        try {
          await rf.raf?.close();
        } catch (_) {}
      }
    }
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

/// Helper exported for the v3 controller — opens/creates the destination file
/// and returns the absolute path + open [RandomAccessFile].
Future<(String path, RandomAccessFile raf)> openDestinationFile({
  required String destinationDirectory,
  required String fileName,
  required Set<String> createdDirectories,
  required int finalSize,
}) async {
  final (path, _, _) = await digestFilePathAndPrepareDirectory(
    parentDirectory: destinationDirectory,
    fileName: fileName,
    createdDirectories: createdDirectories,
  );
  final file = File(path);
  // Pre-allocate the full size so offset writes don't expand the file repeatedly.
  final raf = await file.open(mode: FileMode.write);
  if (finalSize > 0) {
    await raf.truncate(finalSize);
  }
  return (path, raf);
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
