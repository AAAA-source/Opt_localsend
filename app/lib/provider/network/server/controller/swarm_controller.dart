import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:collection/collection.dart';
import 'package:common/api_route_builder.dart';
import 'package:common/model/device.dart';
import 'package:common/model/dto/swarm/announce_dto.dart';
import 'package:common/model/dto/swarm/bitmap_dto.dart';
import 'package:common/model/dto/swarm/prepare_swarm_request_dto.dart';
import 'package:common/model/dto/swarm/prepare_swarm_response_dto.dart';
import 'package:common/model/session_status.dart';
import 'package:localsend_app/model/state/swarm/swarm_receive_state.dart';
import 'package:localsend_app/pages/progress_page.dart';
import 'package:localsend_app/pages/receive_page.dart';
import 'package:localsend_app/provider/favorites_provider.dart';
import 'package:localsend_app/provider/network/server/server_utils.dart';
import 'package:localsend_app/provider/network/swarm/swarm_receive_provider.dart';
import 'package:localsend_app/provider/network/swarm/swarm_send_provider.dart';
import 'package:localsend_app/provider/settings_provider.dart';
import 'package:localsend_app/util/native/directories.dart';
import 'package:localsend_app/util/simple_server.dart';
import 'package:logging/logging.dart';
import 'package:refena_flutter/refena_flutter.dart';
import 'package:routerino/routerino.dart';
import 'package:uuid/uuid.dart';

const _uuid = Uuid();
final _logger = Logger('SwarmController');

/// Installs all v3 (HopSwift swarm) routes on the simple HTTP server.
class SwarmController {
  final ServerUtils server;

  SwarmController(this.server);

  void installRoutes({
    required SimpleServerRouteBuilder router,
    required int port,
    required bool https,
  }) {
    router.post(ApiRoute.prepareSwarm.v3, (HttpRequest req) async {
      await _prepareSwarm(req, port: port, https: https);
    });
    router.post(ApiRoute.uploadChunk.v3, (HttpRequest req) async {
      await _uploadChunk(req);
    });
    router.get(ApiRoute.downloadChunk.v3, (HttpRequest req) async {
      await _downloadChunk(req);
    });
    router.get(ApiRoute.bitmap.v3, (HttpRequest req) async {
      await _bitmap(req);
    });
    router.post(ApiRoute.announce.v3, (HttpRequest req) async {
      await _announce(req);
    });
  }

  Future<void> _prepareSwarm(HttpRequest req, {required int port, required bool https}) async {
    final notifier = server.ref.notifier(swarmReceiveProvider);
    if (notifier.hasActiveSession) {
      await req.respondJson(409, message: 'Already in a swarm session');
      return;
    }
    // Also block if a v2 session is active.
    if (server.getStateOrNull()?.session != null) {
      await req.respondJson(409, message: 'Blocked by another session');
      return;
    }

    final PrepareSwarmRequestDto dto;
    try {
      final body = await req.readAsString();
      dto = PrepareSwarmRequestDto.fromJson(jsonDecode(body) as Map<String, dynamic>);
    } catch (e) {
      await req.respondJson(400, message: 'Malformed payload: $e');
      return;
    }
    if (dto.files.isEmpty) {
      await req.respondJson(400, message: 'No files');
      return;
    }

    final settings = server.ref.read(settingsProvider);
    final destinationDir = settings.destination ?? await getDefaultDestinationDirectory();
    final cacheDir = await getCacheDirectory();
    final senderDevice = dto.info.toDevice(req.ip, port, https, null);
    final senderAlias = server.ref
            .read(favoritesProvider)
            .firstWhereOrNull((e) => e.fingerprint == dto.info.fingerprint)
            ?.alias ??
        dto.info.alias;

    final streamController = StreamController<Map<String, String>?>();
    // Provisional receiving-file records (no RAF yet — opened after acceptance).
    final provisionalFiles = <String, SwarmReceivingFile>{
      for (final file in dto.files.values)
        file.id: SwarmReceivingFile(
          file: file,
          plan: dto.plans[file.id]!,
          token: _uuid.v4(),
          desiredName: file.fileName,
          path: null,
          bitmap: BitmapDto.empty(
            sessionId: dto.sessionId,
            fileId: file.id,
            totalChunks: dto.plans[file.id]!.totalChunks,
          ),
          chunksFromSource: <String, int>{},
          raf: null,
          errorMessage: null,
        ),
    };

    notifier.setSession(SwarmReceiveState(
      sessionId: dto.sessionId,
      status: SessionStatus.waiting,
      sender: senderDevice,
      senderAlias: senderAlias,
      peers: dto.peers,
      myIndex: dto.myIndex,
      files: provisionalFiles,
      startTime: null,
      endTime: null,
      destinationDirectory: destinationDir,
      cacheDirectory: cacheDir,
      createdDirectories: <String>{},
      responseHandler: streamController,
      peerBitmaps: const {},
      errorMessage: null,
    ));

    // Auto-accept paths (quick save / favorites) — match v2 behavior.
    bool quickSave = settings.quickSave;
    if (!quickSave && settings.quickSaveFromFavorites) {
      final isFav = server.ref.read(favoritesProvider).any((e) => e.fingerprint == dto.info.fingerprint);
      if (isFav) quickSave = true;
    }

    Map<String, String>? selection;
    if (quickSave) {
      selection = {for (final f in dto.files.values) f.id: f.fileName};
    } else {
      // Push the v2 ReceivePage UI; user pressing accept will call swarmReceiveProvider.acceptOrDecline
      // ignore: use_build_context_synchronously, unawaited_futures
      _pushReceivePage();
      selection = await streamController.stream.first;
    }
    if (notifier.state == null) {
      await req.respondJson(500, message: 'Invalid state');
      return;
    }
    if (selection == null) {
      await notifier.closeSession();
      await req.respondJson(403, message: 'Declined');
      return;
    }
    if (selection.isEmpty) {
      await notifier.closeSession();
      await req.respondJson(204);
      return;
    }

    // Open files for offset writes
    final updatedFiles = <String, SwarmReceivingFile>{};
    final tokens = <String, String>{};
    for (final entry in selection.entries) {
      final fileId = entry.key;
      final desiredName = entry.value;
      final existing = provisionalFiles[fileId];
      if (existing == null) continue;
      try {
        final (path, raf) = await openDestinationFile(
          destinationDirectory: destinationDir,
          fileName: desiredName,
          createdDirectories: notifier.state!.createdDirectories,
          finalSize: existing.file.size,
        );
        updatedFiles[fileId] = existing.copyWith(path: path, raf: raf);
        tokens[fileId] = existing.token;
      } catch (e, st) {
        _logger.severe('Failed to open destination for $desiredName', e, st);
      }
    }
    notifier.mutate((s) => s.copyWith(
          status: SessionStatus.sending,
          files: updatedFiles,
          startTime: DateTime.now().millisecondsSinceEpoch,
          clearResponseHandler: true,
        ));
    // Kick off the pull worker (peer chunk swap + completion watcher).
    // ignore: unawaited_futures, discarded_futures
    unawaited(notifier.startPullWorker());

    if (quickSave) {
      // ignore: use_build_context_synchronously, unawaited_futures, discarded_futures
      unawaited(Routerino.context.pushImmediately(
        () => ProgressPage(
          showAppBar: false,
          closeSessionOnClose: true,
          sessionId: dto.sessionId,
        ),
      ));
    }

    await req.respondJson(200,
        body: PrepareSwarmResponseDto(sessionId: dto.sessionId, tokens: tokens).toJson());
  }

  void _pushReceivePage() {
    // Lightweight UX wrapper — we reuse the existing ReceivePage built from a ViewProvider.
    // For now, the existing receive flow's ReceivePageVm is v2-specific. We expose
    // a minimal ViewProvider that maps SwarmReceiveState into the same VM shape.
    final viewProvider = ViewProvider((ref) {
      final s = ref.watch(swarmReceiveProvider);
      return ReceivePageVm(
        status: s?.status,
        sender: s?.sender ?? Device.empty,
        showSenderInfo: true,
        files: s?.files.values.map((f) => f.file).toList() ?? const [],
        message: null,
        onAccept: () async {
          final session = ref.read(swarmReceiveProvider);
          if (session == null) return;
          final selection = {for (final f in session.files.values) f.file.id: f.file.fileName};
          ref.notifier(swarmReceiveProvider).acceptOrDecline(selection);
          // ignore: use_build_context_synchronously, unawaited_futures
          await Routerino.context.pushAndRemoveUntilImmediately(
            removeUntil: ReceivePage,
            builder: () => ProgressPage(
              showAppBar: false,
              closeSessionOnClose: true,
              sessionId: session.sessionId,
            ),
          );
        },
        onDecline: () {
          ref.notifier(swarmReceiveProvider).acceptOrDecline(null);
        },
        onClose: () {
          // ignore: discarded_futures
          ref.notifier(swarmReceiveProvider).closeSession();
        },
      );
    });
    // ignore: use_build_context_synchronously, discarded_futures
    Routerino.context.push(() => ReceivePage(viewProvider));
  }

  Future<void> _uploadChunk(HttpRequest req) async {
    final q = req.uri.queryParameters;
    final sessionId = q['sessionId'];
    final fileId = q['fileId'];
    final chunkIndex = int.tryParse(q['chunkIndex'] ?? '');
    final token = q['token'];
    if (sessionId == null || fileId == null || chunkIndex == null || token == null) {
      await req.respondJson(400, message: 'Missing query parameters');
      return;
    }
    final notifier = server.ref.notifier(swarmReceiveProvider);
    final s = notifier.state;
    if (s == null || s.sessionId != sessionId) {
      await req.respondJson(409, message: 'No matching session');
      return;
    }
    final rf = s.files[fileId];
    if (rf == null || rf.token != token) {
      await req.respondJson(403, message: 'Invalid token');
      return;
    }
    // Determine source: sender IP vs a peer IP.
    final source = req.ip == s.sender.ip ? 'sender' : _findPeerFingerprint(req.ip, s) ?? 'unknown';
    // Read the whole body. Chunks are bounded by chunkSize (default 4 MiB) so memory is fine.
    final bytes = await _readAllBytes(req);
    final ok = await notifier.writeChunk(
      fileId: fileId,
      chunkIndex: chunkIndex,
      bytes: bytes,
      source: source,
    );
    if (!ok) {
      await req.respondJson(400, message: 'Chunk rejected (hash/write failure)');
      return;
    }
    // Asynchronously broadcast our new bitmap.
    // ignore: unawaited_futures, discarded_futures
    unawaited(notifier.broadcastBitmap(fileId));
    await req.respondJson(200);
  }

  Future<void> _downloadChunk(HttpRequest req) async {
    final q = req.uri.queryParameters;
    final sessionId = q['sessionId'];
    final fileId = q['fileId'];
    final chunkIndex = int.tryParse(q['chunkIndex'] ?? '');
    final token = q['token'];
    if (sessionId == null || fileId == null || chunkIndex == null || token == null) {
      await req.respondJson(400, message: 'Missing query parameters');
      return;
    }
    final notifier = server.ref.notifier(swarmReceiveProvider);
    final s = notifier.state;
    if (s == null || s.sessionId != sessionId) {
      await req.respondJson(409, message: 'No matching session');
      return;
    }
    final rf = s.files[fileId];
    if (rf == null || rf.token != token) {
      await req.respondJson(403, message: 'Invalid token');
      return;
    }
    final bytes = await notifier.readChunk(fileId: fileId, chunkIndex: chunkIndex);
    if (bytes == null) {
      await req.respondJson(404, message: 'Chunk not available');
      return;
    }
    req.response
      ..statusCode = 200
      ..headers.contentType = ContentType.binary
      ..headers.set(HttpHeaders.contentLengthHeader, '${bytes.length}')
      ..add(bytes);
    await req.response.close();
  }

  Future<void> _bitmap(HttpRequest req) async {
    final q = req.uri.queryParameters;
    final sessionId = q['sessionId'];
    final fileId = q['fileId'];
    if (sessionId == null || fileId == null) {
      await req.respondJson(400, message: 'Missing query parameters');
      return;
    }
    final s = server.ref.read(swarmReceiveProvider);
    if (s == null || s.sessionId != sessionId) {
      await req.respondJson(404, message: 'No matching session');
      return;
    }
    final rf = s.files[fileId];
    if (rf == null) {
      await req.respondJson(404, message: 'Unknown fileId');
      return;
    }
    await req.respondJson(200, body: rf.bitmap.toJson());
  }

  Future<void> _announce(HttpRequest req) async {
    final q = req.uri.queryParameters;
    final sessionId = q['sessionId'];
    if (sessionId == null) {
      await req.respondJson(400, message: 'Missing sessionId');
      return;
    }
    final AnnounceDto dto;
    try {
      final body = await req.readAsString();
      dto = AnnounceDto.fromJson(jsonDecode(body) as Map<String, dynamic>);
    } catch (e) {
      await req.respondJson(400, message: 'Malformed payload: $e');
      return;
    }
    // Sender path: forward to swarmSendProvider
    final sendState = server.ref.read(swarmSendProvider);
    if (sendState.containsKey(sessionId)) {
      server.ref.notifier(swarmSendProvider).onAnnounce(sessionId: sessionId, dto: dto);
    }
    // Receiver path: update local peerBitmaps if we are also receiving this session
    final recvState = server.ref.read(swarmReceiveProvider);
    if (recvState?.sessionId == sessionId) {
      server.ref.notifier(swarmReceiveProvider).onAnnounce(dto);
    }
    await req.respondJson(200);
  }

  String? _findPeerFingerprint(String ip, SwarmReceiveState s) {
    for (final p in s.peers) {
      if (p.ip == ip) return p.fingerprint;
    }
    return null;
  }

  Future<Uint8List> _readAllBytes(HttpRequest req) async {
    final builder = BytesBuilder(copy: false);
    await for (final chunk in req) {
      builder.add(chunk);
    }
    return builder.toBytes();
  }
}
