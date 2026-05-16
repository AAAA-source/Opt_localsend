import 'dart:io';
import 'dart:typed_data';

import 'package:common/model/dto/swarm/chunk_plan_dto.dart';
import 'package:crypto/crypto.dart';

/// Reads [filePath] from disk and produces a [ChunkPlanDto]:
/// per-chunk SHA-256 + full-file SHA-256.
///
/// Designed to run inside an isolate; emits progress via [onProgress]
/// (0..1 over the *bytes hashed*, so the caller can show a "Preparing…" bar).
Future<ChunkPlanDto> planChunks({
  required String fileId,
  required String filePath,
  required int chunkSize,
  void Function(double progress)? onProgress,
}) async {
  final file = File(filePath);
  final fileSize = await file.length();
  if (fileSize == 0) {
    // Empty file: still emit a single 0-byte chunk so the swarm has work to do
    final emptyHash = sha256.convert(const <int>[]).toString();
    onProgress?.call(1);
    return ChunkPlanDto(
      fileId: fileId,
      totalChunks: 1,
      chunkSize: chunkSize,
      lastChunkSize: 0,
      sha256PerChunk: [emptyHash],
      fileSha256: emptyHash,
    );
  }

  final totalChunks = (fileSize + chunkSize - 1) ~/ chunkSize;
  final lastChunkSize = fileSize - (totalChunks - 1) * chunkSize;

  final perChunk = <String>[];
  final fileDigest = AccumulatorSink<Digest>();
  final fileSink = sha256.startChunkedConversion(fileDigest);

  final raf = await file.open();
  try {
    var processed = 0;
    final buf = Uint8List(64 * 1024); // 64 KiB read window
    for (var i = 0; i < totalChunks; i++) {
      final remaining = i == totalChunks - 1 ? lastChunkSize : chunkSize;
      var leftInChunk = remaining;
      final chunkDigest = AccumulatorSink<Digest>();
      final chunkSink = sha256.startChunkedConversion(chunkDigest);
      while (leftInChunk > 0) {
        final toRead = leftInChunk < buf.length ? leftInChunk : buf.length;
        final n = await raf.readInto(buf, 0, toRead);
        if (n <= 0) {
          throw StateError('Unexpected EOF at chunk $i ($leftInChunk bytes missing)');
        }
        final view = Uint8List.sublistView(buf, 0, n);
        chunkSink.add(view);
        fileSink.add(view);
        leftInChunk -= n;
        processed += n;
        if (onProgress != null) {
          onProgress(processed / fileSize);
        }
      }
      chunkSink.close();
      perChunk.add(chunkDigest.events.single.toString());
    }
  } finally {
    await raf.close();
  }

  fileSink.close();
  final fileSha = fileDigest.events.single.toString();
  return ChunkPlanDto(
    fileId: fileId,
    totalChunks: totalChunks,
    chunkSize: chunkSize,
    lastChunkSize: lastChunkSize,
    sha256PerChunk: perChunk,
    fileSha256: fileSha,
  );
}
