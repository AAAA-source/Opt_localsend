import 'dart:async';
import 'dart:io';

import 'package:common/model/device.dart';
import 'package:common/model/session_status.dart';
import 'package:localsend_app/model/cross_file.dart';
import 'package:localsend_app/provider/network/send_provider.dart';
import 'package:localsend_app/provider/network/swarm/swarm_send_provider.dart';
import 'package:localsend_app/util/native/directories.dart';
import 'package:logging/logging.dart';
import 'package:refena_flutter/refena_flutter.dart';

final _logger = Logger('SwarmBenchmark');

/// Two consecutive runs of the same payload — once via v2 multi-send,
/// once via v3 swarm — with per-run wall-clock numbers collected so the
/// proposal's "close the gap" claim can be backed by real measurements.
class BenchmarkResult {
  final int totalBytes;
  final int targetCount;
  final int? v2Ms;
  final int? v3Ms;
  final String? v2Error;
  final String? v3Error;
  final String csvPath;

  const BenchmarkResult({
    required this.totalBytes,
    required this.targetCount,
    required this.v2Ms,
    required this.v3Ms,
    required this.v2Error,
    required this.v3Error,
    required this.csvPath,
  });
}

class BenchmarkProgress {
  final String phase; // 'idle' | 'v2' | 'v3' | 'done' | 'error'
  final String message;
  const BenchmarkProgress(this.phase, this.message);

  static const idle = BenchmarkProgress('idle', '');
}

final swarmBenchmarkProgressProvider = StateProvider<BenchmarkProgress>(
  (ref) => BenchmarkProgress.idle,
);

/// Run v2 (multi-send fan-out, one session per target) followed by v3 (swarm),
/// and append a single CSV row summarising the comparison.
///
/// Writes to `<cacheDir>/hopswift_bench.csv` (created with a header on first run).
Future<BenchmarkResult> runSwarmBenchmark({
  required Ref ref,
  required List<Device> targets,
  required List<CrossFile> files,
}) async {
  if (targets.isEmpty || files.isEmpty) {
    throw StateError('Benchmark needs at least 1 target and 1 file');
  }
  final totalBytes = files.fold<int>(0, (a, f) => a + f.size);
  final targetCount = targets.length;

  // -------- v2 leg --------
  ref.notifier(swarmBenchmarkProgressProvider).setState(
        (_) => BenchmarkProgress('v2', 'Running v2 multi-send to $targetCount targets'),
      );
  int? v2Ms;
  String? v2Error;
  try {
    final v2Start = DateTime.now().millisecondsSinceEpoch;
    await Future.wait([
      for (final t in targets)
        ref.notifier(sendProvider).startSession(
              target: t,
              files: files,
              background: true,
            ),
    ]);
    // After Future.wait, each session is either finished or finishedWithErrors.
    // We treat finishedWithErrors as a still-timed run; the error column will
    // capture per-target trouble.
    v2Ms = DateTime.now().millisecondsSinceEpoch - v2Start;
    final sessions = ref.read(sendProvider).values.toList();
    final problems = sessions
        .where((s) =>
            s.status == SessionStatus.finishedWithErrors ||
            s.status == SessionStatus.declined ||
            s.status == SessionStatus.canceledBySender ||
            s.status == SessionStatus.canceledByReceiver)
        .map((s) => '${s.target.alias}:${s.status.name}')
        .join('|');
    if (problems.isNotEmpty) v2Error = problems;
    _logger.info('v2 leg done in ${v2Ms}ms (problems: ${v2Error ?? "none"})');
  } catch (e, st) {
    _logger.severe('v2 leg failed', e, st);
    v2Error = e.toString();
  }

  // -------- v3 leg --------
  ref.notifier(swarmBenchmarkProgressProvider).setState(
        (_) => BenchmarkProgress('v3', 'Running v3 swarm to $targetCount targets'),
      );
  int? v3Ms;
  String? v3Error;
  try {
    final v3Start = DateTime.now().millisecondsSinceEpoch;
    final sid = await ref.notifier(swarmSendProvider).startSwarmSession(
          targets: targets,
          files: files,
        );
    v3Ms = DateTime.now().millisecondsSinceEpoch - v3Start;
    if (sid != null) {
      final ss = ref.read(swarmSendProvider)[sid];
      if (ss != null && ss.status != SessionStatus.finished) {
        v3Error = 'status=${ss.status.name}';
      }
    } else {
      v3Error = 'no session id (probably no eligible files)';
    }
    _logger.info('v3 leg done in ${v3Ms}ms (problems: ${v3Error ?? "none"})');
  } catch (e, st) {
    _logger.severe('v3 leg failed', e, st);
    v3Error = e.toString();
  }

  // -------- CSV --------
  final cacheDir = await getCacheDirectory();
  final csvPath = '$cacheDir${Platform.pathSeparator}hopswift_bench.csv';
  final csvFile = File(csvPath);
  final isNew = !await csvFile.exists();
  final row = _csvRow([
    DateTime.now().toIso8601String(),
    targetCount.toString(),
    files.length.toString(),
    totalBytes.toString(),
    (v2Ms ?? -1).toString(),
    (v3Ms ?? -1).toString(),
    (v2Ms != null && v3Ms != null && v3Ms > 0)
        ? (v2Ms / v3Ms).toStringAsFixed(3)
        : '',
    v2Error ?? '',
    v3Error ?? '',
  ]);
  try {
    final sink = csvFile.openWrite(mode: FileMode.append);
    try {
      if (isNew) {
        sink.writeln(_csvRow([
          'timestamp',
          'targets',
          'files',
          'totalBytes',
          'v2Ms',
          'v3Ms',
          'speedup_v2_over_v3',
          'v2Error',
          'v3Error',
        ]));
      }
      sink.writeln(row);
    } finally {
      await sink.flush();
      await sink.close();
    }
  } catch (e, st) {
    _logger.warning('Could not write CSV at $csvPath', e, st);
  }

  ref.notifier(swarmBenchmarkProgressProvider).setState(
        (_) => BenchmarkProgress('done', 'Done; CSV at $csvPath'),
      );
  return BenchmarkResult(
    totalBytes: totalBytes,
    targetCount: targetCount,
    v2Ms: v2Ms,
    v3Ms: v3Ms,
    v2Error: v2Error,
    v3Error: v3Error,
    csvPath: csvPath,
  );
}

String _csvRow(List<String> cols) {
  return cols.map(_escape).join(',');
}

String _escape(String s) {
  if (s.contains(',') || s.contains('"') || s.contains('\n')) {
    return '"${s.replaceAll('"', '""')}"';
  }
  return s;
}
