import 'package:flutter/material.dart';
import 'package:localsend_app/provider/network/nearby_devices_provider.dart';
import 'package:localsend_app/provider/network/swarm/swarm_benchmark.dart';
import 'package:localsend_app/provider/selection/selected_sending_files_provider.dart';
import 'package:localsend_app/util/file_size_helper.dart';
import 'package:localsend_app/widget/custom_basic_appbar.dart';
import 'package:refena_flutter/refena_flutter.dart';

/// Debug page that runs the same payload through v2 (multi-send) then v3 (swarm)
/// and appends the wall-clock timings to a CSV under the app cache directory.
class BenchmarkPage extends StatefulWidget {
  const BenchmarkPage({super.key});

  @override
  State<BenchmarkPage> createState() => _BenchmarkPageState();
}

class _BenchmarkPageState extends State<BenchmarkPage> with Refena {
  BenchmarkResult? _result;
  bool _running = false;
  String? _runError;

  Future<void> _run() async {
    if (_running) return;
    final files = ref.read(selectedSendingFilesProvider);
    final targets = ref.read(nearbyDevicesProvider).allDevices.values.toList();
    if (files.isEmpty || targets.length < 2) {
      setState(() => _runError = 'Need >=1 file and >=2 devices');
      return;
    }
    setState(() {
      _running = true;
      _runError = null;
      _result = null;
    });
    try {
      final r = await runSwarmBenchmark(ref: ref, targets: targets, files: files);
      if (!mounted) return;
      setState(() => _result = r);
    } catch (e) {
      if (!mounted) return;
      setState(() => _runError = '$e');
    } finally {
      if (mounted) setState(() => _running = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    final files = ref.watch(selectedSendingFilesProvider);
    final targets = ref.watch(nearbyDevicesProvider).allDevices.values.toList();
    final totalBytes = files.fold<int>(0, (a, f) => a + f.size);
    final progress = ref.watch(swarmBenchmarkProgressProvider);

    return Scaffold(
      appBar: basicLocalSendAppbar('A/B benchmark (v2 vs v3)'),
      body: Padding(
        padding: const EdgeInsets.fromLTRB(16, 20, 16, 30),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('Payload', style: Theme.of(context).textTheme.titleMedium),
            const SizedBox(height: 6),
            Text('${files.length} file(s), ${totalBytes.asReadableFileSize}'),
            const SizedBox(height: 12),
            Text('Targets (${targets.length})',
                style: Theme.of(context).textTheme.titleMedium),
            const SizedBox(height: 6),
            ...targets.map((t) => Text('• ${t.alias} (${t.ip ?? "?"})')),
            const SizedBox(height: 20),
            Row(
              children: [
                ElevatedButton.icon(
                  onPressed: _running ? null : _run,
                  icon: _running
                      ? const SizedBox(
                          width: 16, height: 16,
                          child: CircularProgressIndicator(strokeWidth: 2))
                      : const Icon(Icons.play_arrow),
                  label: Text(_running ? 'Running…' : 'Start'),
                ),
              ],
            ),
            const SizedBox(height: 16),
            if (_running)
              Card(
                child: Padding(
                  padding: const EdgeInsets.all(12),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text('Phase: ${progress.phase}'),
                      const SizedBox(height: 4),
                      Text(progress.message, style: const TextStyle(color: Colors.grey)),
                    ],
                  ),
                ),
              ),
            if (_runError != null)
              Padding(
                padding: const EdgeInsets.only(top: 12),
                child: Text(_runError!,
                    style: TextStyle(color: Theme.of(context).colorScheme.error)),
              ),
            if (_result != null) ...[
              const SizedBox(height: 12),
              _ResultCard(result: _result!),
            ],
          ],
        ),
      ),
    );
  }
}

class _ResultCard extends StatelessWidget {
  final BenchmarkResult result;
  const _ResultCard({required this.result});

  String _fmtMs(int? ms) => ms == null ? '—' : '${(ms / 1000).toStringAsFixed(2)} s';

  @override
  Widget build(BuildContext context) {
    final speedup = (result.v2Ms != null && result.v3Ms != null && result.v3Ms! > 0)
        ? (result.v2Ms! / result.v3Ms!).toStringAsFixed(2)
        : '—';
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('Result', style: Theme.of(context).textTheme.titleMedium),
            const SizedBox(height: 8),
            Text('Targets: ${result.targetCount}'),
            Text('Payload: ${result.totalBytes.asReadableFileSize}'),
            const SizedBox(height: 8),
            Text('v2 multi-send: ${_fmtMs(result.v2Ms)}',
                style: const TextStyle(fontWeight: FontWeight.bold)),
            if (result.v2Error != null && result.v2Error!.isNotEmpty)
              Text('  errors: ${result.v2Error}',
                  style: const TextStyle(color: Colors.orange)),
            Text('v3 swarm:      ${_fmtMs(result.v3Ms)}',
                style: const TextStyle(fontWeight: FontWeight.bold)),
            if (result.v3Error != null && result.v3Error!.isNotEmpty)
              Text('  errors: ${result.v3Error}',
                  style: const TextStyle(color: Colors.orange)),
            const SizedBox(height: 6),
            Text('v2/v3 ratio: $speedup'),
            const SizedBox(height: 12),
            const Text('CSV appended:', style: TextStyle(fontWeight: FontWeight.bold)),
            SelectableText(result.csvPath, style: const TextStyle(fontFamily: 'monospace')),
          ],
        ),
      ),
    );
  }
}
