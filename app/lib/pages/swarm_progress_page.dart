import 'package:common/model/session_status.dart';
import 'package:flutter/material.dart';
import 'package:localsend_app/model/state/swarm/swarm_receive_state.dart';
import 'package:localsend_app/model/state/swarm/swarm_send_state.dart';
import 'package:localsend_app/provider/network/swarm/swarm_receive_provider.dart';
import 'package:localsend_app/provider/network/swarm/swarm_send_provider.dart';
import 'package:localsend_app/util/file_size_helper.dart';
import 'package:localsend_app/widget/custom_basic_appbar.dart';
import 'package:localsend_app/widget/custom_progress_bar.dart';
import 'package:refena_flutter/refena_flutter.dart';

/// Lightweight progress page for v3 (HopSwift swarm) sessions.
///
/// Renders both sender and receiver states. We intentionally keep this
/// separate from the v2 [ProgressPage] because the underlying state shape
/// (bitmap, per-source chunk counts, per-peer completion times) is too
/// different to fit through the v2 file-status enum without lossy mapping.
class SwarmProgressPage extends StatelessWidget {
  /// Sender session id (only meaningful when this page is for the sender).
  /// If null, the page renders the active receiver session instead.
  final String? senderSessionId;
  final bool showAppBar;

  const SwarmProgressPage({
    super.key,
    required this.senderSessionId,
    this.showAppBar = true,
  });

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: showAppBar ? basicLocalSendAppbar(senderSessionId != null ? 'Swarm sending' : 'Swarm receiving') : null,
      body: senderSessionId != null ? _SwarmSenderView(sessionId: senderSessionId!) : const _SwarmReceiverView(),
    );
  }
}

class _SwarmSenderView extends StatelessWidget {
  final String sessionId;
  const _SwarmSenderView({required this.sessionId});

  @override
  Widget build(BuildContext context) {
    return Consumer(
      builder: (context, ref) {
        final s = ref.watch(swarmSendProvider)[sessionId];
        if (s == null) {
          return const Center(child: Text('Session closed'));
        }
        return _SenderBody(state: s);
      },
    );
  }
}

class _SenderBody extends StatelessWidget {
  final SwarmSendState state;
  const _SenderBody({required this.state});

  @override
  Widget build(BuildContext context) {
    final totalBytes = state.totalBytes;
    final totalChunks = state.totalChunks;
    final sentChunks = state.sentChunks;
    final preparing = state.status == SessionStatus.waiting;
    final overall = preparing ? state.prepareProgress : (totalChunks == 0 ? 0.0 : sentChunks / totalChunks);
    return ListView(
      padding: const EdgeInsets.fromLTRB(16, 20, 16, 100),
      children: [
        _StatusHeader(status: state.status),
        const SizedBox(height: 12),
        _Section(
          title: preparing ? 'Preparing (hashing)' : 'Direct uploads (sender → peers)',
          progress: overall.clamp(0.0, 1.0),
          subtitle: preparing ? '${(overall * 100).toStringAsFixed(1)}%' : '$sentChunks / $totalChunks chunks · ${totalBytes.asReadableFileSize}',
        ),
        const SizedBox(height: 16),
        Text('Targets (${state.targets.length})', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 6),
        ...state.targets.map((t) {
          final completeMs = state.peerCompleteTime[t.fingerprint];
          final tookMs = completeMs == null ? null : (completeMs - state.startTime);
          return ListTile(
            dense: true,
            leading: const Icon(Icons.devices),
            title: Text(t.alias),
            subtitle: Text(t.ip ?? '(no ip)'),
            trailing: tookMs == null
                ? const Text('…', style: TextStyle(color: Colors.grey))
                : Text('${(tookMs / 1000).toStringAsFixed(1)} s', style: const TextStyle(fontFeatures: [FontFeature.tabularFigures()])),
          );
        }),
        const SizedBox(height: 16),
        if (state.endTime != null) _SenderTimingFooter(state: state),
      ],
    );
  }
}

class _SenderTimingFooter extends StatelessWidget {
  final SwarmSendState state;
  const _SenderTimingFooter({required this.state});

  @override
  Widget build(BuildContext context) {
    final prepareMs = (state.prepareStartTime != null && state.prepareEndTime != null) ? state.prepareEndTime! - state.prepareStartTime! : null;
    final sendMs = (state.firstChunkSentAt != null && state.lastChunkSentAt != null) ? state.lastChunkSentAt! - state.firstChunkSentAt! : null;
    final lastReceiverMs = state.peerCompleteTime.values.isEmpty
        ? null
        : state.peerCompleteTime.values.reduce((a, b) => a > b ? a : b) - state.startTime;
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text('Timing', style: TextStyle(fontWeight: FontWeight.bold)),
            const SizedBox(height: 4),
            Text('Prepare (hash):       ${_fmt(prepareMs)}'),
            Text('Send window:          ${_fmt(sendMs)}'),
            Text('Last-receiver done:   ${_fmt(lastReceiverMs)}'),
          ],
        ),
      ),
    );
  }
}

class _SwarmReceiverView extends StatelessWidget {
  const _SwarmReceiverView();

  @override
  Widget build(BuildContext context) {
    return Consumer(
      builder: (context, ref) {
        final s = ref.watch(swarmReceiveProvider);
        if (s == null) {
          return const Center(child: Text('Session closed'));
        }
        return _ReceiverBody(state: s);
      },
    );
  }
}

class _ReceiverBody extends StatelessWidget {
  final SwarmReceiveState state;
  const _ReceiverBody({required this.state});

  @override
  Widget build(BuildContext context) {
    final totalBytes = state.totalBytes;
    final receivedBytes = state.receivedBytes;
    final pct = totalBytes == 0 ? 0.0 : receivedBytes / totalBytes;
    final globalBreakdown = <String, int>{};
    for (final rf in state.files.values) {
      rf.chunksFromSource.forEach((k, v) {
        globalBreakdown[k] = (globalBreakdown[k] ?? 0) + v;
      });
    }
    return ListView(
      padding: const EdgeInsets.fromLTRB(16, 20, 16, 100),
      children: [
        _StatusHeader(status: state.status),
        const SizedBox(height: 12),
        _Section(
          title: 'Receiving from ${state.senderAlias}',
          progress: pct.clamp(0.0, 1.0),
          subtitle: '${receivedBytes.asReadableFileSize} / ${totalBytes.asReadableFileSize}',
        ),
        const SizedBox(height: 12),
        Text(
          'Files (${state.files.values.fold<int>(0, (a, u) => a + u.members.length)})',
          style: Theme.of(context).textTheme.titleMedium,
        ),
        const SizedBox(height: 6),
        // Expand each unit back into its original member files (bundles → files)
        // and derive per-file progress from the unit bitmap.
        ...state.files.values.expand((unit) => unit.members.map((m) {
          final received = state.receivedBytesOfMember(unit, m);
          final filePct = m.size == 0 ? 1.0 : received / m.size;
          return Padding(
            padding: const EdgeInsets.symmetric(vertical: 6),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(m.fileName, maxLines: 1, overflow: TextOverflow.ellipsis),
                const SizedBox(height: 4),
                CustomProgressBar(progress: filePct.clamp(0.0, 1.0), borderRadius: 4),
                const SizedBox(height: 2),
                Text(
                  '${received.asReadableFileSize} / ${m.size.asReadableFileSize}',
                  style: const TextStyle(color: Colors.grey, fontSize: 12),
                ),
                if (unit.errorMessage != null) Text(unit.errorMessage!, style: const TextStyle(color: Colors.red, fontSize: 12)),
              ],
            ),
          );
        })),
        const SizedBox(height: 16),
        Text('Chunks by source', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 6),
        if (globalBreakdown.isEmpty)
          const Text('No chunks received yet', style: TextStyle(color: Colors.grey))
        else
          ...globalBreakdown.entries.map((e) {
            final label = e.key == 'sender' ? 'sender' : 'peer ${e.key.substring(0, e.key.length < 8 ? e.key.length : 8)}';
            return ListTile(
              dense: true,
              leading: Icon(e.key == 'sender' ? Icons.cloud_upload : Icons.hub),
              title: Text(label),
              trailing: Text('${e.value} chunks'),
            );
          }),
        const SizedBox(height: 12),
        if (state.endTime != null) _ReceiverTimingFooter(state: state),
      ],
    );
  }
}

class _ReceiverTimingFooter extends StatelessWidget {
  final SwarmReceiveState state;
  const _ReceiverTimingFooter({required this.state});

  @override
  Widget build(BuildContext context) {
    final start = state.startTime;
    final first = state.firstChunkReceivedAt;
    final last = state.lastChunkReceivedAt;
    final firstMs = (start != null && first != null) ? first - start : null;
    final lastMs = (start != null && last != null) ? last - start : null;
    final wallMs = (start != null && state.endTime != null) ? state.endTime! - start : null;
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text('Timing', style: TextStyle(fontWeight: FontWeight.bold)),
            const SizedBox(height: 4),
            Text('First chunk:  ${_fmt(firstMs)}'),
            Text('Last chunk:   ${_fmt(lastMs)}'),
            Text('Wall clock:   ${_fmt(wallMs)}'),
          ],
        ),
      ),
    );
  }
}

class _StatusHeader extends StatelessWidget {
  final SessionStatus status;
  const _StatusHeader({required this.status});

  @override
  Widget build(BuildContext context) {
    final label = switch (status) {
      SessionStatus.waiting => 'Waiting / preparing',
      SessionStatus.sending => 'Transferring',
      SessionStatus.finished => 'Finished',
      SessionStatus.finishedWithErrors => 'Finished with errors',
      SessionStatus.declined => 'Declined',
      SessionStatus.recipientBusy => 'Recipient busy',
      SessionStatus.tooManyAttempts => 'Too many attempts',
      SessionStatus.canceledBySender => 'Canceled by sender',
      SessionStatus.canceledByReceiver => 'Canceled by receiver',
    };
    return Text(label, style: Theme.of(context).textTheme.titleLarge);
  }
}

class _Section extends StatelessWidget {
  final String title;
  final double progress;
  final String? subtitle;
  const _Section({required this.title, required this.progress, this.subtitle});

  @override
  Widget build(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(title, style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 6),
        CustomProgressBar(progress: progress, borderRadius: 5),
        if (subtitle != null) ...[
          const SizedBox(height: 4),
          Text(subtitle!, style: const TextStyle(color: Colors.grey)),
        ],
      ],
    );
  }
}

String _fmt(int? ms) {
  if (ms == null) return '—';
  if (ms < 1000) return '${ms}ms';
  return '${(ms / 1000).toStringAsFixed(2)}s';
}
