import 'dart:async';
import 'dart:collection';
import 'dart:io';
import 'dart:typed_data';

/// Bounded LRU of **read-only** [RandomAccessFile] handles, keyed by absolute
/// path.
///
/// Two swarm hot paths reopened files far too often:
///  * the sender re-`open()`/`close()`d a file on *every* 4 MiB chunk, and
///  * a receiver relaying an already-completed file to peers reopened it per
///    GET.
///
/// This pool keeps a small set of descriptors open and reuses them, capping the
/// number of simultaneously-open fds (so a folder of thousands of files can't
/// exhaust the descriptor table). Each key's seek+read is serialized so
/// concurrent reads of the same file don't clobber each other's position.
class RafReadPool {
  final int capacity;
  // Insertion-ordered → first key is the least-recently-used.
  final LinkedHashMap<String, RandomAccessFile> _handles = LinkedHashMap();
  final Map<String, Future<void>> _locks = {};

  RafReadPool({this.capacity = 128});

  /// Reads [length] bytes at [offset] from [path], opening/reusing a pooled
  /// descriptor. Serialized per path.
  Future<Uint8List> read(String path, int offset, int length) {
    return _withLock(path, () async {
      final raf = await _acquire(path);
      await raf.setPosition(offset);
      return raf.read(length);
    });
  }

  Future<RandomAccessFile> _acquire(String path) async {
    final existing = _handles.remove(path);
    if (existing != null) {
      _handles[path] = existing; // move to most-recently-used
      return existing;
    }
    final raf = await File(path).open(mode: FileMode.read);
    _handles[path] = raf;
    await _evictIfNeeded(path);
    return raf;
  }

  Future<void> _evictIfNeeded(String keepKey) async {
    if (_handles.length <= capacity) return;
    // Evict the oldest handle that isn't currently locked (in use).
    for (final key in _handles.keys.toList()) {
      if (key == keepKey) continue;
      if (_locks.containsKey(key)) continue; // in use → skip
      final raf = _handles.remove(key);
      try {
        await raf?.close();
      } catch (_) {}
      if (_handles.length <= capacity) break;
    }
  }

  Future<T> _withLock<T>(String key, Future<T> Function() fn) async {
    final prev = _locks[key] ?? Future<void>.value();
    final completer = Completer<void>();
    _locks[key] = completer.future;
    await prev;
    try {
      return await fn();
    } finally {
      completer.complete();
      // Awaiting the already-complete future just discards it cleanly.
      if (identical(_locks[key], completer.future)) await _locks.remove(key);
    }
  }

  Future<void> closeAll() async {
    for (final raf in _handles.values) {
      try {
        await raf.close();
      } catch (_) {}
    }
    _handles.clear();
    _locks.clear();
  }
}
