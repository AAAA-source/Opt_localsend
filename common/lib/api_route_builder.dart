import 'package:common/model/device.dart';

const _basePath = '/api/localsend';

/// Type-safe API paths
enum ApiRoute {
  info('info'),
  register('register'),
  prepareUpload('prepare-upload', 'send-request'),
  upload('upload', 'send'),
  cancel('cancel'),
  show('show'),
  prepareDownload('prepare-download'),
  download('download'),

  // v3 (swarm/HopSwift) routes — only the v3 string is meaningful;
  // v1/v2 fields are defined for enum uniformity but never reached on the wire.
  prepareSwarm('prepare-swarm'),
  uploadChunk('upload-chunk'),
  downloadChunk('chunk'),
  bitmap('bitmap'),
  announce('announce');

  const ApiRoute(String path, [String? legacy])
    : v1 = '$_basePath/v1/${legacy ?? path}',
      v2 = '$_basePath/v2/$path',
      v3 = '$_basePath/v3/$path';

  /// The server url for v1
  final String v1;

  /// The server url for v2
  final String v2;

  /// The server url for v3 (swarm)
  final String v3;

  /// The client url
  String target(Device target, {Map<String, String>? query}) {
    return Uri(
      scheme: target.https ? 'https' : 'http',
      host: target.ip,
      port: target.port,
      path: _pickPath(target.version),
      queryParameters: query,
    ).toString();
  }

  /// The client url for polling
  String targetRaw(String ip, int port, bool https, String version) {
    final protocol = https ? 'https' : 'http';
    return '$protocol://$ip:$port${_pickPath(version)}';
  }

  String _pickPath(String version) {
    if (version.startsWith('3')) return v3;
    if (version == '1.0') return v1;
    return v2;
  }
}
