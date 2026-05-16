import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:common/model/stored_security_context.dart';
import 'package:rhttp/rhttp.dart';

/// Thin wrapper around [RhttpClient] for swarm-only HTTP ops:
/// JSON POST, GET text, raw bytes POST/GET.
///
/// One instance per swarm session is enough; reuse across all peers.
class SwarmHttpClient {
  final RhttpClient _client;

  SwarmHttpClient._(this._client);

  factory SwarmHttpClient.create(StoredSecurityContext security) {
    final client = RhttpClient.createSync(
      settings: ClientSettings(
        timeoutSettings: const TimeoutSettings(
          // long timeout: chunk upload of 4MiB on slow Wi-Fi can take >30s
          timeout: Duration(minutes: 10),
        ),
        tlsSettings: TlsSettings(
          verifyCertificates: false,
          clientCertificate: ClientCertificate(
            certificate: security.certificate,
            privateKey: security.privateKey,
          ),
        ),
      ),
    );
    return SwarmHttpClient._(client);
  }

  Future<String> postJson({
    required String url,
    Map<String, String> query = const {},
    required Map<String, dynamic> body,
  }) async {
    final response = await _client.post(
      url,
      query: query,
      body: HttpBody.json(body),
    );
    return response.body;
  }

  Future<String> getString({
    required String url,
    Map<String, String> query = const {},
  }) async {
    final response = await _client.get(url, query: query);
    return response.body;
  }

  /// POST raw bytes (chunk upload). Body is wrapped as a single-element stream.
  Future<void> postBytes({
    required String url,
    required Map<String, String> query,
    required Uint8List bytes,
  }) async {
    await _client.request(
      method: HttpMethod.post,
      expectBody: HttpExpectBody.bytes,
      url: url,
      query: query,
      headers: HttpHeaders.rawMap({
        'Content-Length': bytes.length.toString(),
        'Content-Type': 'application/octet-stream',
      }),
      body: HttpBody.stream(Stream.value(bytes), length: bytes.length),
    );
  }

  /// GET that returns raw bytes (peer-to-peer chunk pull).
  Future<Uint8List> getBytes({
    required String url,
    required Map<String, String> query,
  }) async {
    final response = await _client.request(
      method: HttpMethod.get,
      expectBody: HttpExpectBody.bytes,
      url: url,
      query: query,
    );
    // `body` on a bytes-expecting response is Uint8List
    final dynamic body = (response as dynamic).body;
    if (body is Uint8List) return body;
    if (body is List<int>) return Uint8List.fromList(body);
    throw StateError('Unexpected response body type: ${body.runtimeType}');
  }

  void dispose() {
    // RhttpClient does not expose an explicit close in this version;
    // drop the reference and let GC handle it.
  }
}

/// Decode a string body as JSON map.
Map<String, dynamic> decodeJsonBody(String body) {
  return jsonDecode(body) as Map<String, dynamic>;
}
