/// Receiver → sender response to /v3/prepare-swarm.
///
/// `tokens` maps fileId → per-file upload token (used to authorize chunk uploads
/// and peer pulls). Same token is reused across all chunks of a file.
/// Empty `tokens` means the receiver declined or selected nothing.
class PrepareSwarmResponseDto {
  final String sessionId;
  final Map<String, String> tokens;

  const PrepareSwarmResponseDto({
    required this.sessionId,
    required this.tokens,
  });

  Map<String, dynamic> toJson() => {'sessionId': sessionId, 'tokens': tokens};

  static PrepareSwarmResponseDto fromJson(Map<String, dynamic> map) =>
      PrepareSwarmResponseDto(
        sessionId: map['sessionId'] as String,
        tokens: (map['tokens'] as Map<String, dynamic>).cast<String, String>(),
      );
}
