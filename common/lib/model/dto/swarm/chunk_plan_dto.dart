/// Chunking metadata for a single file in a swarm transfer.
/// Computed by the sender once before the transfer starts and shared with all peers.
class ChunkPlanDto {
  /// File id this plan refers to (same as [FileDto.id]).
  final String fileId;

  /// Total chunk count. Equals `ceil(fileSize / chunkSize)`.
  final int totalChunks;

  /// Size of every chunk except the last one.
  final int chunkSize;

  /// Size of the last chunk (always `<= chunkSize`).
  final int lastChunkSize;

  /// SHA256 (hex, lowercase) for each chunk in order [0..totalChunks).
  final List<String> sha256PerChunk;

  /// SHA256 (hex) of the full file, used for final verification.
  final String fileSha256;

  const ChunkPlanDto({
    required this.fileId,
    required this.totalChunks,
    required this.chunkSize,
    required this.lastChunkSize,
    required this.sha256PerChunk,
    required this.fileSha256,
  });

  int chunkOffset(int index) => index * chunkSize;

  int chunkLength(int index) => index == totalChunks - 1 ? lastChunkSize : chunkSize;

  Map<String, dynamic> toJson() => {
        'fileId': fileId,
        'totalChunks': totalChunks,
        'chunkSize': chunkSize,
        'lastChunkSize': lastChunkSize,
        'sha256PerChunk': sha256PerChunk,
        'fileSha256': fileSha256,
      };

  static ChunkPlanDto fromJson(Map<String, dynamic> map) => ChunkPlanDto(
        fileId: map['fileId'] as String,
        totalChunks: map['totalChunks'] as int,
        chunkSize: map['chunkSize'] as int,
        lastChunkSize: map['lastChunkSize'] as int,
        sha256PerChunk: (map['sha256PerChunk'] as List).cast<String>(),
        fileSha256: map['fileSha256'] as String,
      );
}
