/// Bitmap of received chunks for a single (sessionId, fileId).
/// Compact representation: packed bytes, LSB-first within each byte.
class BitmapDto {
  final String sessionId;
  final String fileId;
  final int totalChunks;
  final List<int> bits; // packed bytes

  const BitmapDto({
    required this.sessionId,
    required this.fileId,
    required this.totalChunks,
    required this.bits,
  });

  bool has(int index) {
    final byte = index >> 3;
    if (byte >= bits.length) return false;
    return (bits[byte] >> (index & 7)) & 1 == 1;
  }

  int get receivedCount {
    var count = 0;
    for (var i = 0; i < totalChunks; i++) {
      if (has(i)) count++;
    }
    return count;
  }

  Map<String, dynamic> toJson() => {
        'sessionId': sessionId,
        'fileId': fileId,
        'totalChunks': totalChunks,
        'bits': bits,
      };

  static BitmapDto fromJson(Map<String, dynamic> map) => BitmapDto(
        sessionId: map['sessionId'] as String,
        fileId: map['fileId'] as String,
        totalChunks: map['totalChunks'] as int,
        bits: (map['bits'] as List).cast<int>(),
      );

  /// Build an all-zero bitmap of the right byte length for [totalChunks].
  static BitmapDto empty({
    required String sessionId,
    required String fileId,
    required int totalChunks,
  }) {
    return BitmapDto(
      sessionId: sessionId,
      fileId: fileId,
      totalChunks: totalChunks,
      bits: List<int>.filled((totalChunks + 7) >> 3, 0),
    );
  }
}
