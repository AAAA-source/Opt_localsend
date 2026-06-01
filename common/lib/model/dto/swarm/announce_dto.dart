import 'package:common/model/dto/swarm/bitmap_dto.dart';

/// Receiver → (other receivers + sender): "here are my current bitmaps".
/// Posted (coalesced) whenever new chunks land.
///
/// `fingerprint` identifies the announcing peer so receivers can update their
/// peer bitmap table. [bitmaps] carries one entry per file that changed since
/// the last announce, so many small files collapse into a single POST.
class AnnounceDto {
  final String fingerprint;
  final List<BitmapDto> bitmaps;

  const AnnounceDto({required this.fingerprint, required this.bitmaps});

  /// Convenience for the single-file case.
  factory AnnounceDto.single({
    required String fingerprint,
    required BitmapDto bitmap,
  }) =>
      AnnounceDto(fingerprint: fingerprint, bitmaps: [bitmap]);

  Map<String, dynamic> toJson() => {
        'fingerprint': fingerprint,
        'bitmaps': bitmaps.map((b) => b.toJson()).toList(),
        // Backward-compat: old peers read a single 'bitmap' field.
        if (bitmaps.length == 1) 'bitmap': bitmaps.first.toJson(),
      };

  static AnnounceDto fromJson(Map<String, dynamic> map) {
    final fingerprint = map['fingerprint'] as String;
    final rawList = map['bitmaps'];
    if (rawList is List) {
      return AnnounceDto(
        fingerprint: fingerprint,
        bitmaps: rawList
            .map((e) => BitmapDto.fromJson(e as Map<String, dynamic>))
            .toList(),
      );
    }
    // Legacy single-bitmap payload.
    return AnnounceDto(
      fingerprint: fingerprint,
      bitmaps: [BitmapDto.fromJson(map['bitmap'] as Map<String, dynamic>)],
    );
  }
}
