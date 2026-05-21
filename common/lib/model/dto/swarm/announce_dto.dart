import 'package:common/model/dto/swarm/bitmap_dto.dart';

/// Receiver → (other receivers + sender): "here is my current bitmap".
/// Posted whenever a new chunk lands.
///
/// `fingerprint` identifies the announcing peer so receivers can update their
/// peer bitmap table.
class AnnounceDto {
  final String fingerprint;
  final BitmapDto bitmap;

  const AnnounceDto({required this.fingerprint, required this.bitmap});

  Map<String, dynamic> toJson() => {
    'fingerprint': fingerprint,
    'bitmap': bitmap.toJson(),
  };

  static AnnounceDto fromJson(Map<String, dynamic> map) => AnnounceDto(
    fingerprint: map['fingerprint'] as String,
    bitmap: BitmapDto.fromJson(map['bitmap'] as Map<String, dynamic>),
  );
}
