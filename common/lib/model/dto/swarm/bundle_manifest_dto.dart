/// Manifest describing how a swarm **bundle** unit is composed of several
/// original small files concatenated into one contiguous byte space.
///
/// A bundle is treated as a single swarm unit (one [ChunkPlanDto] over the
/// concatenation). This manifest maps a bundle byte-offset back to the original
/// files so the receiver can scatter each incoming chunk into the right
/// destination file(s).
class BundleEntryDto {
  /// Original [FileDto.id] of the member file.
  final String fileId;

  /// Byte length of the member file.
  final int size;

  /// Start offset of the member within the bundle's virtual byte space
  /// (prefix sum of the preceding members' sizes).
  final int offset;

  const BundleEntryDto({
    required this.fileId,
    required this.size,
    required this.offset,
  });

  Map<String, dynamic> toJson() => {
        'fileId': fileId,
        'size': size,
        'offset': offset,
      };

  static BundleEntryDto fromJson(Map<String, dynamic> map) => BundleEntryDto(
        fileId: map['fileId'] as String,
        size: map['size'] as int,
        offset: map['offset'] as int,
      );
}

class BundleManifestDto {
  /// Synthetic id of the bundle unit (used as the swarm unitId / plan key).
  final String bundleId;

  /// Member files in concatenation order.
  final List<BundleEntryDto> entries;

  const BundleManifestDto({required this.bundleId, required this.entries});

  Map<String, dynamic> toJson() => {
        'bundleId': bundleId,
        'entries': entries.map((e) => e.toJson()).toList(),
      };

  static BundleManifestDto fromJson(Map<String, dynamic> map) => BundleManifestDto(
        bundleId: map['bundleId'] as String,
        entries: (map['entries'] as List)
            .map((e) => BundleEntryDto.fromJson(e as Map<String, dynamic>))
            .toList(),
      );
}
