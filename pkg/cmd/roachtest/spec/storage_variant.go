// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spec

// StorageVariant represents a specific storage configuration (volume type,
// filesystem, or local SSD preference) that should be tested as a distinct,
// named test variant. This replaces random volume type and filesystem
// selection at cluster creation time, ensuring that each test run uses a
// deterministic infrastructure configuration identified by its test name.
//
// Tests register one variant per StorageVariant, restricting each to the
// appropriate cloud provider. For example, a test compatible with AllClouds
// would produce separate registrations for "vol=gp3" (AWS-only),
// "vol=pd-ssd" (GCE-only), and so on.
//
// Storage variants cover two orthogonal axes:
//   - Volume type (remote storage): cloud-specific types like gp3, pd-ssd, etc.
//   - Local SSD: uses PreferLocalSSD(), applicable on clouds that support it
//     (GCE, AWS, Azure).
//
// These are separate because local SSD is controlled via PreferLocalSSD() /
// DisableLocalSSD() flags, not via VolumeType(). Tests that use
// DisableLocalSSD() should pass skipLocalSSD=true to the storageVariantRegs
// helper to exclude local-SSD variants.
type StorageVariant struct {
	// Name is the suffix appended to the test name (e.g., "vol=gp3",
	// "fs=xfs", "local-ssd"). The test registers as "<baseName>/<Name>".
	Name string
	// VolumeType is the cloud-specific volume type string (e.g., "gp3",
	// "pd-ssd"). Empty means use the cloud provider's default.
	VolumeType string
	// FileSystem overrides the filesystem. Zero value (Ext4) means default.
	FileSystem fileSystemType
	// PreferLocalSSD, when true, adds the PreferLocalSSD() option instead
	// of a VolumeType. This is mutually exclusive with VolumeType.
	PreferLocalSSD bool
	// Cloud restricts this variant to a specific cloud provider. AnyCloud
	// means the variant is cloud-agnostic (e.g., filesystem variants).
	Cloud Cloud
}

// IsLocalSSD returns true if this variant selects local-SSD storage.
func (sv StorageVariant) IsLocalSSD() bool {
	return sv.PreferLocalSSD
}

// ClusterSpecOptions returns the spec.Option values to apply when creating
// a cluster for this variant.
func (sv StorageVariant) ClusterSpecOptions() []Option {
	var opts []Option
	if sv.VolumeType != "" {
		opts = append(opts, VolumeType(sv.VolumeType))
	}
	if sv.PreferLocalSSD {
		opts = append(opts, PreferLocalSSD())
	}
	opts = append(opts, SetFileSystem(sv.FileSystem))
	return opts
}

// CloudVolumeTypes returns the available remote (non-local-SSD) volume types
// for the given cloud provider. This is the single source of truth used by
// both StorageVariants (for deterministic benchmark variants) and
// RandomizeVolumeType (for non-benchmark coverage testing).
func CloudVolumeTypes(cloud Cloud) []string {
	switch cloud {
	case AWS:
		return []string{"gp3", "io2"}
	case GCE:
		return []string{"pd-ssd"}
	case Azure:
		return []string{"premium-ssd", "premium-ssd-v2", "ultra-disk"}
	default:
		return nil
	}
}

// StorageVariants returns the standard set of non-default storage
// configurations. Each entry produces a separate named test registration,
// replacing the former RandomizeVolumeType() / RandomlyUseXfs() approach
// for benchmark tests.
//
// The returned variants are the cross-product of:
//   - Volume types: per-cloud types from CloudVolumeTypes, plus local-SSD
//   - Filesystems: ext4 and xfs
//
// Every variant has an explicit filesystem suffix (e.g., "vol=gp3/fs=ext4"
// and "vol=gp3/fs=xfs"). The base test (no variant suffix) uses the cloud's
// default volume type and OS-default filesystem.
//
// Use the storageVariantRegs helper in the tests package to automatically
// handle cloud filtering and local-SSD exclusion for tests with
// DisableLocalSSD().
func StorageVariants() []StorageVariant {
	type baseVariant struct {
		name           string
		volumeType     string
		preferLocalSSD bool
		cloud          Cloud
	}
	bases := []baseVariant{}
	for _, cloud := range []Cloud{AWS, GCE, Azure} {
		for _, vt := range CloudVolumeTypes(cloud) {
			bases = append(bases, baseVariant{
				name:       "vol=" + vt,
				volumeType: vt,
				cloud:      cloud,
			})
		}
	}
	// Local SSD variant. Uses PreferLocalSSD() which is cloud-agnostic:
	// it enables local SSDs where available (GCE AMD64, AWS with 'd'/'i3'
	// machine types, Azure) and falls back to the default volume type
	// otherwise.
	bases = append(bases, baseVariant{
		name:           "local-ssd",
		preferLocalSSD: true,
		cloud:          AnyCloud,
	})

	// Cross with filesystems: ext4 (default) and xfs.
	// Cross each volume type / local-SSD base with both ext4 and xfs.
	var variants []StorageVariant
	for _, b := range bases {
		variants = append(variants, StorageVariant{
			Name:           b.name + "/fs=ext4",
			VolumeType:     b.volumeType,
			PreferLocalSSD: b.preferLocalSSD,
			FileSystem:     Ext4,
			Cloud:          b.cloud,
		})
		variants = append(variants, StorageVariant{
			Name:           b.name + "/fs=xfs",
			VolumeType:     b.volumeType,
			PreferLocalSSD: b.preferLocalSSD,
			FileSystem:     Xfs,
			Cloud:          b.cloud,
		})
	}
	return variants
}
