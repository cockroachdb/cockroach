// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spec

// GCEStorageKind is the storage intent selected from ClusterSpec fields before
// machine-family compatibility is applied.
type GCEStorageKind int

const (
	// GCEStorageDefaultRemote means no storage option explicitly selected a
	// volume type, and the caller should use the default remote disk for the
	// selected machine family.
	GCEStorageDefaultRemote GCEStorageKind = iota
	// GCEStorageLocalSSD means the cluster should try to use local SSDs.
	GCEStorageLocalSSD
	// GCEStoragePDSSD means the cluster needs pd-ssd. On ARM64 this is the only
	// storage decision that should select T2A instead of the normal C4A default.
	GCEStoragePDSSD
	// GCEStorageExplicitRemote means the cluster explicitly selected a non-local,
	// non-pd-ssd remote volume type.
	GCEStorageExplicitRemote
	// GCEStorageRandomized means RandomizeVolumeType should choose from the
	// machine-compatible storage types later, after the machine family is known.
	GCEStorageRandomized
)

// GCEStorageDecision records the result of the GCE storage decision tree. Reason
// is intentionally written for logs and comments at the call sites, so the C4A
// vs T2A vs AMD64 transitions are explainable without re-reading this function.
type GCEStorageDecision struct {
	Kind       GCEStorageKind
	VolumeType string
	Reason     string
}

func (d GCEStorageDecision) UsesLocalSSD() bool {
	return d.Kind == GCEStorageLocalSSD
}

func (d GCEStorageDecision) RequiresPDSSD() bool {
	return d.Kind == GCEStoragePDSSD
}

// GCEStorageDecision returns the storage intent for a GCE roachtest cluster,
// before applying machine-family compatibility. Keep this as the single storage
// intent entry point: archForTest uses it to decide whether ARM64 should stay on
// C4A, switch to T2A for pd-ssd, or fall back to AMD64; RoachprodOpts uses the
// same result to set roachprod storage flags.
//
// Not every local-SSD intent has the same strength when the selected C4A shape
// cannot satisfy it. VolumeType("local-ssd") is a hard requirement. Benchmark
// local SSD, whether selected by PreferLocalSSD() or by the global
// defaultPreferLocalSSD flag, also preserves local SSD by falling back to AMD64;
// benchmark storage/store-count continuity wins over ARM64. Non-benchmark local
// SSD preferences are weaker: if C4A has no compatible -lssd shape, RoachprodOpts
// may keep C4A and fall back to the machine-compatible remote disk instead.
// Randomized storage is different again; its choices are built after the machine
// family is known, so it only includes local SSD when local SSD is actually
// available.
//
// The decision order intentionally preserves RoachprodOpts' legacy storage
// priority:
//
//   - If VolumeType is set:
//     local-ssd -> local SSD
//     pd-ssd -> pd-ssd
//     other -> explicit remote type
//
//   - If VolumeType is empty, continue with implicit storage selection:
//
//   - VolumeSize != 0:
//     RandomizeVolumeType -> randomized remote storage
//     benchmark -> pd-ssd, preserving benchmark history
//     otherwise -> machine-compatible default remote storage
//
//   - LocalSSD == LocalSSDPreferOn:
//     local SSD preference. This intentionally wins over RandomizeVolumeType,
//     but only benchmarks treat it as strong enough to force C4A -> AMD64 when no
//     compatible C4A -lssd shape exists.
//
//   - RandomizeVolumeType:
//     choose later from machine-compatible storage types. This never selects
//     unavailable local SSD because the random choice is made after machine
//     selection and local-SSD availability checks.
//
//   - LocalSSD == LocalSSDDisable:
//     benchmark -> pd-ssd
//     otherwise -> machine-compatible default remote storage
//
//   - LocalSSD == LocalSSDDefault:
//     defaultPreferLocalSSD -> local SSD preference. For benchmarks, this is
//     strong enough to force C4A -> AMD64 when no compatible C4A -lssd shape
//     exists. For non-benchmarks, it may fall back to remote storage.
//     benchmark -> pd-ssd
//     otherwise -> machine-compatible default remote storage
func (s *ClusterSpec) GCEStorageDecision(
	benchmark bool, defaultPreferLocalSSD bool,
) GCEStorageDecision {
	switch s.VolumeType {
	case "local-ssd":
		return GCEStorageDecision{Kind: GCEStorageLocalSSD, VolumeType: "local-ssd", Reason: "the cluster explicitly requested local-ssd"}
	case "pd-ssd":
		return GCEStorageDecision{Kind: GCEStoragePDSSD, VolumeType: "pd-ssd", Reason: "the cluster explicitly requested pd-ssd"}
	case "":
		// Keep walking the implicit-storage decision tree below.
	default:
		return GCEStorageDecision{Kind: GCEStorageExplicitRemote, VolumeType: s.VolumeType, Reason: "the cluster explicitly requested a remote volume type"}
	}

	if s.VolumeSize != 0 {
		if s.RandomizeVolumeType {
			return GCEStorageDecision{Kind: GCEStorageRandomized, Reason: "the cluster requested randomized remote storage"}
		}
		if benchmark {
			return GCEStorageDecision{
				Kind:       GCEStoragePDSSD,
				VolumeType: "pd-ssd",
				Reason:     "this benchmark selected remote storage by setting a volume size without an explicit volume type",
			}
		}
		return GCEStorageDecision{Kind: GCEStorageDefaultRemote, Reason: "local SSDs cannot be used with a non-zero volume size"}
	}

	if s.LocalSSD == LocalSSDPreferOn {
		return GCEStorageDecision{Kind: GCEStorageLocalSSD, VolumeType: "local-ssd", Reason: "the cluster prefers local SSDs"}
	}

	if s.RandomizeVolumeType {
		return GCEStorageDecision{Kind: GCEStorageRandomized, Reason: "the cluster requested randomized storage"}
	}

	switch s.LocalSSD {
	case LocalSSDDisable:
		if benchmark {
			return GCEStorageDecision{
				Kind:       GCEStoragePDSSD,
				VolumeType: "pd-ssd",
				Reason:     "this benchmark disabled local SSDs without an explicit volume type",
			}
		}
		return GCEStorageDecision{Kind: GCEStorageDefaultRemote, Reason: "local SSDs are disabled"}

	case LocalSSDDefault:
		if defaultPreferLocalSSD {
			return GCEStorageDecision{Kind: GCEStorageLocalSSD, VolumeType: "local-ssd", Reason: "the default roachtest storage preference is local SSD"}
		}
		if benchmark {
			return GCEStorageDecision{
				Kind:       GCEStoragePDSSD,
				VolumeType: "pd-ssd",
				Reason:     "this benchmark is using remote storage because the default local SSD preference is disabled",
			}
		}
		return GCEStorageDecision{Kind: GCEStorageDefaultRemote, Reason: "the default local SSD preference is disabled"}

	default:
		return GCEStorageDecision{Kind: GCEStorageDefaultRemote, Reason: "no GCE storage preference selected local SSD or pd-ssd"}
	}
}
