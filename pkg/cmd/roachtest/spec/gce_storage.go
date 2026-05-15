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

// GCEStorageDecision returns the storage intent for a GCE roachtest cluster.
// Keep this as the single policy entry point: archForTest uses it to decide
// whether ARM64 should stay on C4A, switch to T2A for pd-ssd, or fall back to
// AMD64; RoachprodOpts uses the same result to set roachprod storage flags.
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
