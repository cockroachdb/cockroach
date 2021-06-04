// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package layered

import "github.com/cockroachdb/cockroach/pkg/roachpb"

type Supporter interface {
	// SupportsBinaryVersion determines whether an operation supports
	// a cluster running at the given binary version.
	//
	// TODO: need to check if we want version.Version here. In general
	// should clean up the versions across roachtest; it's a mess.
	SupportsBinaryVersion(roachpb.Version) VersionSupportType
}

// AtLeastSupporter is a supporter that requires a binary version of at least 'AtLeast'.
// If SupportsMixed is true, also supports a cluster in which a supported version is
// running mixed with an older version.
type AtLeastSupporter struct {
	AtLeast       roachpb.Version
	SupportsMixed bool
}

func (s AtLeastSupporter) SupportsBinaryVersion(v roachpb.Version) VersionSupportType {
	if v.Less(s.AtLeast) {
		// Introduced at 20.1, but we're testing only v19.X or below.
		return Unsupported
	}
	// Introduced at 20.1, and we're testing at least that version.
	if s.SupportsMixed {
		// Supports a mixed 20.1/19.2 cluster.
		return CanMixWithPredecessor
	}
	// Need cluster version to be at least 20.1.
	return OnlyFinalized
}

// AtLeastV21Dot2MixedSupporter is a struct that can be embedded into Stepper implementations
// to implement an AtLeastSupporter{AtLeast: v21.2, SupportsMixed: true}. This should be the
// default choice for Steppers introduced prior to the v21.2 release.
type AtLeastV21Dot2MixedSupporter struct{}

func (*AtLeastV21Dot2MixedSupporter) SupportsBinaryVersion(v roachpb.Version) VersionSupportType {
	return AtLeastSupporter{
		AtLeast:       roachpb.Version{Major: 21, Minor: 2},
		SupportsMixed: true,
	}.SupportsBinaryVersion(v)
}

type VersionSupportType byte

const (
	Unsupported           VersionSupportType = iota // does not support this version
	OnlyFinalized                                   // supports cluster only if all nodes at main version & cluster version finalized
	CanMixWithPredecessor                           // supports cluster as long as one node is at main version
)
