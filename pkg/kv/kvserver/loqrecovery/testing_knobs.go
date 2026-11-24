// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

type TestingKnobs struct {
	// MetadataScanTimeout controls how long loss of quorum recovery service will
	// wait for range metadata. Useful to speed up tests verifying collection
	// behaviors when meta ranges are unavailable.
	MetadataScanTimeout time.Duration

	// MaybeInjectError can be used to inject an error after replica info is
	// received when collecting fan-out data. It can be called concurrently,
	// so must be safe for concurrent use.
	MaybeInjectError func(*serverpb.RecoveryCollectLocalReplicaInfoResponse) error
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
