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

	// Replica filter for forwarded replica info when collecting fan-out data.
	// It can be called concurrently, so must be safe for concurrent use.
	ForwardReplicaFilter func(*serverpb.RecoveryCollectLocalReplicaInfoResponse) error
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
