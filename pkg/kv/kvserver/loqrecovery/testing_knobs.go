// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	ForwardReplicaFilter func(*serverpb.RecoveryCollectLocalReplicaInfoResponse) error
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
