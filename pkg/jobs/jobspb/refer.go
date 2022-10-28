// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package jobspb

import "github.com/cockroachdb/cockroach/pkg/clusterversion"

func init() {
	// Waiting for the index/table to expire before issuing ClearRange
	// requests over the table span.
	//
	// TODO(ajwerner): Remove this in 23.1.
	clusterversion.Refer(clusterversion.V22_2, SchemaChangeGCProgress_WAITING_FOR_CLEAR)
	// The GC TTL has expired. This element is marked for imminent deletion
	// or is being cleared.
	//
	// TODO(ajwerner): Remove this in 23.1.
	clusterversion.Refer(clusterversion.V22_2, SchemaChangeGCProgress_CLEARING)
}
