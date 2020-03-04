// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package copysets

import "context"

// CopysetID is a custom type for a cockroach copyset ID.
// Copyset is a group of stores where a range should be contained within
// if copyset based rebalancing is enabled.
type CopysetID int32

// Reader is used to read copysets in the cluster.
type Reader interface {
	// forRF returns copysets for the given replication factor.
	forRF(ctx context.Context, replicationFactor int32) (*Copysets, error)
}

// Maintainer maintains and assigns copysets to stores.
// TODO(hassan): implement Maintainer.
type Maintainer struct {
}

// Ensure Maintainer implements Reader.
var _ Reader = &Maintainer{}

// forRF returns copysets for the given replication factor.
func (cs *Maintainer) forRF(ctx context.Context, replicationFactor int32) (*Copysets, error) {
	return nil, nil
}
