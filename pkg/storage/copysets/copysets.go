// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
