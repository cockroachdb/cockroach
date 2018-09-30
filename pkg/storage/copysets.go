// Copyright 2018 The Cockroach Authors.
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

package storage

import "context"

// copysetsReader is used to read copysets in the cluster.
type copysetsReader interface {
	// forRF returns copysets for the given replication factor.
	forRF(ctx context.Context, replicationFactor int32) (*Copysets, error)
}

// CopysetsMaintainer maintains and assigns copysets to stores.
// TODO(hassan): implement CopysetsMaintainer.
type CopysetsMaintainer struct {
}

// Ensure CopysetsMaintainer implements copysetsReader.
var _ copysetsReader = &CopysetsMaintainer{}

// forRF returns copysets for the given replication factor.
func (cs *CopysetsMaintainer) forRF(
	ctx context.Context, replicationFactor int32,
) (*Copysets, error) {
	return nil, nil
}
