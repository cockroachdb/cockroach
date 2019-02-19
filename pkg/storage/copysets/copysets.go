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

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// CopysetID is a custom type for a cockroach copyset ID.
// Copyset is a group of stores where a range should be contained within
// if copyset based rebalancing is enabled.
type CopysetID int32

// Enabled is a setting which denotes whether copysets are enabled.
var Enabled = settings.RegisterBoolSetting(
	"kv.allocator.enable_copysets",
	"enable copysets to improve tolerance to multi node failures. "+
		"This feature is currently experimental",
	false,
)

// StoreRebalancerEnabled is a setting which denotes whether the store
// rebalancer should take copysets into account while rebalancing replicas.
var StoreRebalancerEnabled = settings.RegisterBoolSetting(
	"kv.allocator.enable_store_rebalancer_with_copysets",
	"enable store rebalancer feature with copysets",
	true,
)

// Reader is used to read copysets in the cluster.
type Reader interface {
	// ForRF returns copysets for the given replication factor.
	ForRF(ctx context.Context, replicationFactor *int32) (*Copysets, error)
}

// Maintainer maintains and assigns copysets to stores.
// TODO(hassan): implement Maintainer.
type Maintainer struct {
}

// Ensure Maintainer implements Reader.
var _ Reader = &Maintainer{}

// ForRF returns copysets for the given replication factor.
func (cs *Maintainer) ForRF(ctx context.Context, replicationFactor *int32) (*Copysets, error) {
	return nil, nil
}
