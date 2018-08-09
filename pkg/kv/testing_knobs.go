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

package kv

import "github.com/cockroachdb/cockroach/pkg/base"

// ClientTestingKnobs contains testing options that dictate the behavior
// of the key-value client.
type ClientTestingKnobs struct {
	// The RPC dispatcher. Defaults to grpc but can be changed here for
	// testing purposes.
	TransportFactory TransportFactory

	// The maximum number of times a txn will attempt to refresh its
	// spans for a single transactional batch.
	// 0 means use a default. -1 means disable refresh.
	MaxTxnRefreshAttempts int
}

var _ base.ModuleTestingKnobs = &ClientTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ClientTestingKnobs) ModuleTestingKnobs() {}
