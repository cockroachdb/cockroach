// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import "sync"

// A store implements the key-value interface, but coordinates
// writes between a raft consensus group.
type store struct {
	engine    Engine            // The underlying key-value store.
	allocator *allocator        // Makes allocation decisions.
	mu        sync.Mutex        // Protects the ranges map.
	ranges    map[string]*Range // Map of ranges by range start key.
}

// newStore returns a new instance of a store.
func newStore(engine Engine, allocator *allocator) *store {
	return &store{
		engine:    engine,
		allocator: allocator,
		ranges:    make(map[string]*Range),
	}
}

// getRange fetches a range by looking at Replica.StartKey. The range is
// fetched quickly if it's already been loaded and is in the ranges
// map; otherwise, an instance of the range is instantiated from the
// underlying store.
func (s *store) getRange(r *Replica) (*Range, error) {
	if rng, ok := s.ranges[string(r.RangeKey)]; ok {
		return rng, nil
	}
	rng, err := NewRange(r.RangeKey, s.engine, s.allocator)
	if err != nil {
		return nil, err
	}
	s.ranges[string(r.RangeKey)] = rng
	return rng, nil
}
