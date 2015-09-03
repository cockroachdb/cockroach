// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package main

import (
	"sync"

	"github.com/cockroachdb/cockroach/proto"
)

const defaultReplicationFactor = 3

type rng struct {
	sync.RWMutex
	factor int // replication factor
	desc   proto.RangeDescriptor
}

func newRange(rangeID proto.RangeID) *rng {
	return &rng{
		desc: proto.RangeDescriptor{
			RangeID: rangeID,
		},
		factor: defaultReplicationFactor,
	}
}

func (r *rng) getDesc() proto.RangeDescriptor {
	r.RLock()
	defer r.RUnlock()
	return r.desc
}

func (r *rng) getFactor() int {
	r.RLock()
	defer r.RUnlock()
	return r.factor
}

func (r *rng) setFactor(factor int) {
	r.Lock()
	defer r.Unlock()
	r.factor = factor
}

/*
func (r *rng) splitRange(newRng *rng) *rng {
	r.Lock()
	defer r.Unlock()
}*/
