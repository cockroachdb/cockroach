// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colfetcher

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var cache specCache

func init() {
	cache.mu.deserialization = make(map[string]*fetchpb.IndexFetchSpec)
}

type specCache struct {
	mu struct {
		syncutil.RWMutex
		// TODO: implement eviction policy.
		deserialization map[string]*fetchpb.IndexFetchSpec
	}
}

func (c *specCache) deserialize(spec []byte) (*fetchpb.IndexFetchSpec, error) {
	key := string(spec)
	c.mu.RLock()
	s, ok := c.mu.deserialization[key]
	c.mu.RUnlock()
	if ok {
		return s, nil
	}
	s = &fetchpb.IndexFetchSpec{}
	if err := s.Unmarshal(spec); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.mu.deserialization[key] = s
	c.mu.Unlock()
	return s, nil
}
