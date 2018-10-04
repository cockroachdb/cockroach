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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package workload

import (
	"context"
	"sync/atomic"

	"github.com/jackc/pgx"
	"golang.org/x/sync/errgroup"
)

// MultiConnPool maintains a set of pgx ConnPools (to different servers).
type MultiConnPool struct {
	Pools []*pgx.ConnPool
	// Atomic counter used by Get().
	counter uint32
}

// NewMultiConnPool creates a new MultiConnPool (with one pool per url).
// The pools have approximately the same number of max connections, adding up to
// maxTotalConnections.
func NewMultiConnPool(maxTotalConnections int, urls ...string) (*MultiConnPool, error) {
	m := &MultiConnPool{
		Pools: make([]*pgx.ConnPool, len(urls)),
	}
	for i := range urls {
		cfg, err := pgx.ParseConnectionString(urls[i])
		if err != nil {
			return nil, err
		}
		// Use the average number of remaining connections (this handles
		// rounding).
		numConn := maxTotalConnections / (len(urls) - i)
		maxTotalConnections -= numConn
		p, err := pgx.NewConnPool(pgx.ConnPoolConfig{
			ConnConfig:     cfg,
			MaxConnections: numConn,
		})
		if err != nil {
			return nil, err
		}

		// "Warm up" the pool so we don't have to establish connections later (which
		// would affect the observed latencies of the first requests). We do this by
		// acquiring all connections (in parallel), then releasing them back to the
		// pool.
		conns := make([]*pgx.Conn, numConn)
		var g errgroup.Group
		for i := range conns {
			i := i
			g.Go(func() error {
				conns[i], err = p.Acquire()
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
		for _, c := range conns {
			p.Release(c)
		}

		m.Pools[i] = p
	}
	return m, nil
}

// Get returns one of the pools, in round-robin manner.
func (m *MultiConnPool) Get() *pgx.ConnPool {
	i := atomic.AddUint32(&m.counter, 1) - 1
	return m.Pools[i%uint32(len(m.Pools))]
}

// PrepareEx prepares the given statement on all the pools.
func (m *MultiConnPool) PrepareEx(
	ctx context.Context, name, sql string, opts *pgx.PrepareExOptions,
) (*pgx.PreparedStatement, error) {
	var ps *pgx.PreparedStatement
	for _, p := range m.Pools {
		var err error
		ps, err = p.PrepareEx(ctx, name, sql, opts)
		if err != nil {
			return nil, err
		}
	}
	// It doesn't matter which PreparedStatement we return, they should be the
	// same.
	return ps, nil
}

// Close closes all the pools.
func (m *MultiConnPool) Close() {
	for _, p := range m.Pools {
		p.Close()
	}
}
