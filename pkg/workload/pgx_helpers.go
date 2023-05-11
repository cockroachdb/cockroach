// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workload

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

// MultiConnPool maintains a set of pgx ConnPools (to different servers).
type MultiConnPool struct {
	Pools []*pgxpool.Pool
	// Atomic counter used by Get().
	counter uint32

	mu struct {
		syncutil.RWMutex
		// preparedStatements is a map from name to SQL. The statements in the map
		// are prepared whenever a new connection is acquired from the pool.
		preparedStatements map[string]string
	}

	// NOTE(seanc@): method is on the hot-path, therefore make all reads to the
	// query exec mode a dirty read.
	method pgx.QueryExecMode
}

// MultiConnPoolCfg encapsulates the knobs passed to NewMultiConnPool.
type MultiConnPoolCfg struct {
	// MaxTotalConnections is the total maximum number of connections across all
	// pools.
	MaxTotalConnections int

	// MaxConnsPerPool is the maximum number of connections in any single pool.
	// Limiting this is useful especially for prepared statements, which are
	// prepared on each connection inside a pool (serially).
	// If 0, there is no per-pool maximum (other than the total maximum number of
	// connections which still applies).
	MaxConnsPerPool int

	// ConnHealthCheckPeriod specifies the amount of time between connection
	// health checks.  Defaults to 10% of MaxConnLifetime.
	ConnHealthCheckPeriod time.Duration

	// MaxConnIdleTime specifies the amount of time a connection will be idle
	// before being closed by the health checker.  Defaults to 50% of the
	// MaxConnLifetime.
	MaxConnIdleTime time.Duration

	// MaxConnLifetime specifies the max age of individual connections in
	// connection pools.  If 0, a default value of 5 minutes is used.
	MaxConnLifetime time.Duration

	// MaxConnLifetimeJitter shortens the max age of a connection by a random
	// duration less than the specified jitter.  If 0, default to 50% of
	// MaxConnLifetime.
	MaxConnLifetimeJitter time.Duration

	// Method specifies the query type to use for the  PG wire protocol.
	Method string

	// MinConns is the minimum number of connections the connection pool will
	// attempt to keep.  Connection count may dip below this value periodically,
	// see pgxpool documentation for details.
	MinConns int

	// WarmupConns specifies the number of connections to prewarm when
	// initializing a MultiConnPool.  A value of 0 automatically initialize the
	// max number of connections per pool.  A value less than 0 skips the
	// connection warmup phase.
	WarmupConns int
}

// NewMultiConnPoolCfgFromFlags constructs a new MultiConnPoolCfg object based
// on the connection flags.
func NewMultiConnPoolCfgFromFlags(cf *ConnFlags) MultiConnPoolCfg {
	return MultiConnPoolCfg{
		ConnHealthCheckPeriod: cf.ConnHealthCheckPeriod,
		MaxConnIdleTime:       cf.MaxConnIdleTime,
		MaxConnLifetime:       cf.MaxConnLifetime,
		MaxConnLifetimeJitter: cf.MaxConnLifetimeJitter,
		MaxConnsPerPool:       cf.Concurrency,
		MaxTotalConnections:   cf.Concurrency,
		Method:                cf.Method,
		MinConns:              cf.MinConns,
		WarmupConns:           cf.WarmupConns,
	}
}

// String values taken from pgx.ParseConfigWithOptions() to maintain
// compatibility with pgx.  See [1] and [2] for additional details.
//
// [1] https://github.com/jackc/pgx/blob/fa5fbed497bc75acee05c1667a8760ce0d634cba/conn.go#L167-L182
// [2] https://github.com/jackc/pgx/blob/fa5fbed497bc75acee05c1667a8760ce0d634cba/conn.go#L578-L612
var stringToMethod = map[string]pgx.QueryExecMode{
	"cache_statement": pgx.QueryExecModeCacheStatement,
	"cache_describe":  pgx.QueryExecModeCacheDescribe,
	"describe_exec":   pgx.QueryExecModeDescribeExec,
	"exec":            pgx.QueryExecModeExec,
	"simple_protocol": pgx.QueryExecModeSimpleProtocol,

	// Preserve backward compatibility with original workload --method's
	"prepare":   pgx.QueryExecModeCacheStatement,
	"noprepare": pgx.QueryExecModeExec,
	"simple":    pgx.QueryExecModeSimpleProtocol,
}

// NewMultiConnPool creates a new MultiConnPool.
//
// Each URL gets one or more pools, and each pool has at most MaxConnsPerPool
// connections.
//
// The pools have approximately the same number of max connections, adding up to
// MaxTotalConnections.
func NewMultiConnPool(
	ctx context.Context, cfg MultiConnPoolCfg, urls ...string,
) (*MultiConnPool, error) {
	m := &MultiConnPool{}
	m.mu.preparedStatements = map[string]string{}

	maxConnLifetime := 300 * time.Second
	if cfg.MaxConnLifetime > 0 {
		maxConnLifetime = cfg.MaxConnLifetime
	}
	maxConnLifetimeJitter := time.Duration(0.5 * float64(maxConnLifetime))
	if cfg.MaxConnLifetimeJitter > 0 {
		maxConnLifetimeJitter = cfg.MaxConnLifetimeJitter
	}
	connHealthCheckPeriod := time.Duration(0.1 * float64(maxConnLifetime))
	if cfg.ConnHealthCheckPeriod > 0 {
		connHealthCheckPeriod = cfg.ConnHealthCheckPeriod
	}
	maxConnIdleTime := time.Duration(0.5 * float64(maxConnLifetime))
	if cfg.MaxConnIdleTime > 0 {
		maxConnIdleTime = cfg.MaxConnIdleTime
	}
	minConns := 0
	if cfg.MinConns > 0 {
		minConns = cfg.MinConns
	}

	connsPerURL := distribute(cfg.MaxTotalConnections, len(urls))
	maxConnsPerPool := cfg.MaxConnsPerPool
	if maxConnsPerPool == 0 {
		maxConnsPerPool = cfg.MaxTotalConnections
	}

	// See
	// https://github.com/jackc/pgx/blob/fa5fbed497bc75acee05c1667a8760ce0d634cba/conn.go#L578-L612
	// for details on the specifics of each query mode.
	queryMode, ok := stringToMethod[strings.ToLower(cfg.Method)]
	if !ok {
		return nil, errors.Errorf("unknown method %s", cfg.Method)
	}
	m.method = queryMode

	for i := range urls {
		connsPerPool := distributeMax(connsPerURL[i], maxConnsPerPool)
		for _, numConns := range connsPerPool {
			poolCfg, err := pgxpool.ParseConfig(urls[i])
			if err != nil {
				return nil, err
			}
			poolCfg.HealthCheckPeriod = connHealthCheckPeriod
			poolCfg.MaxConnLifetime = maxConnLifetime
			poolCfg.MaxConnLifetimeJitter = maxConnLifetimeJitter
			poolCfg.MaxConnIdleTime = maxConnIdleTime
			poolCfg.MaxConns = int32(numConns)
			if minConns > numConns {
				minConns = numConns
			}
			poolCfg.MinConns = int32(minConns)
			poolCfg.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
				m.mu.RLock()
				defer m.mu.RUnlock()
				for name, sql := range m.mu.preparedStatements {
					// Note that calling `Prepare` with a name that has already been
					// prepared is idempotent and short-circuits before doing any
					// communication to the server.
					if _, err := conn.Prepare(ctx, name, sql); err != nil {
						log.Warningf(ctx, "error preparing statement. name=%s sql=%s %v", name, sql, err)
						return false
					}
				}
				return true
			}

			connCfg := poolCfg.ConnConfig
			connCfg.DefaultQueryExecMode = queryMode
			p, err := pgxpool.NewWithConfig(ctx, poolCfg)
			if err != nil {
				return nil, err
			}

			m.Pools = append(m.Pools, p)
		}
	}

	if err := m.WarmupConns(ctx, cfg.WarmupConns); err != nil {
		log.Warningf(ctx, "warming up connection pool failed (%v), continuing workload", err)
	}

	return m, nil
}

// AddPreparedStatement adds the given sql statement to the map of
// statements that will be prepared when a new connection is retrieved
// from the pool.
func (m *MultiConnPool) AddPreparedStatement(name string, statement string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.preparedStatements[name] = statement
}

// Get returns one of the pools, in round-robin manner.
func (m *MultiConnPool) Get() *pgxpool.Pool {
	numPools := uint32(len(m.Pools))
	if numPools == 1 {
		return m.Pools[0]
	}
	i := atomic.AddUint32(&m.counter, 1) - 1

	return m.Pools[i%numPools]
}

// Close closes all the pools.
func (m *MultiConnPool) Close() {
	for _, p := range m.Pools {
		p.Close()
	}
}

// Method returns the query execution mode of the connection pool.
func (m *MultiConnPool) Method() pgx.QueryExecMode {
	return m.method
}

// WarmupConns warms up totalNumConns connections distributed across all pools
// contained within MultiConnPool.  The max number of connections are warmed up
// if totalNumConns is set to 0.  If totalNumConns is less than 0, no
// pre-warming of connections is performed.
func (m *MultiConnPool) WarmupConns(ctx context.Context, totalNumConns int) error {
	if totalNumConns < 0 {
		return nil
	}

	// "Warm up" the pools so we don't have to establish connections later (which
	// would affect the observed latencies of the first requests, especially when
	// prepared statements are used). We do this by
	// acquiring connections (in parallel), then releasing them back to the
	// pool.
	var g errgroup.Group

	// Limit concurrent connection establishment. Allowing this to run
	// at maximum parallelism would trigger syn flood protection on the
	// host, which combined with any packet loss could cause Acquire to
	// return an error and fail the whole function. The value 100 is
	// chosen because it is less than the default value for SOMAXCONN
	// (128).
	g.SetLimit(100)

	type warmupPool struct {
		maxConns int
		pool     *pgxpool.Pool
	}

	warmupPools := make([]warmupPool, len(m.Pools))
	var numWarmupConns int
	numConnsPerPool := distribute(totalNumConns, len(m.Pools))
	for i, p := range m.Pools {
		poolMaxConns := int(p.Config().MaxConns)

		// Tune max conns for the pool
		switch {
		case totalNumConns == 0 && poolMaxConns > 0:
			warmupPools[i].maxConns = poolMaxConns
		case totalNumConns == 0:
			warmupPools[i].maxConns = 1 // always at least one connection
		default:
			warmupPools[i].maxConns = numConnsPerPool[i]
		}

		// Clamp max conns per pool
		if warmupPools[i].maxConns > poolMaxConns {
			warmupPools[i].maxConns = poolMaxConns
		}

		warmupPools[i].pool = p
		numWarmupConns += warmupPools[i].maxConns
	}

	// NOTE(seanc@): see context cancellation note below.
	// TODO(seanc@): Change WithTimeout() back to WithCancel()
	const maxWarmupTime = 5 * time.Minute // NOTE(seanc@): 5min == AWS NLB TCP idle time
	const minWarmupTime = 15 * time.Second
	const maxTimePerConn = 200 * time.Millisecond
	warmupTime := minWarmupTime
	if int(warmupTime) < numWarmupConns*int(maxTimePerConn) {
		warmupTime = time.Duration(numWarmupConns * int(maxTimePerConn))
	}
	if warmupTime > maxWarmupTime {
		warmupTime = maxWarmupTime
	}
	ctx, cancel := context.WithTimeout(ctx, warmupTime)
	defer cancel()

	warmupConns := make(chan *pgxpool.Conn, numWarmupConns)
	for _, p := range warmupPools {
		p := p
		for i := 0; i < p.maxConns; i++ {
			g.Go(func() error {
				conn, err := p.pool.Acquire(ctx)
				if err != nil {
					return err
				}
				warmupConns <- conn
				return nil
			})
		}
	}

	estConns := make([]*pgxpool.Conn, 0, numWarmupConns)
	defer func() {
		for _, conn := range estConns {
			conn.Release()
		}
	}()

	for i := 0; i < numWarmupConns; i++ {
		select {
		case conn := <-warmupConns:
			estConns = append(estConns, conn)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// distribute returns a slice of <num> integers that add up to <total> and are
// within +/-1 of each other.
func distribute(total, num int) []int {
	res := make([]int, num)
	for i := range res {
		// Use the average number of remaining connections.
		div := len(res) - i
		res[i] = (total + div/2) / div
		total -= res[i]
	}
	return res
}

// distributeMax returns a slice of integers that are at most `max` and add up
// to <total>. The slice is as short as possible and the values are within +/-1
// of each other.
func distributeMax(total, max int) []int {
	return distribute(total, (total+max-1)/max)
}
