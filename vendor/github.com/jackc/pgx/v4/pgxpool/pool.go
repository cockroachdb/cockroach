package pgxpool

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/puddle"
)

var defaultMaxConns = int32(4)
var defaultMinConns = int32(0)
var defaultMaxConnLifetime = time.Hour
var defaultMaxConnIdleTime = time.Minute * 30
var defaultHealthCheckPeriod = time.Minute

type connResource struct {
	conn      *pgx.Conn
	conns     []Conn
	poolRows  []poolRow
	poolRowss []poolRows
}

func (cr *connResource) getConn(p *Pool, res *puddle.Resource) *Conn {
	if len(cr.conns) == 0 {
		cr.conns = make([]Conn, 128)
	}

	c := &cr.conns[len(cr.conns)-1]
	cr.conns = cr.conns[0 : len(cr.conns)-1]

	c.res = res
	c.p = p

	return c
}

func (cr *connResource) getPoolRow(c *Conn, r pgx.Row) *poolRow {
	if len(cr.poolRows) == 0 {
		cr.poolRows = make([]poolRow, 128)
	}

	pr := &cr.poolRows[len(cr.poolRows)-1]
	cr.poolRows = cr.poolRows[0 : len(cr.poolRows)-1]

	pr.c = c
	pr.r = r

	return pr
}

func (cr *connResource) getPoolRows(c *Conn, r pgx.Rows) *poolRows {
	if len(cr.poolRowss) == 0 {
		cr.poolRowss = make([]poolRows, 128)
	}

	pr := &cr.poolRowss[len(cr.poolRowss)-1]
	cr.poolRowss = cr.poolRowss[0 : len(cr.poolRowss)-1]

	pr.c = c
	pr.r = r

	return pr
}

// Pool allows for connection reuse.
type Pool struct {
	p                 *puddle.Pool
	config            *Config
	beforeConnect     func(context.Context, *pgx.ConnConfig) error
	afterConnect      func(context.Context, *pgx.Conn) error
	beforeAcquire     func(context.Context, *pgx.Conn) bool
	afterRelease      func(*pgx.Conn) bool
	minConns          int32
	maxConnLifetime   time.Duration
	maxConnIdleTime   time.Duration
	healthCheckPeriod time.Duration

	closeOnce sync.Once
	closeChan chan struct{}
}

// Config is the configuration struct for creating a pool. It must be created by ParseConfig and then it can be
// modified. A manually initialized ConnConfig will cause ConnectConfig to panic.
type Config struct {
	ConnConfig *pgx.ConnConfig

	// BeforeConnect is called before a new connection is made. It is passed a copy of the underlying pgx.ConnConfig and
	// will not impact any existing open connections.
	BeforeConnect func(context.Context, *pgx.ConnConfig) error

	// AfterConnect is called after a connection is established, but before it is added to the pool.
	AfterConnect func(context.Context, *pgx.Conn) error

	// BeforeAcquire is called before a connection is acquired from the pool. It must return true to allow the
	// acquision or false to indicate that the connection should be destroyed and a different connection should be
	// acquired.
	BeforeAcquire func(context.Context, *pgx.Conn) bool

	// AfterRelease is called after a connection is released, but before it is returned to the pool. It must return true to
	// return the connection to the pool or false to destroy the connection.
	AfterRelease func(*pgx.Conn) bool

	// MaxConnLifetime is the duration since creation after which a connection will be automatically closed.
	MaxConnLifetime time.Duration

	// MaxConnIdleTime is the duration after which an idle connection will be automatically closed by the health check.
	MaxConnIdleTime time.Duration

	// MaxConns is the maximum size of the pool. The default is the greater of 4 or runtime.NumCPU().
	MaxConns int32

	// MinConns is the minimum size of the pool. The health check will increase the number of connections to this
	// amount if it had dropped below.
	MinConns int32

	// HealthCheckPeriod is the duration between checks of the health of idle connections.
	HealthCheckPeriod time.Duration

	// If set to true, pool doesn't do any I/O operation on initialization.
	// And connects to the server only when the pool starts to be used.
	// The default is false.
	LazyConnect bool

	createdByParseConfig bool // Used to enforce created by ParseConfig rule.
}

// Copy returns a deep copy of the config that is safe to use and modify.
// The only exception is the tls.Config:
// according to the tls.Config docs it must not be modified after creation.
func (c *Config) Copy() *Config {
	newConfig := new(Config)
	*newConfig = *c
	newConfig.ConnConfig = c.ConnConfig.Copy()
	return newConfig
}

// ConnString returns the connection string as parsed by pgxpool.ParseConfig into pgxpool.Config.
func (c *Config) ConnString() string { return c.ConnConfig.ConnString() }

// Connect creates a new Pool and immediately establishes one connection. ctx can be used to cancel this initial
// connection. See ParseConfig for information on connString format.
func Connect(ctx context.Context, connString string) (*Pool, error) {
	config, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return ConnectConfig(ctx, config)
}

// ConnectConfig creates a new Pool and immediately establishes one connection. ctx can be used to cancel this initial
// connection. config must have been created by ParseConfig.
func ConnectConfig(ctx context.Context, config *Config) (*Pool, error) {
	// Default values are set in ParseConfig. Enforce initial creation by ParseConfig rather than setting defaults from
	// zero values.
	if !config.createdByParseConfig {
		panic("config must be created by ParseConfig")
	}

	p := &Pool{
		config:            config,
		beforeConnect:     config.BeforeConnect,
		afterConnect:      config.AfterConnect,
		beforeAcquire:     config.BeforeAcquire,
		afterRelease:      config.AfterRelease,
		minConns:          config.MinConns,
		maxConnLifetime:   config.MaxConnLifetime,
		maxConnIdleTime:   config.MaxConnIdleTime,
		healthCheckPeriod: config.HealthCheckPeriod,
		closeChan:         make(chan struct{}),
	}

	p.p = puddle.NewPool(
		func(ctx context.Context) (interface{}, error) {
			connConfig := p.config.ConnConfig

			if p.beforeConnect != nil {
				connConfig = p.config.ConnConfig.Copy()
				if err := p.beforeConnect(ctx, connConfig); err != nil {
					return nil, err
				}
			}

			conn, err := pgx.ConnectConfig(ctx, connConfig)
			if err != nil {
				return nil, err
			}

			if p.afterConnect != nil {
				err = p.afterConnect(ctx, conn)
				if err != nil {
					conn.Close(ctx)
					return nil, err
				}
			}

			cr := &connResource{
				conn:      conn,
				conns:     make([]Conn, 64),
				poolRows:  make([]poolRow, 64),
				poolRowss: make([]poolRows, 64),
			}

			return cr, nil
		},
		func(value interface{}) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			conn := value.(*connResource).conn
			conn.Close(ctx)
			select {
			case <-conn.PgConn().CleanupDone():
			case <-ctx.Done():
			}
			cancel()
		},
		config.MaxConns,
	)

	if !config.LazyConnect {
		if err := p.createIdleResources(ctx, int(p.minConns)); err != nil {
			// Couldn't create resources for minpool size. Close unhealthy pool.
			p.Close()
			return nil, err
		}

		// Initially establish one connection
		res, err := p.p.Acquire(ctx)
		if err != nil {
			p.Close()
			return nil, err
		}
		res.Release()
	}

	go p.backgroundHealthCheck()

	return p, nil
}

// ParseConfig builds a Config from connString. It parses connString with the same behavior as pgx.ParseConfig with the
// addition of the following variables:
//
// pool_max_conns: integer greater than 0
// pool_min_conns: integer 0 or greater
// pool_max_conn_lifetime: duration string
// pool_max_conn_idle_time: duration string
// pool_health_check_period: duration string
//
// See Config for definitions of these arguments.
//
//   # Example DSN
//   user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca pool_max_conns=10
//
//   # Example URL
//   postgres://jack:secret@pg.example.com:5432/mydb?sslmode=verify-ca&pool_max_conns=10
func ParseConfig(connString string) (*Config, error) {
	connConfig, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	config := &Config{
		ConnConfig:           connConfig,
		createdByParseConfig: true,
	}

	if s, ok := config.ConnConfig.Config.RuntimeParams["pool_max_conns"]; ok {
		delete(connConfig.Config.RuntimeParams, "pool_max_conns")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot parse pool_max_conns: %w", err)
		}
		if n < 1 {
			return nil, fmt.Errorf("pool_max_conns too small: %d", n)
		}
		config.MaxConns = int32(n)
	} else {
		config.MaxConns = defaultMaxConns
		if numCPU := int32(runtime.NumCPU()); numCPU > config.MaxConns {
			config.MaxConns = numCPU
		}
	}

	if s, ok := config.ConnConfig.Config.RuntimeParams["pool_min_conns"]; ok {
		delete(connConfig.Config.RuntimeParams, "pool_min_conns")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot parse pool_min_conns: %w", err)
		}
		config.MinConns = int32(n)
	} else {
		config.MinConns = defaultMinConns
	}

	if s, ok := config.ConnConfig.Config.RuntimeParams["pool_max_conn_lifetime"]; ok {
		delete(connConfig.Config.RuntimeParams, "pool_max_conn_lifetime")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid pool_max_conn_lifetime: %w", err)
		}
		config.MaxConnLifetime = d
	} else {
		config.MaxConnLifetime = defaultMaxConnLifetime
	}

	if s, ok := config.ConnConfig.Config.RuntimeParams["pool_max_conn_idle_time"]; ok {
		delete(connConfig.Config.RuntimeParams, "pool_max_conn_idle_time")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid pool_max_conn_idle_time: %w", err)
		}
		config.MaxConnIdleTime = d
	} else {
		config.MaxConnIdleTime = defaultMaxConnIdleTime
	}

	if s, ok := config.ConnConfig.Config.RuntimeParams["pool_health_check_period"]; ok {
		delete(connConfig.Config.RuntimeParams, "pool_health_check_period")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid pool_health_check_period: %w", err)
		}
		config.HealthCheckPeriod = d
	} else {
		config.HealthCheckPeriod = defaultHealthCheckPeriod
	}

	return config, nil
}

// Close closes all connections in the pool and rejects future Acquire calls. Blocks until all connections are returned
// to pool and closed.
func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		close(p.closeChan)
		p.p.Close()
	})
}

func (p *Pool) backgroundHealthCheck() {
	ticker := time.NewTicker(p.healthCheckPeriod)

	for {
		select {
		case <-p.closeChan:
			ticker.Stop()
			return
		case <-ticker.C:
			p.checkIdleConnsHealth()
			p.checkMinConns()
		}
	}
}

func (p *Pool) checkIdleConnsHealth() {
	resources := p.p.AcquireAllIdle()

	now := time.Now()
	for _, res := range resources {
		if now.Sub(res.CreationTime()) > p.maxConnLifetime {
			res.Destroy()
		} else if res.IdleDuration() > p.maxConnIdleTime {
			res.Destroy()
		} else {
			res.ReleaseUnused()
		}
	}
}

func (p *Pool) checkMinConns() {
	for i := p.minConns - p.Stat().TotalConns(); i > 0; i-- {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			p.p.CreateResource(ctx)
		}()
	}
}

func (p *Pool) createIdleResources(parentCtx context.Context, targetResources int) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	errs := make(chan error, targetResources)

	for i := 0; i < targetResources; i++ {
		go func() {
			err := p.p.CreateResource(ctx)
			errs <- err
		}()
	}

	var firstError error
	for i := 0; i < targetResources; i++ {
		err := <-errs
		if err != nil && firstError == nil {
			cancel()
			firstError = err
		}
	}

	return firstError
}

// Acquire returns a connection (*Conn) from the Pool
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	for {
		res, err := p.p.Acquire(ctx)
		if err != nil {
			return nil, err
		}

		cr := res.Value().(*connResource)
		if p.beforeAcquire == nil || p.beforeAcquire(ctx, cr.conn) {
			return cr.getConn(p, res), nil
		}

		res.Destroy()
	}
}

// AcquireFunc acquires a *Conn and calls f with that *Conn. ctx will only affect the Acquire. It has no effect on the
// call of f. The return value is either an error acquiring the *Conn or the return value of f. The *Conn is
// automatically released after the call of f.
func (p *Pool) AcquireFunc(ctx context.Context, f func(*Conn) error) error {
	conn, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	return f(conn)
}

// AcquireAllIdle atomically acquires all currently idle connections. Its intended use is for health check and
// keep-alive functionality. It does not update pool statistics.
func (p *Pool) AcquireAllIdle(ctx context.Context) []*Conn {
	resources := p.p.AcquireAllIdle()
	conns := make([]*Conn, 0, len(resources))
	for _, res := range resources {
		cr := res.Value().(*connResource)
		if p.beforeAcquire == nil || p.beforeAcquire(ctx, cr.conn) {
			conns = append(conns, cr.getConn(p, res))
		} else {
			res.Destroy()
		}
	}

	return conns
}

// Config returns a copy of config that was used to initialize this pool.
func (p *Pool) Config() *Config { return p.config.Copy() }

// Stat returns a pgxpool.Stat struct with a snapshot of Pool statistics.
func (p *Pool) Stat() *Stat {
	return &Stat{s: p.p.Stat()}
}

// Exec acquires a connection from the Pool and executes the given SQL.
// SQL can be either a prepared statement name or an SQL string.
// Arguments should be referenced positionally from the SQL string as $1, $2, etc.
// The acquired connection is returned to the pool when the Exec function returns.
func (p *Pool) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Release()

	return c.Exec(ctx, sql, arguments...)
}

// Query acquires a connection and executes a query that returns pgx.Rows.
// Arguments should be referenced positionally from the SQL string as $1, $2, etc.
// See pgx.Rows documentation to close the returned Rows and return the acquired connection to the Pool.
//
// If there is an error, the returned pgx.Rows will be returned in an error state.
// If preferred, ignore the error returned from Query and handle errors using the returned pgx.Rows.
//
// For extra control over how the query is executed, the types QuerySimpleProtocol, QueryResultFormats, and
// QueryResultFormatsByOID may be used as the first args to control exactly how the query is executed. This is rarely
// needed. See the documentation for those types for details.
func (p *Pool) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return errRows{err: err}, err
	}

	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		c.Release()
		return errRows{err: err}, err
	}

	return c.getPoolRows(rows), nil
}

// QueryRow acquires a connection and executes a query that is expected
// to return at most one row (pgx.Row). Errors are deferred until pgx.Row's
// Scan method is called. If the query selects no rows, pgx.Row's Scan will
// return ErrNoRows. Otherwise, pgx.Row's Scan scans the first selected row
// and discards the rest. The acquired connection is returned to the Pool when
// pgx.Row's Scan method is called.
//
// Arguments should be referenced positionally from the SQL string as $1, $2, etc.
//
// For extra control over how the query is executed, the types QuerySimpleProtocol, QueryResultFormats, and
// QueryResultFormatsByOID may be used as the first args to control exactly how the query is executed. This is rarely
// needed. See the documentation for those types for details.
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	c, err := p.Acquire(ctx)
	if err != nil {
		return errRow{err: err}
	}

	row := c.QueryRow(ctx, sql, args...)
	return c.getPoolRow(row)
}

func (p *Pool) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Release()

	return c.QueryFunc(ctx, sql, args, scans, f)
}

func (p *Pool) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	c, err := p.Acquire(ctx)
	if err != nil {
		return errBatchResults{err: err}
	}

	br := c.SendBatch(ctx, b)
	return &poolBatchResults{br: br, c: c}
}

// Begin acquires a connection from the Pool and starts a transaction. Unlike database/sql, the context only affects the begin command. i.e. there is no
// auto-rollback on context cancellation. Begin initiates a transaction block without explicitly setting a transaction mode for the block (see BeginTx with TxOptions if transaction mode is required).
// *pgxpool.Tx is returned, which implements the pgx.Tx interface.
// Commit or Rollback must be called on the returned transaction to finalize the transaction block.
func (p *Pool) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.BeginTx(ctx, pgx.TxOptions{})
}

// BeginTx acquires a connection from the Pool and starts a transaction with pgx.TxOptions determining the transaction mode.
// Unlike database/sql, the context only affects the begin command. i.e. there is no auto-rollback on context cancellation.
// *pgxpool.Tx is returned, which implements the pgx.Tx interface.
// Commit or Rollback must be called on the returned transaction to finalize the transaction block.
func (p *Pool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	t, err := c.BeginTx(ctx, txOptions)
	if err != nil {
		c.Release()
		return nil, err
	}

	return &Tx{t: t, c: c}, err
}

func (p *Pool) BeginFunc(ctx context.Context, f func(pgx.Tx) error) error {
	return p.BeginTxFunc(ctx, pgx.TxOptions{}, f)
}

func (p *Pool) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.Release()

	return c.BeginTxFunc(ctx, txOptions, f)
}

func (p *Pool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Release()

	return c.Conn().CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// Ping acquires a connection from the Pool and executes an empty sql statement against it.
// If the sql returns without error, the database Ping is considered successful, otherwise, the error is returned.
func (p *Pool) Ping(ctx context.Context) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.Release()
	return c.Ping(ctx)
}
