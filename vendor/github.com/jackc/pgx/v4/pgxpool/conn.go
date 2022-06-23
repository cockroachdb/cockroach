package pgxpool

import (
	"context"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/puddle"
)

// Conn is an acquired *pgx.Conn from a Pool.
type Conn struct {
	res *puddle.Resource
	p   *Pool
}

// Release returns c to the pool it was acquired from. Once Release has been called, other methods must not be called.
// However, it is safe to call Release multiple times. Subsequent calls after the first will be ignored.
func (c *Conn) Release() {
	if c.res == nil {
		return
	}

	conn := c.Conn()
	res := c.res
	c.res = nil

	now := time.Now()
	if conn.IsClosed() || conn.PgConn().IsBusy() || conn.PgConn().TxStatus() != 'I' || (now.Sub(res.CreationTime()) > c.p.maxConnLifetime) {
		res.Destroy()
		return
	}

	if c.p.afterRelease == nil {
		res.Release()
		return
	}

	go func() {
		if c.p.afterRelease(conn) {
			res.Release()
		} else {
			res.Destroy()
		}
	}()
}

func (c *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return c.Conn().Exec(ctx, sql, arguments...)
}

func (c *Conn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return c.Conn().Query(ctx, sql, args...)
}

func (c *Conn) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return c.Conn().QueryRow(ctx, sql, args...)
}

func (c *Conn) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	return c.Conn().QueryFunc(ctx, sql, args, scans, f)
}

func (c *Conn) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return c.Conn().SendBatch(ctx, b)
}

func (c *Conn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return c.Conn().CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// Begin starts a transaction block from the *Conn without explicitly setting a transaction mode (see BeginTx with TxOptions if transaction mode is required).
func (c *Conn) Begin(ctx context.Context) (pgx.Tx, error) {
	return c.Conn().Begin(ctx)
}

// BeginTx starts a transaction block from the *Conn with txOptions determining the transaction mode.
func (c *Conn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return c.Conn().BeginTx(ctx, txOptions)
}

func (c *Conn) BeginFunc(ctx context.Context, f func(pgx.Tx) error) error {
	return c.Conn().BeginFunc(ctx, f)
}

func (c *Conn) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) error {
	return c.Conn().BeginTxFunc(ctx, txOptions, f)
}

func (c *Conn) Ping(ctx context.Context) error {
	return c.Conn().Ping(ctx)
}

func (c *Conn) Conn() *pgx.Conn {
	return c.connResource().conn
}

func (c *Conn) connResource() *connResource {
	return c.res.Value().(*connResource)
}

func (c *Conn) getPoolRow(r pgx.Row) *poolRow {
	return c.connResource().getPoolRow(c, r)
}

func (c *Conn) getPoolRows(r pgx.Rows) *poolRows {
	return c.connResource().getPoolRows(c, r)
}
