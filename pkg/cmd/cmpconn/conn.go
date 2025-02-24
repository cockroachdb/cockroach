// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cmpconn assists in comparing results from DB connections.
package cmpconn

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
)

// Conn holds gosql and pgx connections and provides some utility methods.
type Conn interface {
	// DB returns gosql connection.
	DB() *gosql.DB
	// PGX returns pgx connection.
	PGX() *pgx.Conn
	// Values executes prep and exec and returns the results of exec.
	Values(ctx context.Context, prep, exec string) (rows pgx.Rows, err error)
	// Exec executes s.
	Exec(ctx context.Context, s string) error
	// Ping pings a connection.
	Ping(ctx context.Context) error
	// Close closes the connections.
	Close(ctx context.Context)
}

type conn struct {
	db  *gosql.DB
	pgx *pgx.Conn
}

var _ Conn = &conn{}

// DB is part of the Conn interface.
func (c *conn) DB() *gosql.DB {
	return c.db
}

// PGX is part of the Conn interface.
func (c *conn) PGX() *pgx.Conn {
	return c.pgx
}

// Values executes prep and exec and returns the results of exec.
func (c *conn) Values(ctx context.Context, prep, exec string) (rows pgx.Rows, err error) {
	if prep != "" {
		rows, err = c.pgx.Query(ctx, prep, pgx.QuerySimpleProtocol(true))
		if err != nil {
			return nil, err
		}
		rows.Close()
	}
	return c.pgx.Query(ctx, exec, pgx.QuerySimpleProtocol(true))
}

// Exec executes s.
func (c *conn) Exec(ctx context.Context, s string) error {
	_, err := c.pgx.Exec(ctx, s, pgx.QuerySimpleProtocol(true))
	return errors.Wrap(err, "exec")
}

// Ping pings a connection.
func (c *conn) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return c.pgx.Ping(ctx)
}

// Close closes the connections.
func (c *conn) Close(ctx context.Context) {
	_ = c.db.Close()
	_ = c.pgx.Close(ctx)
}

// NewConn returns a new Conn on the given uri and executes initSQL on it.
func NewConn(ctx context.Context, uri string, initSQL ...string) (Conn, error) {
	c := conn{}

	{
		connector, err := pq.NewConnector(uri)
		if err != nil {
			return nil, errors.Wrap(err, "pq conn")
		}
		c.db = gosql.OpenDB(connector)
	}

	{
		config, err := pgx.ParseConfig(uri)
		if err != nil {
			return nil, errors.Wrap(err, "pgx parse")
		}
		conn, err := pgx.ConnectConfig(ctx, config)
		if err != nil {
			return nil, errors.Wrap(err, "pgx conn")
		}
		c.pgx = conn
	}

	for _, s := range initSQL {
		if s == "" {
			continue
		}

		if _, err := c.pgx.Exec(ctx, s); err != nil {
			return nil, errors.Wrap(err, "init SQL")
		}
	}

	return &c, nil
}

// connWithMutators extends Conn by supporting application of mutations to
// queries before their execution.
type connWithMutators struct {
	Conn
	rng         *rand.Rand
	sqlMutators []randgen.Mutator
}

var _ Conn = &connWithMutators{}

// NewConnWithMutators returns a new Conn on the given uri and executes initSQL
// on it. The mutators are applied to initSQL and will be applied to all
// queries to be executed in CompareConns.
func NewConnWithMutators(
	ctx context.Context, uri string, rng *rand.Rand, sqlMutators []randgen.Mutator, initSQL ...string,
) (Conn, error) {
	mutatedInitSQL := make([]string, len(initSQL))
	for i, s := range initSQL {
		mutatedInitSQL[i] = s
		if s == "" {
			continue
		}

		mutatedInitSQL[i], _ = randgen.ApplyString(rng, s, sqlMutators...)
	}
	conn, err := NewConn(ctx, uri, mutatedInitSQL...)
	if err != nil {
		return nil, err
	}
	return &connWithMutators{
		Conn:        conn,
		rng:         rng,
		sqlMutators: sqlMutators,
	}, nil
}

// CompareConns executes prep and exec on all connections in conns. If any
// differ, an error is returned. ignoreSQLErrors determines whether SQL errors
// are ignored.
// NOTE: exec will be mutated for each connection of type connWithMutators.
func CompareConns(
	ctx context.Context,
	timeout time.Duration,
	conns map[string]Conn,
	prep, exec string,
	ignoreSQLErrors bool,
) (ignoredErr bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	connRows := make(map[string]pgx.Rows)
	connExecs := make(map[string]string)
	for name, conn := range conns {
		connExecs[name] = exec
		if cwm, withMutators := conn.(*connWithMutators); withMutators {
			connExecs[name], _ = randgen.ApplyString(cwm.rng, exec, cwm.sqlMutators...)
		}
		rows, err := conn.Values(ctx, prep, connExecs[name])
		if err != nil {
			return true, nil //nolint:returnerrcheck
		}
		//nolint:deferloop TODO(#137605)
		defer rows.Close()
		connRows[name] = rows
	}

	// Annotate our error message with the exec queries since they can be
	// mutated and differ per connection.
	defer func() {
		if err == nil {
			return
		}
		var sb strings.Builder
		prev := ""
		for name, mutated := range connExecs {
			fmt.Fprintf(&sb, "\n%s:", name)
			if prev == mutated {
				sb.WriteString(" [same as previous]\n")
			} else {
				fmt.Fprintf(&sb, "\n%s;\n", mutated)
			}
			prev = mutated
		}
		err = fmt.Errorf("%w%s", err, sb.String())
	}()

	return compareRows(connRows, ignoreSQLErrors)
}

// compareRows compares the results of executing of queries on all connections.
// It always returns an error if there are any differences. Additionally,
// ignoreSQLErrors specifies whether SQL errors should be ignored (in which
// case the function returns nil if SQL error occurs).
func compareRows(
	connRows map[string]pgx.Rows, ignoreSQLErrors bool,
) (ignoredErr bool, retErr error) {
	var first []interface{}
	var firstName string
	var minCount int
	rowCounts := make(map[string]int)
ReadRows:
	for {
		first = nil
		firstName = ""
		for name, rows := range connRows {
			if !rows.Next() {
				minCount = rowCounts[name]
				break ReadRows
			}
			rowCounts[name]++
			vals, err := rows.Values()
			if err != nil {
				if ignoreSQLErrors {
					// This function can fail if, for example,
					// a number doesn't fit into a float64. Ignore
					// them and move along to another query.
					return true, nil
				}
				return false, err
			}
			if firstName == "" {
				firstName = name
				first = vals
			} else {
				if err := CompareVals(first, vals); err != nil {
					return false, errors.Wrapf(err, "compare %s to %s", firstName, name)
				}
			}
		}
	}
	// Make sure all are empty.
	for name, rows := range connRows {
		for rows.Next() {
			rowCounts[name]++
		}
		if err := rows.Err(); err != nil {
			if ignoreSQLErrors {
				// Aww someone had a SQL error maybe, so we can't use this
				// query.
				return true, nil
			}
			return false, err
		}
	}
	// Ensure each connection returned the same number of rows.
	for name, count := range rowCounts {
		if minCount != count {
			return false, fmt.Errorf("%s had %d rows, expected %d", name, count, minCount)
		}
	}
	return false, nil
}
