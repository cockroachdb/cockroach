// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/jackc/pgx"
	"github.com/lib/pq"
)

// Conn holds gosql and pgx connections and provides some utility methods.
type Conn interface {
	// DB returns gosql connection.
	DB() *gosql.DB
	// PGX returns pgx connection.
	PGX() *pgx.Conn
	// Values executes prep and exec and returns the results of exec.
	Values(ctx context.Context, prep, exec string) (rows *pgx.Rows, err error)
	// Exec executes s.
	Exec(ctx context.Context, s string) error
	// Ping pings a connection.
	Ping() error
	// Close closes the connections.
	Close()
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

var simpleProtocol = &pgx.QueryExOptions{SimpleProtocol: true}

// Values executes prep and exec and returns the results of exec.
func (c *conn) Values(ctx context.Context, prep, exec string) (rows *pgx.Rows, err error) {
	if prep != "" {
		rows, err = c.pgx.QueryEx(ctx, prep, simpleProtocol)
		if err != nil {
			return nil, err
		}
		rows.Close()
	}
	return c.pgx.QueryEx(ctx, exec, simpleProtocol)
}

// Exec executes s.
func (c *conn) Exec(ctx context.Context, s string) error {
	_, err := c.pgx.ExecEx(ctx, s, simpleProtocol)
	return errors.Wrap(err, "exec")
}

// Ping pings a connection.
func (c *conn) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return c.pgx.Ping(ctx)
}

// Close closes the connections.
func (c *conn) Close() {
	_ = c.db.Close()
	_ = c.pgx.Close()
}

// NewConn returns a new Conn on the given uri and executes initSQL on it.
func NewConn(uri string, initSQL ...string) (Conn, error) {
	c := conn{}

	{
		connector, err := pq.NewConnector(uri)
		if err != nil {
			return nil, errors.Wrap(err, "pq conn")
		}
		c.db = gosql.OpenDB(connector)
	}

	{
		config, err := pgx.ParseURI(uri)
		if err != nil {
			return nil, errors.Wrap(err, "pgx parse")
		}
		conn, err := pgx.Connect(config)
		if err != nil {
			return nil, errors.Wrap(err, "pgx conn")
		}
		c.pgx = conn
	}

	for _, s := range initSQL {
		if s == "" {
			continue
		}

		if _, err := c.pgx.Exec(s); err != nil {
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
	uri string, rng *rand.Rand, sqlMutators []randgen.Mutator, initSQL ...string,
) (Conn, error) {
	mutatedInitSQL := make([]string, len(initSQL))
	for i, s := range initSQL {
		mutatedInitSQL[i] = s
		if s == "" {
			continue
		}

		mutatedInitSQL[i], _ = randgen.ApplyString(rng, s, sqlMutators...)
	}
	conn, err := NewConn(uri, mutatedInitSQL...)
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
) (err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	connRows := make(map[string]*pgx.Rows)
	connExecs := make(map[string]string)
	for name, conn := range conns {
		connExecs[name] = exec
		if cwm, withMutators := conn.(*connWithMutators); withMutators {
			connExecs[name], _ = randgen.ApplyString(cwm.rng, exec, cwm.sqlMutators...)
		}
		rows, err := conn.Values(ctx, prep, connExecs[name])
		if err != nil {
			return nil //nolint:returnerrcheck
		}
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
func compareRows(connRows map[string]*pgx.Rows, ignoreSQLErrors bool) error {
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
					err = nil
				}
				return err
			}
			if firstName == "" {
				firstName = name
				first = vals
			} else {
				if err := CompareVals(first, vals); err != nil {
					return fmt.Errorf("compare %s to %s:\n%v", firstName, name, err)
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
				err = nil
			}
			return err
		}
	}
	// Ensure each connection returned the same number of rows.
	for name, count := range rowCounts {
		if minCount != count {
			return fmt.Errorf("%s had %d rows, expected %d", name, count, minCount)
		}
	}
	return nil
}
