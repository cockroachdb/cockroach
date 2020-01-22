// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package cmpconn assits in comparing results from DB connections.
package cmpconn

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/mutations"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/jackc/pgx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// Conn holds gosql and pgx connections.
type Conn struct {
	DB          *gosql.DB
	PGX         *pgx.Conn
	rng         *rand.Rand
	sqlMutators []sqlbase.Mutator
}

// NewConn returns a new Conn on the given uri and executes initSQL on it. The
// mutators are applied to initSQL.
func NewConn(
	uri string, rng *rand.Rand, sqlMutators []sqlbase.Mutator, initSQL ...string,
) (*Conn, error) {
	c := Conn{
		rng:         rng,
		sqlMutators: sqlMutators,
	}

	{
		connector, err := pq.NewConnector(uri)
		if err != nil {
			return nil, errors.Wrap(err, "pq conn")
		}
		db := gosql.OpenDB(connector)
		c.DB = db
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
		c.PGX = conn
	}

	for _, s := range initSQL {
		if s == "" {
			continue
		}

		s, _ = mutations.ApplyString(rng, s, sqlMutators...)
		if _, err := c.PGX.Exec(s); err != nil {
			return nil, errors.Wrap(err, "init SQL")
		}
	}

	return &c, nil
}

// Close closes the connections.
func (c *Conn) Close() {
	_ = c.DB.Close()
	_ = c.PGX.Close()
}

// Ping pings a connection.
func (c *Conn) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return c.PGX.Ping(ctx)
}

// Exec executes s.
func (c *Conn) Exec(ctx context.Context, s string) error {
	_, err := c.PGX.ExecEx(ctx, s, simpleProtocol)
	return errors.Wrap(err, "exec")
}

// Values executes prep and exec and returns the results of exec. Mutators
// passed in during NewConn are applied only to exec.
func (c *Conn) Values(ctx context.Context, prep, exec string) (*pgx.Rows, error) {
	if prep != "" {
		rows, err := c.PGX.QueryEx(ctx, prep, simpleProtocol)
		if err != nil {
			return nil, err
		}
		rows.Close()
	}
	exec, _ = mutations.ApplyString(c.rng, exec, c.sqlMutators...)
	return c.PGX.QueryEx(ctx, exec, simpleProtocol)
}

var simpleProtocol = &pgx.QueryExOptions{SimpleProtocol: true}

// CompareConns executes prep and exec on all connections in conns. If any
// differ, an error is returned. SQL errors are ignored.
func CompareConns(
	ctx context.Context, timeout time.Duration, conns map[string]*Conn, prep, exec string,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	connRows := make(map[string]*pgx.Rows)
	for name, conn := range conns {
		rows, err := conn.Values(ctx, prep, exec)
		if err != nil {
			return nil //nolint:returnerrcheck
		}
		defer rows.Close()
		connRows[name] = rows
	}
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
				// This function can fail if, for example,
				// a number doesn't fit into a float64. Ignore
				// them and move along to another query.
				return nil //nolint:returnerrcheck
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
			// Aww someone had a SQL error maybe, so we can't use this query.
			return nil //nolint:returnerrcheck
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
