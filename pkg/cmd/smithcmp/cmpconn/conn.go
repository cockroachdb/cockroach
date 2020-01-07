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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/jackc/pgx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
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
func (c *Conn) Values(ctx context.Context, prep, exec string) ([][]interface{}, error) {
	if prep != "" {
		rows, err := c.PGX.QueryEx(ctx, prep, simpleProtocol)
		if err != nil {
			return nil, err
		}
		rows.Close()
	}
	exec, _ = mutations.ApplyString(c.rng, exec, c.sqlMutators...)
	rows, err := c.PGX.QueryEx(ctx, exec, simpleProtocol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var vals [][]interface{}
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			return nil, err
		}
		vals = append(vals, row)
	}
	return vals, rows.Err()
}

var simpleProtocol = &pgx.QueryExOptions{SimpleProtocol: true}

// CompareConns executes prep and exec on all connections in conns. If any
// differ, an error is returned.
func CompareConns(
	ctx context.Context, timeout time.Duration, conns map[string]*Conn, prep, exec string,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	vvs := map[string][][]interface{}{}
	var lock syncutil.Mutex
	for name := range conns {
		name := name
		g.Go(func() error {
			conn := conns[name]
			vals, err := conn.Values(ctx, prep, exec)
			if err != nil {
				return errors.Wrap(err, name)
			}
			lock.Lock()
			vvs[name] = vals
			lock.Unlock()
			return nil
		})
	}
	err := g.Wait()
	if err != nil {
		fmt.Println("ERR:", err)
		// We don't care about SQL errors because sqlsmith sometimes
		// produces bogus queries.
		return nil
	}
	var first [][]interface{}
	var firstName string
	for name, vals := range vvs {
		if first == nil {
			first = vals
			firstName = name
			continue
		}
		compareStart := timeutil.Now()
		fmt.Printf("comparing %s to %s...", firstName, name)
		err := CompareVals(first, vals)
		fmt.Printf(" %s\n", timeutil.Since(compareStart))
		if err != nil {
			return err
		}
	}
	return nil
}
