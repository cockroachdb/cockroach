// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	gosql "database/sql"
	"time"

	"github.com/jackc/pgx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// Conn holds gosql and pgx connections.
type Conn struct {
	DB  *gosql.DB
	PGX *pgx.Conn
}

// NewConn returns a new Conn on the given uri and executes initSQL on it.
func NewConn(uri string, initSQL ...string) (*Conn, error) {
	var c Conn

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

// Values executes prep and exec and returns the results of exec.
func (c *Conn) Values(ctx context.Context, prep, exec string) ([][]interface{}, error) {
	if prep != "" {
		rows, err := c.PGX.QueryEx(ctx, prep, simpleProtocol)
		if err != nil {
			return nil, err
		}
		rows.Close()
	}
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
