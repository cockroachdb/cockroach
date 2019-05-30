// Copyright 2019 The Cockroach Authors.
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
// permissions and limitations under the License.

package main

import (
	"context"
	gosql "database/sql"
	"time"

	"github.com/jackc/pgx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

type Conn struct {
	DB  *gosql.DB
	PGX *pgx.Conn
}

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

func (c *Conn) Close() {
	c.DB.Close()
	c.PGX.Close()
}

func (c *Conn) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return c.PGX.Ping(ctx)
}

func (c *Conn) Exec(s string) error {
	_, err := c.PGX.Exec(s)
	return errors.Wrap(err, "exec")
}

func (c *Conn) Values(ctx context.Context, s string) ([][]interface{}, error) {
	rows, err := c.PGX.QueryEx(ctx, s, nil)
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
