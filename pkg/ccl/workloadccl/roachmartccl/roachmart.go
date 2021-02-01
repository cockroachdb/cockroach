// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package roachmartccl

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// These need to be kept in sync with the zones used when --geo is passed
// to roachprod.
//
// TODO(benesch): avoid hardcoding these.
var zones = []string{"us-central1-b", "us-west1-b", "europe-west2-b"}

var usersSchema = func() string {
	var buf bytes.Buffer
	buf.WriteString(`(
	zone STRING,
	email STRING,
	address STRING,
	PRIMARY KEY (zone, email)
) PARTITION BY LIST (zone) (`)
	for i, z := range zones {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf("\n\tPARTITION %[1]q VALUES IN ('%[1]s')", z))
	}
	buf.WriteString("\n)")
	return buf.String()
}()

const (
	ordersSchema = `(
		user_zone STRING,
		user_email STRING,
		id INT,
		fulfilled BOOL,
		PRIMARY KEY (user_zone, user_email, id),
		FOREIGN KEY (user_zone, user_email) REFERENCES users
	) INTERLEAVE IN PARENT users (user_zone, user_email)`

	defaultUsers  = 10000
	defaultOrders = 100000

	zoneLocationsStmt = `UPSERT INTO system.locations VALUES
		('zone', 'us-east1-b', 33.0641249, -80.0433347),
		('zone', 'us-west1-b', 45.6319052, -121.2010282),
		('zone', 'europe-west2-b', 51.509865, 0)
	`
)

type roachmart struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed          int64
	partition     bool
	localZone     string
	localPercent  int
	users, orders int
}

func init() {
	workload.Register(roachmartMeta)
}

var roachmartMeta = workload.Meta{
	Name:        `roachmart`,
	Description: `Roachmart models a geo-distributed online storefront with users and orders`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &roachmart{}
		g.flags.FlagSet = pflag.NewFlagSet(`roachmart`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`local-zone`:    {RuntimeOnly: true},
			`local-percent`: {RuntimeOnly: true},
		}
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.BoolVar(&g.partition, `partition`, true, `Whether to apply zone configs to the partitions of the users table.`)
		g.flags.StringVar(&g.localZone, `local-zone`, ``, `The zone in which this load generator is running.`)
		g.flags.IntVar(&g.localPercent, `local-percent`, 50, `Percent (0-100) of operations that operate on local data.`)
		g.flags.IntVar(&g.users, `users`, defaultUsers, `Initial number of accounts in users table.`)
		g.flags.IntVar(&g.orders, `orders`, defaultOrders, `Initial number of orders in orders table.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (m *roachmart) Meta() workload.Meta { return roachmartMeta }

// Flags implements the Flagser interface.
func (m *roachmart) Flags() workload.Flags { return m.flags }

// Hooks implements the Hookser interface.
func (m *roachmart) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if m.localZone == "" {
				return errors.New("local zone must be specified")
			}
			found := false
			for _, z := range zones {
				if z == m.localZone {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("unknown zone %q (options: %s)", m.localZone, zones)
			}
			return nil
		},

		PreCreate: func(db *gosql.DB) error {
			if _, err := db.Exec(`SET CLUSTER SETTING sql.defaults.interleaved_tables.enabled = true`); err != nil {
				return err
			}
			return nil
		},

		PreLoad: func(db *gosql.DB) error {
			if _, err := db.Exec(zoneLocationsStmt); err != nil {
				return err
			}
			if !m.partition {
				return nil
			}
			for _, z := range zones {
				// We are removing the EXPERIMENTAL keyword in 2.1. For compatibility
				// with 2.0 clusters we still need to try with it if the
				// syntax without EXPERIMENTAL fails.
				// TODO(knz): Remove this in 2.2.
				makeStmt := func(s string) string {
					return fmt.Sprintf(s, fmt.Sprintf("%q", z), fmt.Sprintf("'constraints: [+zone=%s]'", z))
				}
				stmt := makeStmt("ALTER PARTITION %[1]s OF TABLE users CONFIGURE ZONE = %[2]s")
				_, err := db.Exec(stmt)
				if err != nil && strings.Contains(err.Error(), "syntax error") {
					stmt = makeStmt("ALTER PARTITION %[1]s OF TABLE users EXPERIMENTAL CONFIGURE ZONE %[2]s")
					_, err = db.Exec(stmt)
				}
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (m *roachmart) Tables() []workload.Table {
	users := workload.Table{
		Name:   `users`,
		Schema: usersSchema,
		InitialRows: workload.Tuples(
			m.users,
			func(rowIdx int) []interface{} {
				rng := rand.New(rand.NewSource(m.seed + int64(rowIdx)))
				const emailTemplate = `user-%d@roachmart.example`
				return []interface{}{
					zones[rowIdx%3],                     // zone
					fmt.Sprintf(emailTemplate, rowIdx),  // email
					string(randutil.RandBytes(rng, 64)), // address
				}
			},
		),
	}
	orders := workload.Table{
		Name:   `orders`,
		Schema: ordersSchema,
		InitialRows: workload.Tuples(
			m.orders,
			func(rowIdx int) []interface{} {
				user := users.InitialRows.BatchRows(rowIdx % m.users)[0]
				zone, email := user[0], user[1]
				return []interface{}{
					zone,                         // user_zone
					email,                        // user_email
					rowIdx,                       // id
					[]string{`f`, `t`}[rowIdx%2], // fulfilled
				}
			},
		),
	}
	return []workload.Table{users, orders}
}

// Ops implements the Opser interface.
func (m *roachmart) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(m, m.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(m.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(m.connFlags.Concurrency + 1)

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}

	const query = `SELECT * FROM orders WHERE user_zone = $1 AND user_email = $2`
	for i := 0; i < m.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewSource(m.seed))
		usersTable := m.Tables()[0]
		hists := reg.GetHandle()
		workerFn := func(ctx context.Context) error {
			wantLocal := rng.Intn(100) < m.localPercent

			// Pick a random user and advance until we have one that matches
			// our locality requirements.
			var zone, email interface{}
			for i := rng.Int(); ; i++ {
				user := usersTable.InitialRows.BatchRows(i % m.users)[0]
				zone, email = user[0], user[1]
				userLocal := zone == m.localZone
				if userLocal == wantLocal {
					break
				}
			}
			start := timeutil.Now()
			_, err := db.Exec(query, zone, email)
			if wantLocal {
				hists.Get(`local`).Record(timeutil.Since(start))
			} else {
				hists.Get(`remote`).Record(timeutil.Since(start))
			}
			return err
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
}
