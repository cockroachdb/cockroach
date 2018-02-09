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
	"errors"
	"fmt"
	"math/rand"

	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

// These need to be kept in sync with the zones used when --geo is passed
// to roachprod.
//
// TODO(benesch): avoid hardcoding these.
var zones = []string{"us-east1-b", "us-west1-b", "europe-west2-b"}

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

	zoneLocationsStmt = `INSERT INTO system.locations VALUES
		('zone', 'us-east1-b', 33.0641249, -80.0433347),
		('zone', 'us-west1-b', 45.6319052, -121.2010282),
		('zone', 'europe-west2-b', 51.509865, 0)
	`
)

type roachmart struct {
	flags workload.Flags

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

		PreLoad: func(db *gosql.DB) error {
			if _, err := db.Exec(zoneLocationsStmt); err != nil {
				return err
			}
			if !m.partition {
				return nil
			}
			for _, z := range zones {
				_, err := db.Exec(fmt.Sprintf(
					"ALTER PARTITION %[1]q OF TABLE users EXPERIMENTAL CONFIGURE ZONE 'constraints: [+zone=%[1]s]'",
					z))
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
	rng := rand.New(rand.NewSource(m.seed))
	users := workload.Table{
		Name:            `users`,
		Schema:          usersSchema,
		InitialRowCount: m.users,
		InitialRowFn: func(rowIdx int) []interface{} {
			const emailTemplate = `user-%d@roachmart.example`
			return []interface{}{
				zones[rowIdx%3],                     // zone
				fmt.Sprintf(emailTemplate, rowIdx),  // email
				string(randutil.RandBytes(rng, 64)), // address
			}
		},
	}
	orders := workload.Table{
		Name:            `orders`,
		Schema:          ordersSchema,
		InitialRowCount: m.orders,
		InitialRowFn: func(rowIdx int) []interface{} {
			user := users.InitialRowFn(rowIdx % m.users)
			zone, email := user[0], user[1]
			return []interface{}{
				zone,   // user_zone
				email,  // user_email
				rowIdx, // id
				[]string{`f`, `t`}[rowIdx%2], // fulfilled
			}
		},
	}
	return []workload.Table{users, orders}
}

// Ops implements the Opser interface.
func (m *roachmart) Ops() workload.Operations {
	return workload.Operations{
		Name: `fetch one user's orders`,
		Fn: func(sqlDB *gosql.DB, reg *workload.HistogramRegistry) (func(context.Context) error, error) {
			const query = `SELECT * FROM orders WHERE user_zone = $1 AND user_email = $2`
			rng := rand.New(rand.NewSource(m.seed))
			usersTable := m.Tables()[0]
			hists := reg.GetHandle()

			return func(ctx context.Context) error {
				wantLocal := rng.Intn(100) < m.localPercent

				// Pick a random user and advance until we have one that matches
				// our locality requirements.
				var zone, email interface{}
				for i := rng.Int(); ; i++ {
					user := usersTable.InitialRowFn(i % m.users)
					zone, email = user[0], user[1]
					userLocal := zone == m.localZone
					if userLocal == wantLocal {
						break
					}
				}
				start := timeutil.Now()
				_, err := sqlDB.ExecContext(ctx, query, zone, email)
				if wantLocal {
					hists.Get(`local`).Record(timeutil.Since(start))
				} else {
					hists.Get(`remote`).Record(timeutil.Since(start))
				}
				return err
			}, nil
		},
	}
}
