// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package roachmartccl

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"math/rand"

	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/testutils/workload"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var dataCenters = []string{"us_east", "us_west", "europe"}

// These need to be kept in sync with dataCenters, above, and the localities
// used when --geo is passed to roachprod.
//
// TODO(benesch): avoid hardcoding these.
var localities = []string{"us-east1-b", "us-west1-b", "europe-west2-b"}

const (
	usersSchema = `(
		data_center STRING,
		email STRING,
		address STRING,
		PRIMARY KEY (data_center, email)
	) PARTITION BY LIST (data_center) (
		PARTITION us_east VALUES IN ('us_east'),
		PARTITION us_west VALUES IN ('us_west'),
		PARTITION europe  VALUES IN ('europe')
	)`

	ordersSchema = `(
		user_data_center STRING,
		user_email STRING,
		id INT DEFAULT unique_rowid(),
		fulfilled BOOL,
		PRIMARY KEY (user_data_center, user_email, id),
		FOREIGN KEY (user_data_center, user_email) REFERENCES users
	) INTERLEAVE IN PARENT users (user_data_center, user_email)`

	defaultUsers  = 10000
	defaultOrders = 100000
)

type roachmart struct {
	flags *pflag.FlagSet

	seed            int64
	partition       bool
	localDataCenter string
	localPercent    int
	users, orders   int
}

func init() {
	workload.Register(roachmartMeta)
}

var roachmartMeta = workload.Meta{
	Name:        `roachmart`,
	Description: `Roachmart models a geo-distributed online storefront with users and orders`,
	New: func() workload.Generator {
		g := &roachmart{flags: pflag.NewFlagSet(`roachmart`, pflag.ContinueOnError)}
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.BoolVar(&g.partition, `partition`, false, `Whether to apply zone configs to the partitions of the users table.`)
		g.flags.StringVar(&g.localDataCenter, `local-data-center`, ``, `The data center in which this load generator is running.`)
		g.flags.IntVar(&g.localPercent, `local-percent`, 50, `Percent (0-100) of operations that operate on local data.`)
		g.flags.IntVar(&g.users, `users`, defaultUsers, `Initial number of accounts in users table.`)
		g.flags.IntVar(&g.orders, `orders`, defaultOrders, `Initial number of orders in orders table.`)
		return g
	},
}

// Meta implements the Generator interface.
func (m *roachmart) Meta() workload.Meta { return roachmartMeta }

// Flags implements the Generator interface.
func (m *roachmart) Flags() *pflag.FlagSet {
	return m.flags
}

// Hooks implements the Generator interface.
func (m *roachmart) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if m.localDataCenter == "" {
				return errors.New("local data center must be specified")
			}
			found := false
			for _, dc := range dataCenters {
				if dc == m.localDataCenter {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("unknown local data center %q (options: %s)",
					m.localDataCenter, dataCenters)
			}
			return nil
		},

		PreLoad: func(db *gosql.DB) error {
			if !m.partition {
				return nil
			}
			for i, dc := range dataCenters {
				locality := localities[i]
				_, err := db.Exec(fmt.Sprintf(
					"ALTER PARTITION %s OF TABLE %s EXPERIMENTAL CONFIGURE ZONE 'constraints: [+data-center=%s]'",
					dc, "users", locality))
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
		InitialRowFn: func(rowIdx int) []string {
			const emailTemplate = `'user-%d@roachmart.example'`
			return []string{
				`'` + dataCenters[rowIdx%3] + `'`,               // data_center
				fmt.Sprintf(emailTemplate, rowIdx),              // email
				`'` + string(randutil.RandBytes(rng, 64)) + `'`, // address
			}
		},
	}
	orders := workload.Table{
		Name:            `orders`,
		Schema:          ordersSchema,
		InitialRowCount: m.orders,
		InitialRowFn: func(rowIdx int) []string {
			user := users.InitialRowFn(rowIdx % m.users)
			dataCenter, email := user[0], user[1]
			return []string{
				dataCenter,                       // user_data_center
				email,                            // user_email
				"DEFAULT",                        // id
				[]string{`'f'`, `'t'`}[rowIdx%2], // fulfilled
			}
		},
	}
	return []workload.Table{users, orders}
}

func (m *roachmart) Ops() []workload.Operation {
	op := workload.Operation{
		Name: `fetch one user's orders`,
		Fn: func(sqlDB *gosql.DB) (func(context.Context) error, error) {
			const query = `SELECT * FROM orders WHERE user_data_center = $1 AND user_email = $2`
			rng := rand.New(rand.NewSource(m.seed))
			usersTable := m.Tables()[0]

			return func(ctx context.Context) error {
				wantLocal := rng.Intn(100) < m.localPercent

				// Pick a random user and advance until we have one that matches
				// our locality requirements.
				var dc, email string
				for i := rng.Int(); ; i++ {
					user := usersTable.InitialRowFn(i % m.users)
					dc, email = user[0], user[1]
					userLocal := dc == `'`+m.localDataCenter+`'`
					if userLocal == wantLocal {
						break
					}
				}
				fmt.Println(query, dc, email)
				_, err := sqlDB.ExecContext(ctx, query, dc, email)
				return err
			}, nil
		},
	}
	return []workload.Operation{op}
}
