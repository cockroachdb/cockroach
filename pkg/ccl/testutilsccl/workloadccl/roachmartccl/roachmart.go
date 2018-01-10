// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package roachmartccl

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/testutils/workload"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const (
	usersSchema = `(
		data_center STRING,
		email STRING,
		address STRING,
		PRIMARY KEY (data_center, email)
	) PARTITION BY LIST (data_center) (
		PARTITION us_west VALUES IN ('us-west'),
		PARTITION us_east VALUES IN ('us-east'),
		PARTITION europe  VALUES IN ('eu')
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

	seed          int64
	users, orders int
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

// Configure implements the Generator interface.
func (m *roachmart) Configure(flags []string) error {
	if m.flags.Parsed() {
		return errors.New("Configure was already called")
	}
	return m.flags.Parse(flags)
}

// Tables implements the Generator interface.
func (m *roachmart) Tables() []workload.Table {
	rng := rand.New(rand.NewSource(m.seed))
	users := workload.Table{
		Name:            `users`,
		Schema:          usersSchema,
		InitialRowCount: m.users,
		InitialRowFn: func(rowIdx int) []string {
			var dataCenters = []string{`'us-east'`, `'us-west'`, `'eu'`}
			const emailTemplate = `'user-%d@roachmart.example'`
			return []string{
				dataCenters[rowIdx%3],                           // data_center
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
	return nil
}
