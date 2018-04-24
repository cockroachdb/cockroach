// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"
)

// isAlive returns whether the node queried by db is alive
func isAlive(db *gosql.DB) bool {
	_, err := db.Exec("SHOW DATABASES")
	return err == nil
}

// dbUnixEpoch returns the current time in db
func dbUnixEpoch(db *gosql.DB) (float64, error) {
	var epoch float64
	if err := db.QueryRow("SELECT NOW()::DECIMAL").Scan(&epoch); err != nil {
		return 0, err
	}
	return epoch, nil
}

// skewInjector is used to inject skews in roachtests
type skewInjector struct {
	c        *cluster
	deployed bool
}

// deploy installs ntp and downloads / compiles bumptime used to create a clock skew
func (si *skewInjector) deploy(ctx context.Context) {
	si.c.Install(ctx, si.c.All(), "ntp")
	si.c.Run(ctx, si.c.All(), "curl -kO https://raw.githubusercontent.com/cockroachdb/jepsen/master/"+
		"cockroachdb/resources/bumptime.c")
	si.c.Run(ctx, si.c.All(), "gcc bumptime.c -o bumptime && rm bumptime.c")
	si.deployed = true
}

// skew injects a skew of s into the node with the given nodeID
func (si *skewInjector) skew(ctx context.Context, nodeID int, s time.Duration) {
	if !si.deployed {
		si.c.t.Fatal("Skew injector must be deployed before injecting a clock skew")
	}

	si.c.Run(
		ctx,
		si.c.Node(nodeID),
		fmt.Sprintf("./bumptime %f", float64(s)/float64(time.Millisecond)),
	)
}

// recover force syncs time on the node with the given nodeID to recover
// from any skews
func (si *skewInjector) recover(ctx context.Context, nodeID int) {
	if !si.deployed {
		si.c.t.Fatal("Skew injector must be deployed before recovering from clock skews")
	}

	syncCmds := []string{
		"service ntp stop",
		"ntpdate -u time.google.com",
		"service ntp start",
	}
	for _, cmd := range syncCmds {
		si.c.Run(
			ctx,
			si.c.Node(nodeID),
			cmd,
		)
	}
}

// newSkewInjector creates a skewInjector which can be used to inject
// and recover from skew
func newSkewInjector(c *cluster) *skewInjector {
	return &skewInjector{c: c}
}
