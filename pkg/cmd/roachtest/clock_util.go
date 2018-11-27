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
	if err := db.QueryRow("SELECT now()::DECIMAL").Scan(&epoch); err != nil {
		return 0, err
	}
	return epoch, nil
}

// offsetInjector is used to inject clock offsets in roachtests
type offsetInjector struct {
	c        *cluster
	deployed bool
}

// deploy installs ntp and downloads / compiles bumptime used to create a clock offset
func (oi *offsetInjector) deploy(ctx context.Context) {
	if err := oi.c.RunE(ctx, oi.c.All(), "test -x ./bumptime"); err != nil {
		oi.c.Install(ctx, oi.c.All(), "ntp")
		oi.c.Install(ctx, oi.c.All(), "gcc")
		oi.c.Run(ctx, oi.c.All(), "sudo", "service", "ntp", "stop")
		oi.c.Run(ctx,
			oi.c.All(),
			"curl",
			"-kO",
			"https://raw.githubusercontent.com/cockroachdb/jepsen/master/cockroachdb/resources/bumptime.c",
		)
		oi.c.Run(ctx, oi.c.All(), "gcc", "bumptime.c", "-o", "bumptime", "&&", "rm bumptime.c")
	}

	oi.deployed = true
}

// offset injects a offset of s into the node with the given nodeID
func (oi *offsetInjector) offset(ctx context.Context, nodeID int, s time.Duration) {
	if !oi.deployed {
		oi.c.t.Fatal("Offset injector must be deployed before injecting a clock offset")
	}

	oi.c.Run(
		ctx,
		oi.c.Node(nodeID),
		fmt.Sprintf("sudo ./bumptime %f", float64(s)/float64(time.Millisecond)),
	)
}

// recover force syncs time on the node with the given nodeID to recover
// from any offsets
func (oi *offsetInjector) recover(ctx context.Context, nodeID int) {
	if !oi.deployed {
		oi.c.t.Fatal("Offset injector must be deployed before recovering from clock offsets")
	}

	syncCmds := [][]string{
		{"sudo", "service", "ntp", "stop"},
		{"sudo", "ntpdate", "-u", "time.google.com"},
		{"sudo", "service", "ntp", "start"},
	}
	for _, cmd := range syncCmds {
		oi.c.Run(
			ctx,
			oi.c.Node(nodeID),
			cmd...,
		)
	}
}

// newOffsetInjector creates a offsetInjector which can be used to inject
// and recover from clock offsets
func newOffsetInjector(c *cluster) *offsetInjector {
	return &offsetInjector{c: c}
}
