// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/layered"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func registerLayer(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "layer",
		Owner:   registry.OwnerTestEng,
		Cluster: r.MakeClusterSpec(6),
		Run:     runLayer,
	})
}

func initRunDropKVSeq() layered.Sequence {
	return &layered.SliceSequence{
		Name: "kv",
		Items: []layered.Step{
			&layered.SimpleStep{
				N: "init-kv",
				O: registry.OwnerTestEng,
				F: func(ctx context.Context, r *rand.Rand, t layered.Fataler, c layered.SeqEnv) {
					db := c.Unique("kv")
					c.InvokeWorkload(ctx, t, "init", "kv", "--db", db, "--splits", "10")
					c.State()["db"] = db
				},
			},
			&layered.SimpleStep{
				N: "run-kv",
				O: registry.OwnerTestEng,
				F: func(ctx context.Context, r *rand.Rand, t layered.Fataler, c layered.SeqEnv) {
					c.InvokeWorkload(ctx, t, "run", "kv", "--db", c.State()["db"].(string), "--max-rate", "10", "--duration", "60s")
				},
			},
			&layered.SimpleStep{
				N: "drop-kv",
				O: registry.OwnerTestEng,
				F: func(ctx context.Context, r *rand.Rand, t layered.Fataler, c layered.SeqEnv) {
					sqlutils.MakeSQLRunner(c.Conn()).Exec(t, `DROP DATABASE `+c.State()["db"].(string))
				},
			},
		},
	}
}

func copyRandomTableSeq() layered.Sequence {
	return &layered.SliceSequence{
		Name: "copy-random-table",
		Items: []layered.Step{
			&layered.SimpleStep{
				N: "discover-table",
				O: registry.OwnerTestEng,
				F: func(ctx context.Context, r *rand.Rand, t layered.Fataler, c layered.SeqEnv) {
					db := sqlutils.MakeSQLRunner(c.Conn())
					// NB: there should be a way to more deliberately pick a schema
					// that the user has access to because we probably don't want to
					// restrict ourselves to running as root here (think tenants etc
					// down the line).
					row := db.QueryRow(t, `
select schema_name, database_name, name, now() from crdb_internal.tables where database_name is not null order by random() limit 1
`)
					var schemaName string
					var dbName string
					var tableName string
					var ts string
					row.Scan(&schemaName, &dbName, &tableName, &ts)
					c.State()["db"] = dbName
					c.State()["schema"] = schemaName
					c.State()["table"] = tableName
					c.State()["ts"] = ts
				},
			},
			&layered.SimpleStep{
				N: "copy-table",
				O: registry.OwnerTestEng,
				F: func(ctx context.Context, r *rand.Rand, t layered.Fataler, c layered.SeqEnv) {
					fqTable := fmt.Sprintf("%s.%s.%s", c.State()["db"], c.State()["schema"], c.State()["table"])
					dest := c.Unique(c.State()["table"].(string))
					// ts := c.State()["ts"] // NB: passing timestamps to AOST seems pretty pedestrian, is there a better way?
					sqlutils.MakeSQLRunner(c.Conn()).Exec(t,
						// fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM %s AS OF SYSTEM TIME '%s'`, dest, fqTable, ts),
						// ^-- unsupported unfortunately, see:
						// https://cockroachlabs.slack.com/archives/C01RX2G8LT1/p1653079384313619
						// Need another way to make sure a) not interfering with foreground traffic b)
						// table is actually still there.
						fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM %s`, dest, fqTable),
					)
					c.State()["dest"] = dest
				},
			},
			&layered.SimpleStep{
				N: "drop-copy",
				O: registry.OwnerTestEng,
				F: func(ctx context.Context, r *rand.Rand, t layered.Fataler, c layered.SeqEnv) {
					sqlutils.MakeSQLRunner(c.Conn()).Exec(t, `DROP TABLE `+c.State()["dest"].(string))
				},
			},
		},
	}
}

func runLayer(ctx context.Context, t test.Test, c cluster.Cluster) {
	var seqs []layered.WeightedSequence
	for i := 0; i < 4; i++ {
		seqs = append(seqs,
			layered.WeightedSequence{
				Weight: 1.0,
				Seq:    initRunDropKVSeq(),
			},
			layered.WeightedSequence{
				Weight: 1.0,
				Seq:    copyRandomTableSeq(),
			},
		)
	}

	c.Put(ctx, t.Cockroach(), "cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, 5))

	var rec layered.UMLRecorder
	defer func() {
		_ = os.WriteFile(filepath.Join(t.ArtifactsDir(), "schedule.puml.txt"), []byte(rec.String()), 0644)
	}()
	sch := &layered.Scheduler{SchedOptions: layered.SchedOptions{
		Sequences:   seqs,
		Concurrency: 3,
		EventHandler: func(ctx context.Context, event layered.Event) {
			switch ev := event.(type) {
			case *layered.EvStepStart:
				// This is dumb but otherwise the previous stop and current start
				// fall into the same time slot for the UMLRecorder and it will
				// not render properly.
				// Might be easier to give each sequence its own ID, like <id>_seqname,
				// then the staggering is also more obvious.
				time.Sleep(2 * time.Second)
				rec.Record(timeutil.Now(), strconv.Itoa(ev.Worker+1), ev.S)
			case *layered.EvStepDone:
				rec.Idle(timeutil.Now(), strconv.Itoa(ev.Worker+1))
				if ev.Result != nil {
					t.L().Errorf("%v", ev)
				}
			default:
				t.L().Printf("%v", ev)
			}
		},
		Rand: rand.New(rand.NewSource(0)),
	}}
	require.NoError(t, layered.Run(ctx, sch, &layeredEnv{
		c:                  c,
		l:                  t.L(),
		connNode:           1,
		clusterNodes:       c.Range(1, 5),
		workloadDeployNode: 6,
	}))

}

type layeredEnv struct {
	c                  cluster.Cluster
	l                  *logger.Logger
	connNode           int // TODO(tbg): needs to be dynamic + protected if there is downtime, etc
	clusterNodes       option.NodeListOption
	workloadDeployNode int
}

func (l *layeredEnv) InvokeWorkload(ctx context.Context, t layered.Fataler, args ...string) {
	args = append([]string{"./cockroach", "workload"}, args...)
	// NB: this should be something Env knows about so it can be scraped once we
	// add prometheus observability into the mix. For POC/starters we could
	// probably just allocate (and hard-code into the prom config) 1000 ports and
	// hand them out one by one.
	args = append(args, "--prometheus-port", "0")
	sl, err := l.c.ExternalPGUrl(ctx, l.l, l.clusterNodes)
	require.NoError(t, err)
	args = append(args, sl...)

	// NB: must not use Run here because c's Fatal tears down the entire Run. All
	// cluster.Cluster methods that call Fatalf really ought to take the `t`, as
	// is it's just a bad interface.
	if err := l.c.RunE(ctx, l.c.Node(l.workloadDeployNode), args...); err != nil {
		t.Fatalf("%v", err)
	}
}

func (l *layeredEnv) Conn() *gosql.DB {
	return l.c.Conn(context.Background(), l.l, l.connNode)
}
