// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// "make test" would normally test this file, but it should only be tested
// within docker compose.

// +build compose

package compare

import (
	"context"
	"flag"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/smithcmp/cmpconn"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/mutations"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var flagEach = flag.Duration("each", 10*time.Minute, "individual test timeout")

func TestCompare(t *testing.T) {
	uris := map[string]string{
		"postgres":   "postgresql://postgres@postgres:5432/",
		"cockroach1": "postgresql://root@cockroach1:26257/?sslmode=disable",
		"cockroach2": "postgresql://root@cockroach2:26257/?sslmode=disable",
	}
	configs := map[string]testConfig{
		"default": {
			opts: []sqlsmith.SmitherOption{sqlsmith.PostgresMode()},
			conns: []testConn{
				{
					name:     "cockroach1",
					mutators: []sqlbase.Mutator{mutations.PostgresMutator},
				},
				{
					name:     "postgres",
					mutators: []sqlbase.Mutator{mutations.PostgresMutator},
				},
			},
		},
	}

	ctx := context.Background()
	for confName, config := range configs {
		t.Run(confName, func(t *testing.T) {
			rng, _ := randutil.NewPseudoRand()
			conns := map[string]*cmpconn.Conn{}
			for _, testCn := range config.conns {
				uri, ok := uris[testCn.name]
				if !ok {
					t.Fatalf("bad connection name: %s", testCn.name)
				}
				conn, err := cmpconn.NewConn(uri, rng, testCn.mutators)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				conns[testCn.name] = conn
			}
			smither, err := sqlsmith.NewSmither(conns[config.conns[0].name].DB, rng, config.opts...)
			if err != nil {
				t.Fatal(err)
			}
			until := time.After(*flagEach)
			for {
				select {
				case <-until:
					return
				default:
				}
				query := smither.Generate()
				// #44029
				if strings.Contains(query, "FULL JOIN") {
					continue
				}
				// #44079
				if strings.Contains(query, "|| NULL::") {
					continue
				}
				query, _ = mutations.ApplyString(rng, query, mutations.PostgresMutator)
				if err := cmpconn.CompareConns(ctx, time.Second*30, conns, "" /* prep */, query); err != nil {
					_ = ioutil.WriteFile("/test/query.sql", []byte(query+";"), 0666)
					t.Log(query)
					t.Fatal(err)
				}
				// Make sure we can still ping on a connection. If we can't we may have
				// crashed something.
				for name, conn := range conns {
					if err := conn.Ping(); err != nil {
						t.Log(query)
						t.Fatalf("%s: ping: %v", name, err)
					}
				}
			}
		})
	}
}

type testConfig struct {
	opts  []sqlsmith.SmitherOption
	conns []testConn
}

type testConn struct {
	name     string
	mutators []sqlbase.Mutator
}
