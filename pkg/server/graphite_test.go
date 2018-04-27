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
// permissions and limitations under the License.

package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestGraphite tests that a server pushes metrics data to Graphite endpoint,
// if configured. In addition, it verifies that things don't fall apart when
// the endpoint goes away.
func TestGraphite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ctx := context.Background()

	const setQ = `SET CLUSTER SETTING "%s" = "%s"`
	const interval = 3 * time.Millisecond
	db := sqlutils.MakeSQLRunner(rawDB)
	db.Exec(t, fmt.Sprintf(setQ, graphiteIntervalKey, interval))

	listen := func() {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal("failed to open port", err)
		}
		p := lis.Addr().String()
		log.Infof(ctx, "Open port %s and listening", p)

		defer func() {
			log.Infof(ctx, "Close port %s", p)
			if err := lis.Close(); err != nil {
				t.Fatal("failed to close port", err)
			}
		}()

		db.Exec(t, fmt.Sprintf(setQ, "external.graphite.endpoint", p))
		if _, e := lis.Accept(); e != nil {
			t.Fatal("failed to receive connection", e)
		} else {
			log.Info(ctx, "received connection")
		}
	}

	listen()
	log.Info(ctx, "Make sure things don't fall apart when endpoint goes away.")
	time.Sleep(5 * interval)
	listen()
}
