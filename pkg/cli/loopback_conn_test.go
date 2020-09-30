// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestLoopbackConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(context.Background())

	if _, err := db.Exec("CREATE USER operator WITH PASSWORD 'unused'"); err != nil {
		t.Fatal(err)
	}

	s := ts.(*server.TestServer)

	sqlConn := newLoopbackSQLConn(s.Server, "operator")

	if err := sqlConn.ensureConn(); err != nil {
		t.Fatal(err)
	}

	if err := sqlConn.Exec("BEGIN", nil); err != nil {
		t.Fatal(err)
	}

	data, err := sqlConn.QueryRow("SELECT 123, current_user", nil)
	if err != nil {
		t.Fatal(err)
	}
	if i, ok := data[0].(int64); !ok || i != 123 {
		t.Fatalf("expected int 123, got %v (%T)", data[0], data[0])
	}
	if i, ok := data[1].(string); !ok || i != "operator" {
		t.Fatalf("expected string operator, got %v (%T)", data[1], data[1])
	}

	if err := sqlConn.Exec("COMMIT", nil); err != nil {
		t.Fatal(err)
	}
}
