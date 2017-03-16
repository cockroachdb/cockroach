// Copyright 2017 The Cockroach Authors.
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
//
// Author: Nikhil Benesch (benesch@cockroachlabs.com)

package sql

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestInternalExecutor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	var iex sqlutil.InternalExecutor = InternalExecutor{
		LeaseManager: s.LeaseManager().(*LeaseManager),
	}
	txn := client.NewTxn(context.TODO(), *db)

	t.Run("ExecuteStatement", func(t *testing.T) {
		n, err := iex.ExecuteStatementInTransaction("select-many", txn, "SELECT * FROM (VALUES (1), (2), (3))")
		if err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("expected 3 rows but got %d", n)
		}
	})

	t.Run("QueryRow with constants", func(t *testing.T) {
		row, err := iex.QueryRowInTransaction("select-one", txn, "SELECT 1, 2, 3")
		if err != nil {
			t.Fatal(err)
		}
		expectedRow := make(parser.Datums, 3)
		for i := 0; i < 3; i++ {
			v := parser.DInt(i + 1)
			expectedRow[i] = &v
		}
		if !reflect.DeepEqual(row, expectedRow) {
			t.Fatalf("expected %v but got %v", expectedRow, row)
		}
	})

	t.Run("QueryRow with now()", func(t *testing.T) {
		before := timeutil.Now()
		row, err := iex.QueryRowInTransaction("select-now", txn, "SELECT now()")
		after := timeutil.Now()
		if err != nil {
			t.Fatal(err)
		}
		ts := row[0].(*parser.DTimestampTZ).Time
		if ts.Before(before) {
			t.Fatalf("expected transaction timestamp %v to be after %v", ts, before)
		}
		if ts.After(after) {
			t.Fatalf("expected transaction timestamp %v to be before %v", ts, after)
		}
	})

	t.Run("GetTableSpan", func(t *testing.T) {
		span, err := iex.GetTableSpan(security.RootUser, txn, "system", "namespace")
		if err != nil {
			t.Fatal(err)
		}
		tablePrefixKey := roachpb.Key(keys.MakeTablePrefix(keys.NamespaceTableID))
		expectedSpan := roachpb.Span{
			Key:    tablePrefixKey,
			EndKey: tablePrefixKey.PrefixEnd(),
		}
		if !reflect.DeepEqual(span, expectedSpan) {
			t.Fatalf("expected span %v but got %v", expectedSpan, span)
		}
	})
}
