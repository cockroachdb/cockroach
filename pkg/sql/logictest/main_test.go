// Copyright 2015 The Cockroach Authors.
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

package logictest

import (
	"os"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go

// Add a placeholder implementation to test the plan hook. It accepts statements
// of the form `SHOW planhook` and returns a single row with the string value
// 'planhook'.
func init() {
	testingPlanHook := func(
		stmt parser.Statement, state sql.PlanHookState,
	) (func(context.Context, chan<- parser.Datums) error, sqlbase.ResultColumns, error) {
		show, ok := stmt.(*parser.Show)
		if !ok || show.Name != "planhook" {
			return nil, nil, nil
		}
		header := sqlbase.ResultColumns{
			{Name: "value", Typ: parser.TypeString},
		}
		return func(_ context.Context, resultsCh chan<- parser.Datums) error {
			resultsCh <- parser.Datums{parser.NewDString(show.Name)}
			return nil
		}, header, nil
	}
	sql.AddPlanHook(testingPlanHook)
}

func TestMain(m *testing.M) {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}
