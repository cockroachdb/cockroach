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

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var testSetting = settings.RegisterIntSetting(
	"sql.test_setting",
	"a test setting used by TestSetClusterSetting",
	0,
)

// TestSetClusterSetting verifies the expected semantics for the SET CLUSTER
// SETTING statement. The update should be visible on the node that executes the
// statement as soon as the statement returns, and it should eventually become
// visible on the other nodes in the cluster too.
func TestSetClusterSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numNodes = 2
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sv0 := &tc.Server(0).ClusterSettings().SV
	sv1 := &tc.Server(0).ClusterSettings().SV
	verify := func(e int64) {
		t.Helper()
		if a := testSetting.Get(sv0); e != a {
			t.Fatalf("settings update was not immediately reflected on node that executed update: "+
				"expected %d, got %d", e, a)
		}
		testutils.SucceedsSoon(t, func() error {
			if a := testSetting.Get(sv1); e != a {
				return fmt.Errorf("settings update is not yet reflected on other node in cluster: "+
					"expected %d, got %d", e, a)
			}
			return nil
		})
	}

	db0 := tc.ServerConn(0)
	if _, err := db0.Exec(`SET CLUSTER SETTING sql.test_setting = 1`); err != nil {
		t.Fatal(err)
	}
	verify(int64(1))
	if _, err := db0.Exec(`RESET CLUSTER SETTING sql.test_setting`); err != nil {
		t.Fatal(err)
	}
	verify(testSetting.Default())
}
