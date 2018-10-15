// Copyright 2016 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestErrorCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	count1 := telemetry.GetFeatureCounts()["errorcodes."+pgerror.CodeSyntaxError]

	_, err := db.Query("SELECT 1+")
	if err == nil {
		t.Fatal("expected error, got no error")
	}

	count2 := telemetry.GetFeatureCounts()["errorcodes."+pgerror.CodeSyntaxError]

	if count2-count1 != 1 {
		t.Fatalf("expected 1 syntax error, got %d", count2-count1)
	}

	rows, err := db.Query(`SHOW SYNTAX 'SELECT 1+'`)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	for rows.Next() {
		// Do nothing. SHOW SYNTAX itself is tested elsewhere.
		// We just check the counts below.
	}
	rows.Close()

	count3 := telemetry.GetFeatureCounts()["errorcodes."+pgerror.CodeSyntaxError]

	if count3-count2 != 1 {
		t.Fatalf("expected 1 syntax error, got %d", count3-count2)
	}
}
