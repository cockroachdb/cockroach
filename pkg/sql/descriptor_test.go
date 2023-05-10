// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestFormatDefaultRegionNotice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		primary string
		regions []string
		expect  string
	}
	tests := []testCase{
		{
			primary: "us-east1",
			expect:  `defaulting to 'WITH PRIMARY REGION "us-east1"' as no primary region was specified`,
		},
		{
			primary: "us-east1",
			regions: []string{"us-west2"},
			expect:  `defaulting to 'WITH PRIMARY REGION "us-east1" REGIONS "us-west2"' as no primary region was specified`,
		},
		{
			primary: "us-east1",
			regions: []string{"us-west2", "us-central3"},
			expect:  `defaulting to 'WITH PRIMARY REGION "us-east1" REGIONS "us-west2", "us-central3"' as no primary region was specified`,
		},
	}
	for _, test := range tests {
		var regions []tree.Name
		for _, region := range test.regions {
			regions = append(regions, tree.Name(region))
		}
		require.Equal(t, test.expect, formatDefaultRegionNotice(tree.Name(test.primary), regions).Error())
	}
}

func TestCreatePrivOnPublic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	envutil.TestSetEnv(t, "COCKROACH_NO_DEF_CREATE_ON_PUBLIC", "true")
	params := base.TestServerArgs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := db.Exec(` 
CREATE DATABASE t1; 
CREATE ROLE test; 
CREATE DATABASE test WITH OWNER test;
SET ROLE test;
`)
	require.NoError(t, err)

	_, err = db.Exec(`
CREATE TABLE t1.foo(a int)`)
	require.Error(t, err)

	_, err = db.Exec(`
CREATE TABLE test.foo(a int)`)
	require.NoError(t, err)

	_, err = db.Exec(`
SET ROLE root;
ALTER ROLE ALL SET nondefault_create_on_public = false;
SET ROLE test;
CREATE TABLE t1.foo(a int)
`)
	require.Error(t, err)
}
