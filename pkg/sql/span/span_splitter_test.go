// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package span_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSpanSplitterDoesNotSplitSystemTableFamilySpans(t *testing.T) {
	splitter := span.MakeSplitter(
		systemschema.DescriptorTable,
		systemschema.DescriptorTable.GetPrimaryIndex(),
		intsets.MakeFast(0),
	)

	if res := splitter.CanSplitSpanIntoFamilySpans(1, false); res {
		t.Errorf("expected the system table to not be splittable")
	}
}

func TestSpanSplitterCanSplitSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	codec := srv.ApplicationLayer().Codec()
	tcs := []struct {
		sql           string
		index         string
		prefixLen     int
		neededColumns intsets.Fast
		containsNull  bool
		canSplit      bool
	}{
		{
			sql:           "a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), FAMILY (a, b, c), FAMILY (d)",
			index:         "t_pkey",
			prefixLen:     2,
			neededColumns: intsets.MakeFast(0),
			canSplit:      true,
		},
		{
			sql:           "a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), FAMILY (a, b, c), FAMILY (d)",
			index:         "t_pkey",
			prefixLen:     1,
			neededColumns: intsets.MakeFast(0),
			canSplit:      false,
		},
		{
			sql:           "a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), FAMILY (a, b, c, d)",
			index:         "t_pkey",
			prefixLen:     2,
			neededColumns: intsets.MakeFast(0),
			canSplit:      true,
		},
		{
			sql:           "a INT, b INT, c INT, INDEX i (b) STORING (a, c), FAMILY (a), FAMILY (b), FAMILY (c)",
			index:         "i",
			prefixLen:     1,
			neededColumns: intsets.MakeFast(0),
			canSplit:      false,
		},
		{
			sql:           "a INT, b INT, c INT, UNIQUE INDEX i (b) STORING (a, c), FAMILY (a), FAMILY (b), FAMILY (c)",
			index:         "i",
			prefixLen:     1,
			neededColumns: intsets.MakeFast(0),
			containsNull:  true,
			canSplit:      false,
		},
		{
			sql:           "a INT, b INT, c INT, UNIQUE INDEX i (b) STORING (a, c), FAMILY (a), FAMILY (b), FAMILY (c)",
			index:         "i",
			prefixLen:     1,
			neededColumns: intsets.MakeFast(0),
			containsNull:  false,
			canSplit:      true,
		},
		{
			sql:           "a INT, b INT, c INT, UNIQUE INDEX i (b), FAMILY (a), FAMILY (b), FAMILY (c)",
			index:         "i",
			prefixLen:     1,
			neededColumns: intsets.MakeFast(0),
			containsNull:  false,
			canSplit:      true,
		},
		{
			sql:           "a INT, b INT, c INT, UNIQUE INDEX i (b), FAMILY (a), FAMILY (b), FAMILY (c)",
			index:         "i",
			prefixLen:     1,
			neededColumns: intsets.MakeFast(0),
			containsNull:  true,
			canSplit:      false,
		},
	}
	if _, err := sqlDB.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range tcs {
		t.Run(tc.sql, func(t *testing.T) {
			if _, err := sqlDB.Exec("DROP TABLE IF EXISTS t.t"); err != nil {
				t.Fatal(err)
			}
			sql := fmt.Sprintf("CREATE TABLE t.t (%s)", tc.sql)
			if _, err := sqlDB.Exec(sql); err != nil {
				t.Fatal(err)
			}
			desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "t", "t")
			idx, err := catalog.MustFindIndexByName(desc, tc.index)
			if err != nil {
				t.Fatal(err)
			}
			splitter := span.MakeSplitter(desc, idx, tc.neededColumns)
			if res := splitter.CanSplitSpanIntoFamilySpans(tc.prefixLen, tc.containsNull); res != tc.canSplit {
				t.Errorf("expected result to be %v, but found %v", tc.canSplit, res)
			}
		})
	}
}

func TestSpanSplitterFamilyIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	codec := srv.ApplicationLayer().Codec()
	tcs := []struct {
		sql           string
		index         string
		neededColumns intsets.Fast
		forDelete     bool
		forSideEffect bool
		familyIDs     []descpb.FamilyID
	}{
		{
			sql:           "a INT NOT NULL, PRIMARY KEY (a)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(0),
			familyIDs:     []descpb.FamilyID{0},
		},
		{
			sql:           "a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a), FAMILY (a), FAMILY (b)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(1),
			familyIDs:     []descpb.FamilyID{1},
		},
		{
			sql:           "a INT NOT NULL, b INT NULL, PRIMARY KEY (a), FAMILY (a), FAMILY (b)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(1),
			familyIDs:     []descpb.FamilyID(nil),
		},
		{
			sql:           "a INT NOT NULL, b INT NULL, c INT NULL, PRIMARY KEY (a), FAMILY (a, b, c)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(2),
			familyIDs:     []descpb.FamilyID{0},
		},
		{
			sql:           "a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), FAMILY (a, b), FAMILY (c, d)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(2, 3),
			familyIDs:     []descpb.FamilyID(nil),
		},
		{
			sql:           "a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, PRIMARY KEY (a, b), FAMILY (a, b), FAMILY (c)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(0, 1),
			familyIDs:     []descpb.FamilyID{0},
		},
		{
			sql:           "a INT NOT NULL, b INT NULL, c INT NOT NULL, PRIMARY KEY (a, b), FAMILY (a), FAMILY (b, c)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(0, 2),
			familyIDs:     []descpb.FamilyID{1},
		},
		{
			sql:           "a INT NOT NULL, b INT NULL, c INT NOT NULL, PRIMARY KEY (a, b), FAMILY (a, c), FAMILY (b)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(0, 2),
			familyIDs:     []descpb.FamilyID{0},
		},
		{
			sql:           "a INT NOT NULL, b INT NULL, c INT NOT NULL, d INT NOT NULL, PRIMARY KEY (a, b), FAMILY (a, b), FAMILY (c, d)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(0, 1, 2, 3),
			familyIDs:     []descpb.FamilyID{1},
		},
		{
			sql:           "a INT NOT NULL, b INT NULL, c INT NOT NULL, d INT NOT NULL, PRIMARY KEY (a, b), FAMILY (a, b), FAMILY (c, d)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(0, 1, 2, 3),
			forDelete:     true,
			familyIDs:     []descpb.FamilyID(nil),
		},
		{
			sql:           "a INT NOT NULL, b INT NULL, c INT NOT NULL, d INT NOT NULL, PRIMARY KEY (a, b), FAMILY (a, b), FAMILY (c, d)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(0, 1, 2, 3),
			forSideEffect: true,
			familyIDs:     []descpb.FamilyID(nil),
		},
		{
			sql:           "a INT NOT NULL, b INT NULL, c INT NOT NULL, d INT NOT NULL, e INT NULL, PRIMARY KEY (a, b), FAMILY (a, c), FAMILY (b, d), FAMILY (e)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(0, 3),
			familyIDs:     []descpb.FamilyID{1},
		},
		{
			sql:           "a INT NOT NULL, b INT NULL, c INT NOT NULL, d INT NOT NULL, e INT NULL, PRIMARY KEY (a, b), FAMILY (a, c), FAMILY (b, d), FAMILY (e)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(0, 3),
			forDelete:     true,
			familyIDs:     []descpb.FamilyID(nil),
		},
		{
			sql:           "a INT NOT NULL, b INT NULL, c INT NOT NULL, d INT NOT NULL, e INT NULL, PRIMARY KEY (a, b), FAMILY (a, c), FAMILY (b, d), FAMILY (e)",
			index:         "t_pkey",
			neededColumns: intsets.MakeFast(0, 3),
			forSideEffect: true,
			familyIDs:     []descpb.FamilyID{0, 1},
		},
	}
	if _, err := sqlDB.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range tcs {
		t.Run(tc.sql, func(t *testing.T) {
			if _, err := sqlDB.Exec("DROP TABLE IF EXISTS t.t"); err != nil {
				t.Fatal(err)
			}
			sql := fmt.Sprintf("CREATE TABLE t.t (%s)", tc.sql)
			if _, err := sqlDB.Exec(sql); err != nil {
				t.Fatal(err)
			}
			desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "t", "t")
			idx, err := catalog.MustFindIndexByName(desc, tc.index)
			if err != nil {
				t.Fatal(err)
			}
			splitter := span.MakeSplitterBase(desc, idx, tc.neededColumns, tc.forDelete, tc.forSideEffect)
			require.Equal(t, tc.familyIDs, splitter.FamilyIDs())
		})
	}
}
