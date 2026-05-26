// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdcevent

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestProjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT, 
  b STRING, 
  c STRING,
  PRIMARY KEY (b, a)
)`)

	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	encDatums := makeEncDatumRow(tree.NewDInt(1), tree.NewDString("one"), tree.DNull)

	t.Run("row_was_deleted", func(t *testing.T) {
		input := TestingMakeEventRow(desc, 0, encDatums, true)
		p := MakeProjection(input.EventDescriptor)
		pr, err := p.Project(input)
		require.NoError(t, err)
		require.Equal(t, []string{"one", "1"}, slurpDatums(t, pr.ForEachKeyColumn()))
		require.Equal(t, []string(nil), slurpDatums(t, pr.ForEachColumn()))
	})

	t.Run("identity", func(t *testing.T) {
		input := TestingMakeEventRow(desc, 0, encDatums, false)
		p := MakeProjection(input.EventDescriptor)
		idx := 0
		require.NoError(t, input.ForEachColumn().Datum(func(d tree.Datum, col ResultColumn) error {
			p.AddValueColumn(col.Name, col.Typ)
			err := p.SetValueDatumAt(idx, d)
			idx++
			return err
		}))

		pr, err := p.Project(input)
		require.NoError(t, err)
		require.Equal(t, slurpDatums(t, input.ForEachKeyColumn()), slurpDatums(t, pr.ForEachKeyColumn()))
		require.Equal(t, slurpDatums(t, input.ForEachColumn()), slurpDatums(t, pr.ForEachColumn()))
	})

	t.Run("must_be_correct_type", func(t *testing.T) {
		input := TestingMakeEventRow(desc, 0, encDatums, false)
		p := MakeProjection(input.EventDescriptor)
		p.AddValueColumn("wrong_type", types.Int)
		require.Regexp(t, "expected type int", p.SetValueDatumAt(0, tree.NewDString("fail")))
		// But we allow NULL.
		require.NoError(t, p.SetValueDatumAt(0, tree.DNull))
	})

	t.Run("project_extra_column", func(t *testing.T) {
		input := TestingMakeEventRow(desc, 0, encDatums, false)
		p := MakeProjection(input.EventDescriptor)
		idx := 0
		require.NoError(t, input.ForEachColumn().Datum(func(d tree.Datum, col ResultColumn) error {
			p.AddValueColumn(col.Name, col.Typ)
			err := p.SetValueDatumAt(idx, d)
			idx++
			return err
		}))
		p.AddValueColumn("test", types.Int)
		require.NoError(t, p.SetValueDatumAt(idx, tree.NewDInt(5)))

		pr, err := p.Project(input)
		require.NoError(t, err)
		require.Equal(t, slurpDatums(t, input.ForEachKeyColumn()), slurpDatums(t, pr.ForEachKeyColumn()))
		expectValues := slurpDatums(t, input.ForEachColumn())
		expectValues = append(expectValues, "5")
		require.Equal(t, expectValues, slurpDatums(t, pr.ForEachColumn()))
	})
}

func makeEncDatumRow(datums ...tree.Datum) (row rowenc.EncDatumRow) {
	for _, d := range datums {
		row = append(row, rowenc.EncDatum{Datum: d})
	}
	return row
}
