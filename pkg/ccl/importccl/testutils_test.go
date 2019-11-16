// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func descForTable(
	t *testing.T, create string, parent, id sqlbase.ID, fks fkHandler,
) *sqlbase.TableDescriptor {
	t.Helper()
	parsed, err := parser.Parse(create)
	if err != nil {
		t.Fatalf("could not parse %q: %v", create, err)
	}
	nanos := testEvalCtx.StmtTimestamp.UnixNano()

	settings := testEvalCtx.Settings

	var stmt *tree.CreateTable

	if len(parsed) == 2 {
		stmt = parsed[1].AST.(*tree.CreateTable)
		name := parsed[0].AST.(*tree.CreateSequence).Name.String()

		ts := hlc.Timestamp{WallTime: nanos}
		priv := sqlbase.NewDefaultPrivilegeDescriptor()
		desc, err := sql.MakeSequenceTableDesc(
			name, tree.SequenceOptions{}, parent, id-1, ts, priv, nil, /* params */
		)
		if err != nil {
			t.Fatal(err)
		}
		fks.resolver[name] = &desc
	} else {
		stmt = parsed[0].AST.(*tree.CreateTable)
	}
	table, err := MakeSimpleTableDescriptor(context.TODO(), settings, stmt, parent, id, fks, nanos)
	if err != nil {
		t.Fatalf("could not interpret %q: %v", create, err)
	}
	if err := fixDescriptorFKState(table.TableDesc()); err != nil {
		t.Fatal(err)
	}
	return table.TableDesc()
}

var testEvalCtx = &tree.EvalContext{
	SessionData: &sessiondata.SessionData{
		DataConversion: sessiondata.DataConversionConfig{Location: time.UTC},
	},
	StmtTimestamp: timeutil.Unix(100000000, 0),
	Settings:      cluster.MakeTestingClusterSettings(),
}
