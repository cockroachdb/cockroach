// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdecomp_test

import (
	"bufio"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestBuildDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
		defer tc.Stopper().Stop(ctx)
		s0 := tc.Server(0)
		tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		// We need to disable the declarative schema changer so that we don't end
		// up high-fiving ourselves here.
		tdb.Exec(t, `SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer = 'off'`)
		execCfg := s0.ExecutorConfig().(sql.ExecutorConfig)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return run(ctx, t, d, &execCfg, tdb)
		})
	})
}

func run(
	ctx context.Context,
	t *testing.T,
	d *datadriven.TestData,
	execCfg *sql.ExecutorConfig,
	tdb *sqlutils.SQLRunner,
) string {
	switch d.Cmd {
	case "setup":
		stmts, err := parser.Parse(d.Input)
		require.NoError(t, err)
		require.NotEmpty(t, stmts, "missing statement(s) for setup command")
		for _, stmt := range stmts {
			tdb.Exec(t, stmt.SQL)
		}
		return ""

	case "decompose":
		fields := strings.Fields(d.Input)
		require.Lenf(t, fields, 1, "'decompose' requires one simple name, invalid input: %s", d.Input)
		id := getDescriptorID(t, fields[0], tdb)
		var backRefs catalog.DescriptorIDSet
		m := make(map[scpb.Element]scpb.Status)
		visitor := func(status scpb.Status, element scpb.Element) {
			m[element] = status
		}
		require.NoError(t, sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
			lookupFn := func(id catid.DescID) catalog.Descriptor {
				ret, err := col.GetImmutableDescriptorByID(ctx, txn, id, tree.CommonLookupFlags{
					Required:       true,
					RequireMutable: false,
					AvoidLeased:    true,
					IncludeOffline: true,
					IncludeDropped: true,
					AvoidSynthetic: false,
				})
				require.NoError(t, err)
				return ret
			}
			desc := lookupFn(id)
			backRefs = scdecomp.WalkDescriptor(desc, lookupFn, visitor)
			return nil
		}))
		return marshalResult(t, m, backRefs)

	default:
		return fmt.Sprintf("unknown command: %s", d.Cmd)
	}
}

func getDescriptorID(t *testing.T, name string, tdb *sqlutils.SQLRunner) catid.DescID {
	const q = `SELECT max(id) FROM system.namespace WHERE name = $1 AND "parentID" <> 1`
	results := tdb.QueryStr(t, q, name)
	require.NotEmptyf(t, results, "no descriptor found with simple name %q", name)
	require.Lenf(t, results, 1, "multiple descriptors found with simple name %q", name)
	str := results[0][0]
	if str == "NULL" {
		t.Fatalf("no descriptor found with simple name %q", name)
	}
	idInt, err := strconv.Atoi(str)
	require.NoError(t, err)
	return catid.DescID(idInt)
}

func marshalResult(
	t *testing.T, m map[scpb.Element]scpb.Status, backRefs catalog.DescriptorIDSet,
) string {
	var b strings.Builder
	str := make(map[scpb.Element]string, len(m))
	rank := make(map[scpb.Element]int, len(m))
	elts := make([]scpb.Element, 0, len(m))
	for e := range m {
		{
			// Compute the struct field index of the element in the ElementProto
			// to sort the elements in order of appearance in that message.
			var ep scpb.ElementProto
			ep.SetValue(e)
			v := reflect.ValueOf(ep)
			for i := 0; i < v.NumField(); i++ {
				if !v.Field(i).IsNil() {
					rank[e] = i
					break
				}
			}
		}
		elts = append(elts, e)
		yaml, err := sctestutils.ProtoToYAML(e)
		require.NoError(t, err)
		str[e] = yaml
	}
	sort.Slice(elts, func(i, j int) bool {
		if d := rank[elts[i]] - rank[elts[j]]; d != 0 {
			return d < 0
		}
		return str[elts[i]] < str[elts[j]]
	})
	b.WriteString("BackReferencedIDs:\n")
	for _, id := range backRefs.Ordered() {
		b.WriteString(fmt.Sprintf("  - %d\n", id))
	}
	b.WriteString("ElementState:\n")
	for _, e := range elts {
		typeName := fmt.Sprintf("%T", e)
		b.WriteString(strings.ReplaceAll(typeName, "*scpb.", "- ") + ":\n")
		b.WriteString(indentText(str[e], "    "))
		b.WriteString(fmt.Sprintf("  Status: %s\n", m[e].String()))
	}
	return b.String()
}

// indentText indents text for formatting out marshaled data.
func indentText(input string, tab string) string {
	result := strings.Builder{}
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		result.WriteString(tab)
		result.WriteString(line)
		result.WriteString("\n")
	}
	return result.String()
}
