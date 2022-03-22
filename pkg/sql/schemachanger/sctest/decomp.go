// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctest

import (
	"bufio"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// DecomposeToElements exercises the descriptor-to-element decomposition
// functionality in the form of a data-driven test.
func DecomposeToElements(t *testing.T, dir string, newCluster NewClusterFunc) {
	skip.UnderRace(t)
	skip.UnderStress(t)
	ctx := context.Background()
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		// Create a test cluster.
		db, cleanup := newCluster(t, nil /* knobs */)
		tdb := sqlutils.MakeSQLRunner(db)
		defer cleanup()
		// We need to disable the declarative schema changer so that we don't end
		// up high-fiving ourselves here.
		tdb.Exec(t, `SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer = 'off'`)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return runDecomposeTest(ctx, t, d, tdb)
		})
	})
}

func runDecomposeTest(
	ctx context.Context, t *testing.T, d *datadriven.TestData, tdb *sqlutils.SQLRunner,
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
		name := fields[0]
		var desc catalog.Descriptor
		allDescs := sctestdeps.ReadDescriptorsFromDB(ctx, t, tdb)
		_ = allDescs.ForEachDescriptorEntry(func(d catalog.Descriptor) error {
			if d.GetName() == name {
				desc = d
			}
			return nil
		})
		require.NotNilf(t, desc, "descriptor with name %q not found", name)
		m := make(map[scpb.Element]scpb.Status)
		visitor := func(status scpb.Status, element scpb.Element) {
			m[element] = status
		}
		backRefs := scdecomp.WalkDescriptor(desc, allDescs.LookupDescriptorEntry, visitor)
		return marshalResult(t, m, backRefs)

	default:
		return fmt.Sprintf("unknown command: %s", d.Cmd)
	}
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
