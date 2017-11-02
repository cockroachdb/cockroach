// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"golang.org/x/net/context"
)

func TestGenerateSubzoneSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	prettyTrimmedKey := func(key []byte) string {
		untrimmedKey := append(keys.MakeTablePrefix(0), key...)
		return strings.TrimPrefix(roachpb.Key(untrimmedKey).String(), "/Table/0")
	}

	tests := []struct {
		name   string
		schema string

		// subzones are 1:1 to the `subzones` input of GenerateSubzoneSpans,
		// each formatted as `@index_name` or `.partition_name`
		subzones []string

		// expected are 1:1 to the output of GenerateSubzoneSpans, each
		// formatted as `{subzone} {start}-{end}` (e.g. `@primary /1-/2`), where
		// {subzone} is formatted identically to the test shorthand above, and
		// {start} and {end} are formatted using our key pretty printer, but
		// with the table removed. The end key is always specified in tests
		// (though GenerateSubzoneSpans may omit it to save space).
		expected []string
	}{
		{
			name:   `no subzones`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY)`,
		},

		{
			name:     `all indexes`,
			schema:   `CREATE TABLE t (a INT PRIMARY KEY, b INT, INDEX idx1 (b), INDEX idx2 (b))`,
			subzones: []string{`@primary`, `@idx1`, `@idx2`},
			expected: []string{`@primary /1-/2`, `@idx1 /2-/3`, `@idx2 /3-/4`},
		},
		{
			name:     `all indexes - shuffled`,
			schema:   `CREATE TABLE t (a INT PRIMARY KEY, b INT, INDEX idx1 (b), INDEX idx2 (b))`,
			subzones: []string{`@idx2`, `@primary`, `@idx1`},
			expected: []string{`@primary /1-/2`, `@idx1 /2-/3`, `@idx2 /3-/4`},
		},
		{
			name:     `some indexes`,
			schema:   `CREATE TABLE t (a INT PRIMARY KEY, b INT, INDEX idx1 (b), INDEX idx2 (b))`,
			subzones: []string{`@primary`, `@idx2`},
			expected: []string{`@primary /1-/2`, `@idx2 /3-/4`},
		},

		{
			name: `single col list partitioning`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p3 VALUES (3),
				PARTITION p4 VALUES (4)
			)`,
			subzones: []string{`@primary`, `.p3`, `.p4`},
			expected: []string{
				`@primary /1-/1/3`,
				`     .p3 /1/3-/1/4`,
				`     .p4 /1/4-/1/5`,
				`@primary /1/5-/2`,
			},
		},
		{
			name: `single col list partitioning - DEFAULT`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p3 VALUES (3),
				PARTITION p4 VALUES (4),
				PARTITION pd VALUES (DEFAULT)
			)`,
			subzones: []string{`@primary`, `.p3`, `.p4`, `.pd`},
			expected: []string{
				`.pd /1-/1/3`,
				`.p3 /1/3-/1/4`,
				`.p4 /1/4-/1/5`,
				`.pd /1/5-/2`,
			},
		},
		{
			name: `multi col list partitioning`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES (3, 4),
				PARTITION p56 VALUES (5, 6),
				PARTITION p57 VALUES (5, 7)
			)`,
			subzones: []string{`@primary`, `.p34`, `.p56`, `.p57`},
			expected: []string{
				`@primary /1-/1/3/4`,
				`    .p34 /1/3/4-/1/3/5`,
				`@primary /1/3/5-/1/5/6`,
				`    .p56 /1/5/6-/1/5/7`,
				`    .p57 /1/5/7-/1/5/8`,
				`@primary /1/5/8-/2`,
			},
		},
		{
			name: `multi col list partitioning - DEFAULT`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES (3, 4),
				PARTITION p56 VALUES (5, 6),
				PARTITION p57 VALUES (5, 7),
				PARTITION p5d VALUES (5, DEFAULT),
				PARTITION pd VALUES (DEFAULT, DEFAULT)
			)`,
			subzones: []string{`@primary`, `.p34`, `.p56`, `.p57`, `.p5d`, `.pd`},
			expected: []string{
				` .pd /1-/1/3/4`,
				`.p34 /1/3/4-/1/3/5`,
				` .pd /1/3/5-/1/5`,
				`.p5d /1/5-/1/5/6`,
				`.p56 /1/5/6-/1/5/7`,
				`.p57 /1/5/7-/1/5/8`,
				`.p5d /1/5/8-/1/6`,
				` .pd /1/6-/2`,
			},
		},

		{
			name: `single col range partitioning`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY RANGE (a) (
				PARTITION p3 VALUES LESS THAN (3),
				PARTITION p4 VALUES LESS THAN (4)
			)`,
			subzones: []string{`@primary`, `.p3`, `.p4`},
			expected: []string{
				`     .p3 /1-/1/3`,
				`     .p4 /1/3-/1/4`,
				`@primary /1/4-/2`,
			},
		},
		{
			name: `single col range partitioning - MAXVALUE`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY RANGE (a) (
				PARTITION p3 VALUES LESS THAN (3),
				PARTITION p4 VALUES LESS THAN (4),
				PARTITION pm VALUES LESS THAN (MAXVALUE)
			)`,
			subzones: []string{`@primary`, `.p3`, `.p4`, `.pm`},
			expected: []string{
				`.p3 /1-/1/3`,
				`.p4 /1/3-/1/4`,
				`.pm /1/4-/2`,
			},
		},
		{
			name: `multi col range partitioning`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34 VALUES LESS THAN (3, 4),
				PARTITION p56 VALUES LESS THAN (5, 6),
				PARTITION p57 VALUES LESS THAN (5, 7)
			)`,
			subzones: []string{`@primary`, `.p34`, `.p56`, `.p57`},
			expected: []string{
				`    .p34 /1-/1/3/4`,
				`    .p56 /1/3/4-/1/5/6`,
				`    .p57 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
		},
		{
			name: `multi col range partitioning - MAXVALUE`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34 VALUES LESS THAN (3, 4),
				PARTITION p3m VALUES LESS THAN (3, MAXVALUE),
				PARTITION p56 VALUES LESS THAN (5, 6),
				PARTITION p57 VALUES LESS THAN (5, 7),
				PARTITION pm VALUES LESS THAN (MAXVALUE, MAXVALUE)
			)`,
			subzones: []string{`@primary`, `.p34`, `.p3m`, `.p56`, `.p57`, `.pm`},
			expected: []string{
				`.p34 /1-/1/3/4`,
				`.p3m /1/3/4-/1/4`,
				`.p56 /1/4-/1/5/6`,
				`.p57 /1/5/6-/1/5/7`,
				` .pm /1/5/7-/2`,
			},
		},

		{
			name: `list-list partitioning`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
				PARTITION p3 VALUES (3) PARTITION BY LIST (b) (
					PARTITION p34 VALUES (4)
				),
				PARTITION p5 VALUES (5) PARTITION BY LIST (b) (
					PARTITION p56 VALUES (6),
					PARTITION p5d VALUES (DEFAULT)
				),
				PARTITION pd VALUES (DEFAULT)
			)`,
			subzones: []string{`@primary`, `.p3`, `.p34`, `.p5`, `.p56`, `.p5d`, `.pd`},
			expected: []string{
				` .pd /1-/1/3`,
				` .p3 /1/3-/1/3/4`,
				`.p34 /1/3/4-/1/3/5`,
				` .p3 /1/3/5-/1/4`,
				` .pd /1/4-/1/5`,
				`.p5d /1/5-/1/5/6`,
				`.p56 /1/5/6-/1/5/7`,
				`.p5d /1/5/7-/1/6`,
				` .pd /1/6-/2`,
			},
		},
		{
			name: `list-range partitioning`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
				PARTITION p3 VALUES (3) PARTITION BY RANGE (b) (
					PARTITION p34 VALUES LESS THAN (4)
				),
				PARTITION p5 VALUES (5) PARTITION BY RANGE (b) (
					PARTITION p56 VALUES LESS THAN (6),
					PARTITION p5d VALUES LESS THAN (MAXVALUE)
				),
				PARTITION pd VALUES (DEFAULT)
			)`,
			subzones: []string{`@primary`, `.p3`, `.p34`, `.p5`, `.p56`, `.p5d`, `.pd`},
			expected: []string{
				` .pd /1-/1/3`,
				`.p34 /1/3-/1/3/4`,
				` .p3 /1/3/4-/1/4`,
				` .pd /1/4-/1/5`,
				`.p56 /1/5-/1/5/6`,
				`.p5d /1/5/6-/1/6`,
				` .pd /1/6-/2`,
			},
		},

		{
			name: `inheritance - index`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION pd VALUES (DEFAULT)
			)`,
			subzones: []string{`@primary`},
			expected: []string{`@primary /1-/2`},
		},
		{
			name: `inheritance - single col default`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p3 VALUES (3),
				PARTITION pd VALUES (DEFAULT)
			)`,
			subzones: []string{`@primary`, `.pd`},
			expected: []string{`.pd /1-/2`},
		},
		{
			name: `inheritance - multi col default`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES (3, 4),
				PARTITION p3d VALUES (3, DEFAULT),
				PARTITION p56 VALUES (5, 6),
				PARTITION p5d VALUES (5, DEFAULT)
			)`,
			subzones: []string{`@primary`, `.p3d`, `.p56`},
			expected: []string{
				`@primary /1-/1/3`,
				`    .p3d /1/3-/1/4`,
				`@primary /1/4-/1/5/6`,
				`    .p56 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
		},
		{
			name: `inheritance - subpartitioning`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
				PARTITION p3 VALUES (3) PARTITION BY LIST (b) (
					PARTITION p34 VALUES (4),
					PARTITION p3d VALUES (DEFAULT)
				),
				PARTITION p5 VALUES (5) PARTITION BY LIST (b) (
					PARTITION p56 VALUES (6),
					PARTITION p5d VALUES (DEFAULT)
				),
				PARTITION p7 VALUES (7) PARTITION BY LIST (b) (
					PARTITION p78 VALUES (8),
					PARTITION p7d VALUES (DEFAULT)
				)
			)`,
			subzones: []string{`@primary`, `.p3d`, `.p56`, `.p7`},
			expected: []string{
				`@primary /1-/1/3`,
				`    .p3d /1/3-/1/4`,
				`@primary /1/4-/1/5/6`,
				`    .p56 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/1/7`,
				`     .p7 /1/7-/1/8`,
				`@primary /1/8-/2`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()

			var tableDesc *sqlbase.TableDescriptor
			{
				stmt, err := parser.ParseOne(test.schema)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				createTable, ok := stmt.(*parser.CreateTable)
				if !ok {
					t.Fatalf("expected *parser.CreateTable got %T", stmt)
				}
				const parentID, tableID = keys.MaxReservedDescID + 1, keys.MaxReservedDescID + 2
				tableDesc, err = makeCSVTableDescriptor(
					ctx, createTable, parentID, tableID, hlc.UnixNano())
				if err != nil {
					t.Fatalf("%+v", err)
				}
				if err := tableDesc.ValidateTable(); err != nil {
					t.Fatalf("%+v", err)
				}
			}

			var subzones []config.Subzone
			for _, subzoneShort := range test.subzones {
				if strings.HasPrefix(subzoneShort, "@") {
					idxDesc, _, err := tableDesc.FindIndexByName(subzoneShort[1:])
					if err != nil {
						log.Info(ctx, tableDesc)
						t.Fatalf("could not find index %s: %+v", subzoneShort, err)
					}
					subzones = append(subzones, config.Subzone{IndexID: uint32(idxDesc.ID)})
				} else if strings.HasPrefix(subzoneShort, ".") {
					// TODO(dan): decide if config.Subzone needs to have IndexID
					// set when PartitionName is non-empty. The proto comment
					// doesn't specify.
					subzones = append(subzones, config.Subzone{PartitionName: subzoneShort[1:]})
				}
			}
			spans, err := GenerateSubzoneSpans(tableDesc, subzones)
			if err != nil {
				t.Fatalf("generating subzone spans: %+v", err)
			}

			var actual []string
			for _, span := range spans {
				// Verify that we're always doing the space savings when we can.
				if span.Key.PrefixEnd().Equal(span.EndKey) {
					t.Errorf("endKey should be omitted when equal to key.PrefixEnd [%s, %s)",
						prettyTrimmedKey(span.Key), prettyTrimmedKey(span.EndKey))
				}
				if len(span.EndKey) == 0 {
					span.EndKey = span.Key.PrefixEnd()
				}

				// TODO(dan): Check that spans are sorted.

				var subzoneShort string
				if subzone := subzones[span.SubzoneIndex]; len(subzone.PartitionName) > 0 {
					subzoneShort = "." + subzone.PartitionName
				} else {
					idxDesc, err := tableDesc.FindIndexByID(sqlbase.IndexID(subzone.IndexID))
					if err != nil {
						t.Fatalf("could not find index with ID %d: %+v", subzone.IndexID, err)
					}
					subzoneShort = "@" + idxDesc.Name
				}
				actual = append(actual, fmt.Sprintf("%s %s-%s", subzoneShort,
					prettyTrimmedKey(span.Key), prettyTrimmedKey(span.EndKey),
				))
			}

			if len(actual) != len(test.expected) {
				t.Fatalf("got \n    %v\n expected \n    %v", actual, test.expected)
			}
			for i := range actual {
				if expected := strings.TrimSpace(test.expected[i]); actual[i] != expected {
					t.Errorf("%d: got [%s] expected [%s]", i, actual[i], expected)
				}
			}
		})
	}
}
