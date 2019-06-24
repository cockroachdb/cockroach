// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rsg

import (
	"fmt"
	"testing"
)

const yaccExample = `
name:
  IDENT
| unreserved_keyword
| col_name_keyword

unreserved_keyword:
  ABORT
| ACTION
| ADD
| ADMIN

col_name_keyword:
  ANNOTATE_TYPE
| BETWEEN
| BIGINT
| BIT

column_name:         name

constraint_name:     name

column_def:
  column_name typename col_qual_list
  {
    tableDef, err := tree.NewColumnTableDef(tree.Name($1), $2.colType(), $3.colQuals())
    if err != nil {
      sqllex.Error(err.Error())
      return 1
    }
    $$.val = tableDef
  }

col_qual_list:
  col_qual_list col_qualification
  {
    $$.val = append($1.colQuals(), $2.colQual())
  }
| /* EMPTY */
  {
    $$.val = []tree.NamedColumnQualification(nil)
  }

col_qualification:
  CONSTRAINT constraint_name col_qualification_elem
  {
    $$.val = tree.NamedColumnQualification{Name: tree.Name($2), Qualification: $3.colQualElem()}
  }
| col_qualification_elem
  {
    $$.val = tree.NamedColumnQualification{Qualification: $1.colQualElem()}
  }

col_qualification_elem:
  NOT NULL
  {
    $$.val = tree.NotNullConstraint{}
  }
| NULL
  {
    $$.val = tree.NullConstraint{}
  }
| UNIQUE
  {
    $$.val = tree.UniqueConstraint{}
  }
| PRIMARY KEY
  {
    $$.val = tree.PrimaryKeyConstraint{}
  }
`

func getRSG(t *testing.T) *RSG {
	r, err := NewRSG(1, yaccExample, false)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestGenerate(t *testing.T) {
	tests := []struct {
		root        string
		depth       int
		repetitions int
		expected    []string
	}{
		{
			root:        "column_def",
			depth:       20,
			repetitions: 10,
			expected: []string{
				"BIT typename",
				"ANNOTATE_TYPE typename CONSTRAINT ADD PRIMARY KEY NULL",
				"ident typename PRIMARY KEY CONSTRAINT ident NULL",
				"BETWEEN typename NULL",
				"ADD typename",
				"ABORT typename",
				"ACTION typename",
				"BIGINT typename",
				"ident typename",
				"BETWEEN typename CONSTRAINT ident UNIQUE",
			},
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s-%d-%d", tc.root, tc.depth, tc.repetitions), func(t *testing.T) {
			r := getRSG(t)

			out := make([]string, tc.repetitions)
			for i := range out {
				out[i] = r.Generate(tc.root, tc.depth)
			}

			// Enable to help with writing tests.
			if false {
				for _, o := range out {
					fmt.Printf("%q,\n", o)
				}
				return
			}

			if len(out) != len(tc.expected) {
				t.Fatal("unexpected")
			}
			for i, o := range out {
				if o != tc.expected[i] {
					t.Fatalf("got %q, expected %q", o, tc.expected[i])
				}
			}
		})
	}
}
