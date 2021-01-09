// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package funcdesc

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestParseUserDefinedFuncDef(t *testing.T) {
	tests := []struct {
		formalParams []string
		funcdef      string
		want         tree.Expr
		wantErr      bool
	}{
		{
			formalParams: []string{"a"},
			funcdef:      "SELECT a",
			want:         &tree.UnresolvedName{NumParts: 1, Parts: [4]string{"a"}},
		},
		{
			formalParams: []string{"a", "b"},
			funcdef:      "SELECT a+b",
			want: &tree.BinaryExpr{Operator: tree.Plus,
				Left:  &tree.UnresolvedName{NumParts: 1, Parts: [4]string{"a"}},
				Right: &tree.UnresolvedName{NumParts: 1, Parts: [4]string{"b"}},
			},
		},
		{
			// No non-selects allowed
			funcdef: "INSERT INTO t VALUES(3)",
			wantErr: true,
		},
		{
			// No limits allowed
			funcdef: "SELECT 1 LIMIT 1",
			wantErr: true,
		},
		{
			// No distinct allowed
			funcdef: "SELECT DISTINCT 1 LIMIT 1",
			wantErr: true,
		},
		{
			// No order by allowed
			funcdef: "SELECT 1 ORDER BY 1",
			wantErr: true,
		},
		{
			// No with allowed
			funcdef: "WITH x as (SELECT 1) SELECT 1",
			wantErr: true,
		},
		{
			// No group by allowed
			funcdef: "SELECT 1 GROUP BY 1",
			wantErr: true,
		},
		// TODO(jordan): this case should not work! We need to validate that the
		// scalar expression has no unbound references.
		// {
		// 	formalParams: []string{"a", "b"},
		// 	funcdef:      "SELECT c",
		// 	wantErr:      true,
		// },
	}
	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			_, got, err := ParseUserDefinedFuncDef(tt.funcdef)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUserDefinedFuncDef() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseUserDefinedFuncDef() got = %v, want %v", got, tt.want)
			}
		})
	}
}
