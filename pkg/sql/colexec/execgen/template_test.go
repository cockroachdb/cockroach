// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execgen

import (
	"testing"

	"github.com/dave/dst"
)

func TestGenerateAllTemplateArgs(t *testing.T) {
	params := make([]templateParamInfo, 3)
	for i := range params {
		params[i] = templateParamInfo{
			field: &dst.Field{Type: dst.NewIdent("bool")},
		}
	}

	res := generateAllTemplateArgs(params)

	argsMap := map[string]struct{}{}
	for _, args := range res {
		argsStr := prettyPrintExprs(args...)
		if _, ok := argsMap[argsStr]; ok {
			t.Fatalf("duplicate template args %s", argsStr)
		}
		argsMap[argsStr] = struct{}{}
	}

	if len(res) != 8 {
		t.Fatalf("wrong number of template arg set, expected 8, found %d", len(res))
	}
}
