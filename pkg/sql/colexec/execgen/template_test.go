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
	arg := make([]templateParamInfo, 3)
	for i := range arg {
		arg[i] = templateParamInfo{
			field: &dst.Field{Type: dst.NewIdent("bool")},
		}
	}

	res := generateAllTemplateArgs(arg)

	if len(res) != 8 {
		t.Fail()
	}
}
