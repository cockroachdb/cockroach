// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
	"testing"
)

func TestStarDatum(t *testing.T) {
	typedExpr := StarDatumInstance
	// Test formatting using the indexed var format interceptor.
	var buf bytes.Buffer
	typedExpr.Format(
		&buf,
		FmtStarDatumFormat(FmtSimple, func(buf *bytes.Buffer, _ FmtFlags) {
			fmt.Fprintf(buf, "STAR")
		}),
	)
	str := buf.String()
	expectedStr := "STAR"
	if str != expectedStr {
		t.Errorf("invalid expression string '%s', expected '%s'", str, expectedStr)
	}
}
