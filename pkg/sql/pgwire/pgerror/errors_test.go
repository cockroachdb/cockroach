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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package pgerror

import (
	"fmt"
	"regexp"
	"testing"
)

var (
	initErr = "err"
	code    = "abc"
	detail  = "def"
	hint    = "ghi"
)

func TestFormatPgError(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{NewError(code, initErr),
			`err
code: abc
.*sql/pgwire/pgerror.*`},
		{WithDetail(NewError(code, initErr), detail),
			`err
code: abc
detail: def
.*sql/pgwire/pgerror.*`},
		{WithHint(NewError(code, initErr), hint),
			`err
code: abc
hint: ghi
.*sql/pgwire/pgerror.*`},
		{WithDetail(NewError(code, initErr), detail),
			`err
code: abc
detail: def
.*sql/pgwire/pgerror.*`},
		{WithDetail(WithHint(NewError(code, initErr), hint), detail),
			`err
code: abc
detail: def
hint: ghi
.*sql/pgwire/pgerror.*`},
	}
	for i, tc := range tests {
		got := fmt.Sprintf("%+v", tc.err)

		match, err := regexp.MatchString("(?s)"+tc.want, got)
		if err != nil {
			t.Fatal(err)
		}
		if !match {
			t.Errorf("%d: fmt.Sprintf(%%+v, err):\n got: %q\nwant: %q", i, got, tc.want)
		}
	}
}
