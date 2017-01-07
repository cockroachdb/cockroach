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

	"strings"

	"github.com/pkg/errors"
)

var (
	initErr = errors.Errorf("err")
	code    = "abc"
	detail  = "def"
	hint    = "ghi"
	wrap    = "jkl"
)

func TestWithPGCodeNil(t *testing.T) {
	got := WithPGCode(nil, "")
	if got != nil {
		t.Errorf("pgerror.WithPGCode(nil, \"\"): got %#v, expected nil", got)
	}
}

func TestWithPGCodeEmptyCode(t *testing.T) {
	got := WithPGCode(initErr, "")
	if got != initErr {
		t.Errorf("pgerror.WithPGCode(initErr, \"\"): got %#v, expected %#v", got, initErr)
	}
}

func TestWithDetailNil(t *testing.T) {
	got := WithDetail(nil, "")
	if got != nil {
		t.Errorf("pgerror.WithDetail(nil, \"\"): got %#v, expected nil", got)
	}
}

func TestWithDetailEmptyDetail(t *testing.T) {
	got := WithDetail(initErr, "")
	if got != initErr {
		t.Errorf("pgerror.WithDetail(initErr, \"\"): got %#v, expected %#v", got, initErr)
	}
}

func TestWithHintNil(t *testing.T) {
	got := WithHint(nil, "")
	if got != nil {
		t.Errorf("pgerror.WithHint(nil, \"\"): got %#v, expected nil", got)
	}
}

func TestWithHintEmptyHint(t *testing.T) {
	got := WithHint(initErr, "")
	if got != initErr {
		t.Errorf("pgerror.WithHint(initErr, \"\"): got %#v, expected %#v", got, initErr)
	}
}

func TestWithSourceContextNil(t *testing.T) {
	got := WithSourceContext(nil, 0)
	if got != nil {
		t.Errorf("pgerror.WithSourceContext(nil, \"\"): got %#v, expected nil", got)
	}
}

func TestCause(t *testing.T) {
	tests := []error{
		WithPGCode(initErr, code),
		WithDetail(initErr, detail),
		WithHint(initErr, hint),
		WithSourceContext(initErr, 0),
		WithDetail(WithPGCode(initErr, code), detail),
		WithPGCode(WithDetail(initErr, detail), code),
		WithSourceContext(WithHint(initErr, hint), 0),
		WithHint(WithSourceContext(initErr, 0), hint),
		WithSourceContext(WithHint(WithDetail(WithPGCode(initErr, code), detail), hint), 0),
		WithPGCode(WithDetail(WithHint(WithSourceContext(initErr, 0), hint), detail), code),
		WithPGCode(WithDetail(errors.Wrap(WithHint(WithSourceContext(initErr, 0), hint), wrap), detail), code),
		errors.Wrap(WithPGCode(WithDetail(WithHint(WithSourceContext(initErr, 0), hint), detail), code), wrap),
	}
	for i, tc := range tests {
		got := errors.Cause(tc)
		if got != initErr {
			t.Errorf("%d: errors.Cause(%#v): got %#v, expected %#v", i, tc, got, initErr)
		}
	}
}

func TestPGCode(t *testing.T) {
	tests := []struct {
		err     error
		hasCode bool
	}{
		{WithPGCode(initErr, code), true},
		{WithDetail(initErr, detail), false},
		{WithHint(initErr, hint), false},
		{WithSourceContext(initErr, 0), false},
		{WithDetail(WithPGCode(initErr, code), detail), true},
		{WithPGCode(WithDetail(initErr, detail), code), true},
		{WithSourceContext(WithHint(initErr, hint), 0), false},
		{WithHint(WithSourceContext(initErr, 0), hint), false},
		{WithSourceContext(WithHint(WithDetail(WithPGCode(initErr, code), detail), hint), 0), true},
		{WithPGCode(WithDetail(WithHint(WithSourceContext(initErr, 0), hint), detail), code), true},
		{WithSourceContext(WithHint(errors.Wrap(WithDetail(WithPGCode(initErr, code), detail), wrap), hint), 0), true},
		{errors.Wrap(WithPGCode(WithDetail(WithHint(WithSourceContext(initErr, 0), hint), detail), code), wrap), true},
	}
	for i, tc := range tests {
		gotCodeStr, gotCode := PGCode(tc.err)
		switch {
		case gotCode && tc.hasCode:
			// verify code
			if gotCodeStr != code {
				t.Errorf("%d: pgerror.PGCode(%#v): got code %q, expected code %q", i, tc.err, gotCodeStr, code)
			}
		case gotCode && !tc.hasCode:
			t.Errorf("%d: pgerror.PGCode(%#v): got code %q, expected no code", i, tc.err, gotCodeStr)
		case !gotCode && tc.hasCode:
			t.Errorf("%d: pgerror.PGCode(%#v): got no code, expected code %q", i, tc.err, code)
		case !gotCode && !tc.hasCode:
			// no code to verify
		default:
			panic("unhandled case")
		}
	}
}

func TestDetail(t *testing.T) {
	tests := []struct {
		err       error
		hasDetail bool
	}{
		{WithPGCode(initErr, code), false},
		{WithDetail(initErr, detail), true},
		{WithHint(initErr, hint), false},
		{WithSourceContext(initErr, 0), false},
		{WithDetail(WithPGCode(initErr, code), detail), true},
		{WithPGCode(WithDetail(initErr, detail), code), true},
		{WithSourceContext(WithHint(initErr, hint), 0), false},
		{WithHint(WithSourceContext(initErr, 0), hint), false},
		{WithSourceContext(WithHint(WithDetail(WithPGCode(initErr, code), detail), hint), 0), true},
		{WithPGCode(WithDetail(WithHint(WithSourceContext(initErr, 0), hint), detail), code), true},
		{WithSourceContext(WithHint(errors.Wrap(WithDetail(WithPGCode(initErr, code), detail), wrap), hint), 0), true},
		{errors.Wrap(WithPGCode(WithDetail(WithHint(WithSourceContext(initErr, 0), hint), detail), code), wrap), true},
	}
	for i, tc := range tests {
		gotDetailStr, gotDetail := Detail(tc.err)
		switch {
		case gotDetail && tc.hasDetail:
			// verify detail
			if gotDetailStr != detail {
				t.Errorf("%d: pgerror.Detail(%#v): got detail %q, expected detail %q", i, tc.err, gotDetailStr, detail)
			}
		case gotDetail && !tc.hasDetail:
			t.Errorf("%d: pgerror.Detail(%#v): got detail %q, expected no detail", i, tc.err, gotDetailStr)
		case !gotDetail && tc.hasDetail:
			t.Errorf("%d: pgerror.Detail(%#v): got no detail, expected detail %q", i, tc.err, detail)
		case !gotDetail && !tc.hasDetail:
			// no detail to verify
		default:
			panic("unhandled case")
		}
	}
}

func TestHint(t *testing.T) {
	tests := []struct {
		err     error
		hasHint bool
	}{
		{WithPGCode(initErr, code), false},
		{WithDetail(initErr, detail), false},
		{WithHint(initErr, hint), true},
		{WithSourceContext(initErr, 0), false},
		{WithDetail(WithPGCode(initErr, code), detail), false},
		{WithPGCode(WithDetail(initErr, detail), code), false},
		{WithSourceContext(WithHint(initErr, hint), 0), true},
		{WithHint(WithSourceContext(initErr, 0), hint), true},
		{WithSourceContext(WithHint(WithDetail(WithPGCode(initErr, code), detail), hint), 0), true},
		{WithPGCode(WithDetail(WithHint(WithSourceContext(initErr, 0), hint), detail), code), true},
		{WithSourceContext(WithHint(errors.Wrap(WithDetail(WithPGCode(initErr, code), detail), wrap), hint), 0), true},
		{errors.Wrap(WithPGCode(WithDetail(WithHint(WithSourceContext(initErr, 0), hint), detail), code), wrap), true},
	}
	for i, tc := range tests {
		gotHintStr, gotHint := Hint(tc.err)
		switch {
		case gotHint && tc.hasHint:
			// verify hint
			if gotHintStr != hint {
				t.Errorf("%d: pgerror.Hint(%#v): got hint %q, expected hint %q", i, tc.err, gotHintStr, hint)
			}
		case gotHint && !tc.hasHint:
			t.Errorf("%d: pgerror.Hint(%#v): got hint %q, expected no hint", i, tc.err, gotHintStr)
		case !gotHint && tc.hasHint:
			t.Errorf("%d: pgerror.Hint(%#v): got no hint, expected hint %q", i, tc.err, hint)
		case !gotHint && !tc.hasHint:
			// no hint to verify
		default:
			panic("unhandled case")
		}
	}
}

func TestSourceContext(t *testing.T) {
	tests := []struct {
		err       error
		hasSrcCtx bool
	}{
		{WithPGCode(initErr, code), false},
		{WithDetail(initErr, detail), false},
		{WithHint(initErr, hint), false},
		{WithSourceContext(initErr, 0), true},
		{WithDetail(WithPGCode(initErr, code), detail), false},
		{WithPGCode(WithDetail(initErr, detail), code), false},
		{WithSourceContext(WithHint(initErr, hint), 0), true},
		{WithHint(WithSourceContext(initErr, 0), hint), true},
		{WithSourceContext(WithHint(errors.Wrap(WithDetail(WithPGCode(initErr, code), detail), wrap), hint), 0), true},
		{errors.Wrap(WithPGCode(WithDetail(WithHint(WithSourceContext(initErr, 0), hint), detail), code), wrap), true},
	}
	for i, tc := range tests {
		gotSrcCtxObj, gotSrcCtx := SourceContext(tc.err)
		switch {
		case gotSrcCtx && tc.hasSrcCtx:
			// verify SourceContext
			const srcCtxFileSuffix = "pgerror/errors_test.go"
			const srcCtxFunc = "TestSourceContext"

			if a, e := gotSrcCtxObj.File, srcCtxFileSuffix; !strings.HasSuffix(a, e) {
				t.Errorf("%d: pgerror.SourceContext(%#v): got SrcCtx.File %q, expected file with suffix %q", i, tc.err, a, e)
			}
			if a, e := gotSrcCtxObj.Function, srcCtxFunc; a != e {
				t.Errorf("%d: pgerror.SourceContext(%#v): got SrcCtx.Function %q, expected function %q", i, tc.err, a, e)
			}
		case gotSrcCtx && !tc.hasSrcCtx:
			t.Errorf("%d: pgerror.SourceContext(%#v): got SrcCtx %q, expected no SrcCtx", i, tc.err, gotSrcCtxObj)
		case !gotSrcCtx && tc.hasSrcCtx:
			t.Errorf("%d: pgerror.SourceContext(%#v): got no SrcCtx, expected SrcCtx", i, tc.err)
		case !gotSrcCtx && !tc.hasSrcCtx:
			// no SourceContext to verify
		default:
			panic("unhandled case")
		}
	}
}

func TestFormatPgError(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{WithPGCode(initErr, code),
			`err
.*sql/pgwire/pgerror.*
code: abc`},
		{WithDetail(initErr, detail),
			`err
.*sql/pgwire/pgerror.*
detail: def`},
		{WithHint(initErr, hint),
			`err
.*sql/pgwire/pgerror.*
hint: ghi`},
		{WithSourceContext(initErr, 0),
			`err
.*sql/pgwire/pgerror.*
location: TestFormatPgError, .*/pgerror/errors_test.go:\d*`},
		{WithDetail(WithPGCode(initErr, code), detail),
			`err
.*sql/pgwire/pgerror.*
code: abc
detail: def`},
		{WithPGCode(WithDetail(initErr, detail), code),
			`err
.*sql/pgwire/pgerror.*
detail: def
code: abc`},
		{WithSourceContext(WithHint(initErr, hint), 0),
			`err
.*sql/pgwire/pgerror.*
hint: ghi
location: TestFormatPgError, .*/pgerror/errors_test.go:\d*`},
		{WithHint(WithSourceContext(initErr, 0), hint),
			`err
.*sql/pgwire/pgerror.*
location: TestFormatPgError, .*/pgerror/errors_test.go:\d*
hint: ghi`},
		{WithSourceContext(WithHint(WithDetail(WithPGCode(initErr, code), detail), hint), 0),
			`err
.*sql/pgwire/pgerror.*
code: abc
detail: def
hint: ghi
location: TestFormatPgError, .*/pgerror/errors_test.go:\d*`},
		{WithPGCode(WithDetail(WithHint(WithSourceContext(initErr, 0), hint), detail), code),
			`err
.*sql/pgwire/pgerror.*
location: TestFormatPgError, .*/pgerror/errors_test.go:\d*
hint: ghi
detail: def
code: abc`},
		{WithSourceContext(WithHint(errors.Wrap(WithDetail(WithPGCode(initErr, code), detail), wrap), hint), 0),
			`err
.*sql/pgwire/pgerror.*
code: abc
detail: def
jkl
.*sql/pgwire/pgerror.*
hint: ghi
location: TestFormatPgError, .*/pgerror/errors_test.go:\d*`},
		{errors.Wrap(WithPGCode(WithDetail(WithHint(WithSourceContext(initErr, 0), hint), detail), code), wrap),
			`err
.*sql/pgwire/pgerror.*
location: TestFormatPgError, .*/pgerror/errors_test.go:\d*
hint: ghi
detail: def
code: abc
jkl
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
