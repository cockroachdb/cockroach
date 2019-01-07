// Copyright 2017 The Cockroach Authors.
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

package builtins

import (
	"regexp"
	"strings"
	"testing"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

func TestHelpFunctions(t *testing.T) {
	// This test checks that all the built-in functions receive contextual help.
	for f := range builtins {
		if unicode.IsUpper(rune(f[0])) {
			continue
		}
		t.Run(f, func(t *testing.T) {
			_, err := parser.Parse("select " + f + "(??")
			if err == nil {
				t.Errorf("parser didn't trigger error")
				return
			}
			if err.Error() != "help token in input" {
				t.Fatal(err)
			}
			pgerr, ok := pgerror.GetPGCause(err)
			if !ok {
				t.Fatalf("expected pg error, got %v", err)
			}
			if !strings.HasPrefix(pgerr.Hint, "help:\n") {
				t.Errorf("expected 'help: ' prefix, got %q", pgerr.Hint)
				return
			}
			help := pgerr.Hint[6:]
			pattern := "Function:\\s+" + f + "\n"
			if m, err := regexp.MatchString(pattern, help); err != nil || !m {
				if err != nil {
					t.Errorf("pattern match failure: %v", err)
					return
				}
				t.Errorf("help text didn't match %q:\n%s", pattern, help)
			}
		})
	}
}
