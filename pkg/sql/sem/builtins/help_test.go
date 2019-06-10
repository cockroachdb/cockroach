// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
			if !strings.HasPrefix(err.Error(), "help token in input") {
				t.Fatal(err)
			}
			pgerr := pgerror.Flatten(err)
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
