// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package envutil

import (
	"os"
	"testing"
)

func TestEnvOrDefault(t *testing.T) {
	const def = 123
	os.Clearenv()
	// These tests are mostly an excuse to exercise otherwise unused code.
	// TODO(knz): Test everything.
	if act := EnvOrDefaultBytes("COCKROACH_X", def); act != def {
		t.Errorf("expected %d, got %d", def, act)
	}
	if act := EnvOrDefaultInt("COCKROACH_X", def); act != def {
		t.Errorf("expected %d, got %d", def, act)
	}
}

func TestCheckVarName(t *testing.T) {
	t.Run("checkVarName", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			valid bool
		}{
			{
				name:  "abc123",
				valid: false,
			},
			{
				name:  "ABC 123",
				valid: false,
			},
			{
				name:  "@&) 123",
				valid: false,
			},
			{
				name:  "ABC123",
				valid: true,
			},
			{
				name:  "ABC_123",
				valid: true,
			},
		} {
			func() {
				defer func() {
					r := recover()
					if !tc.valid && r == nil {
						t.Errorf("expected panic for name %q, got none", tc.name)
					} else if tc.valid && r != nil {
						t.Errorf("unexpected panic for name %q, got %q", tc.name, r)
					}
				}()

				checkVarName(tc.name)
			}()
		}
	})

	t.Run("checkInternalVarName", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			valid bool
		}{
			{
				name:  "ABC_123",
				valid: false,
			},
			{
				name:  "COCKROACH_X",
				valid: true,
			},
		} {
			func() {
				defer func() {
					r := recover()
					if !tc.valid && r == nil {
						t.Errorf("expected panic for name %q, got none", tc.name)
					} else if tc.valid && r != nil {
						t.Errorf("unexpected panic for name %q, got %q", tc.name, r)
					}
				}()

				checkInternalVarName(tc.name)
			}()
		}
	})

	t.Run("checkExternalVarName", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			valid bool
		}{
			{
				name:  "COCKROACH_X",
				valid: false,
			},
			{
				name:  "ABC_123",
				valid: true,
			},
		} {
			func() {
				defer func() {
					r := recover()
					if !tc.valid && r == nil {
						t.Errorf("expected panic for name %q, got none", tc.name)
					} else if tc.valid && r != nil {
						t.Errorf("unexpected panic for name %q, got %q", tc.name, r)
					}
				}()

				checkExternalVarName(tc.name)
			}()
		}
	})
}
