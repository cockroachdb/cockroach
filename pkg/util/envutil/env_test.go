// Copyright 2016 The Cockroach Authors.
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
