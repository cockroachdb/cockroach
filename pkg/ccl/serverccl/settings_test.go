// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestConsoleKeysVisibility(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, key := range settings.ConsoleKeys() {
		setting, found := settings.LookupForLocalAccessByKey(key, true /* forSystemTenant */)
		if !found {
			t.Fatalf("not found: %q", key)
		}

		if setting.Class() == settings.SystemOnly {
			t.Errorf("setting %q used in console, cannot be system-only", key)
		}
	}
}

func TestSettingListWithPreviousApplicationClass(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for key := range settings.TestingListPrevAppSettings() {
		setting, found := settings.LookupForLocalAccessByKey(key, true /* forSystemTenant */)
		if !found {
			t.Fatalf("not found: %q", key)
		}

		if setting.InternalKey() != key {
			t.Errorf("prev-application setting list must contain key %q, doesn't match %q", setting.InternalKey(), key)
		}
	}
}
