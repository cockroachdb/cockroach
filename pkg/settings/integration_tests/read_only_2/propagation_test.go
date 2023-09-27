// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package read_only_2

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/integration_tests"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var roS = settings.RegisterStringSetting(settings.SystemVisible, "tenant.read.only", "desc", "initial")

// TestSettingDefaultPropagationReadOnly2 runs one of 4 invocations of
// `RunSettingDefaultPropagationTest`. The test is split 4-ways across
// 4 packages to avoid timeouts in CI and increased test parallelism.
func TestSettingDefaultPropagationReadOnly2(t *testing.T) {
	defer leaktest.AfterTest(t)()

	integration_tests.RunSettingDefaultPropagationTest(t, roS, false)
}
