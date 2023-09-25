// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package read_write_2

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/integration_tests"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var rwS = settings.RegisterStringSetting(settings.TenantWritable, "tenant.writable", "desc", "initial")

func TestSettingDefaultPropagationReadWrite2(t *testing.T) {
	defer leaktest.AfterTest(t)()

	integration_tests.RunSettingDefaultPropagationTest(t, rwS, false)
}
