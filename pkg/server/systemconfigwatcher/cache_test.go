// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systemconfigwatcher

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher/systemconfigwatchertest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	systemconfigwatchertest.TestSystemConfigWatcher(t, true /* skipSecondary */)
}
