// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestOperationsFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		step Step
	}{
		{step: step(get(`a`))},
		{step: step(del(`a`, 1))},
		{step: step(batch(get(`b`), reverseScanForUpdate(`c`, `e`), get(`f`)))},
		{
			step: step(
				closureTxn(ClosureTxnType_Commit,
					batch(get(`g`), get(`h`), del(`i`, 1)),
					delRange(`j`, `k`, 2),
					put(`k`, 3),
				)),
		},
	}

	w := echotest.Walk(t, testutils.TestDataPath(t, t.Name()))
	defer w.Check(t)
	for i, test := range tests {
		name := fmt.Sprint(i)
		t.Run(name, w.Do(t, name, func(t *testing.T, path string) {
			var actual strings.Builder
			test.step.format(&actual, formatCtx{indent: "···"})
			echotest.Require(t, strings.TrimLeft(actual.String(), "\n"), path)
		}))
	}
}
