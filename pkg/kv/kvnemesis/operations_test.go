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

var (
	k1  = uint64ToKey(1)
	k2  = uint64ToKey(2)
	k3  = uint64ToKey(3)
	k4  = uint64ToKey(4)
	k5  = uint64ToKey(5)
	k6  = uint64ToKey(6)
	k7  = uint64ToKey(7)
	k8  = uint64ToKey(8)
	k9  = uint64ToKey(9)
	k10 = uint64ToKey(10)
	k11 = uint64ToKey(11)
)

func TestOperationsFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		step Step
	}{
		{step: step(get(k1))},
		{step: step(del(k1, 1))},
		{step: step(batch(get(k2), reverseScanForUpdate(k3, k5), get(k6)))},
		{
			step: step(
				closureTxn(ClosureTxnType_Commit,
					batch(get(k7), get(k8), del(k9, 1)),
					delRange(k10, k11, 2),
					put(k11, 3),
				)),
		},
	}

	w := echotest.NewWalker(t, testutils.TestDataPath(t, t.Name()))
	for i, test := range tests {
		name := fmt.Sprint(i)
		t.Run(name, w.Run(t, name, func(t *testing.T) string {
			var actual strings.Builder
			test.step.format(&actual, formatCtx{indent: "···"})
			return strings.TrimLeft(actual.String(), "\n")
		}))
	}
}
