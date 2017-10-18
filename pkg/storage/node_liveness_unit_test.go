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

package storage

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestShouldReplaceLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()

	l := func(epo int64, expiration hlc.Timestamp, draining, decom bool) Liveness {
		return Liveness{
			Epoch:           epo,
			Expiration:      hlc.LegacyTimestamp(expiration),
			Draining:        draining,
			Decommissioning: decom,
		}
	}
	const (
		no  = false
		yes = true
	)
	now := hlc.Timestamp{WallTime: 12345}

	for _, test := range []struct {
		old, new Liveness
		exp      bool
	}{
		{
			// Epoch update only.
			Liveness{},
			l(1, hlc.Timestamp{}, false, false),
			yes,
		},
		{
			// No Epoch update, but Expiration update.
			l(1, now, false, false),
			l(1, now.Add(0, 1), false, false),
			yes,
		},
		{
			// No update.
			l(1, now, false, false),
			l(1, now, false, false),
			no,
		},
		{
			// Only Decommissioning changes.
			l(1, now, false, false),
			l(1, now, false, true),
			yes,
		},
		{
			// Only Draining changes.
			l(1, now, false, false),
			l(1, now, true, false),
			yes,
		},
		{
			// Decommissioning changes, but Epoch moves backwards.
			l(10, now, true, true),
			l(9, now, true, false),
			no,
		},
		{
			// Draining changes, but Expiration moves backwards..
			l(10, now, false, false),
			l(10, now.Add(-1, 0), true, false),
			no,
		},
	} {
		t.Run("", func(t *testing.T) {
			if act := shouldReplaceLiveness(test.old, test.new); act != test.exp {
				t.Errorf("unexpected update: %+v", test)
			}
		})
	}
}
