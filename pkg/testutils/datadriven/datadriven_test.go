// Copyright 2019 The Cockroach Authors.
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

package datadriven

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	input := `
# NB: we allow duplicate args. It's unclear at this time whether this is useful,
# either way, ScanArgs simply picks the first occurrence.
make argTuple=(1, 🍌) argInt=12 argString=greedily argString=totally_ignored
sentence
----
Did the following: make sentence
1 hungry monkey eats a 🍌
while 12 other monkeys watch greedily
`

	RunTestFromString(t, input, func(d *TestData) string {
		var one int
		var twelve int
		var banana string
		var greedily string
		d.ScanArgs(t, "argTuple", &one, &banana)
		d.ScanArgs(t, "argInt", &twelve)
		d.ScanArgs(t, "argString", &greedily)
		return fmt.Sprintf("Did the following: %s %s\n%d hungry monkey eats a %s\nwhile %d other monkeys watch %s\n",
			d.Cmd, d.Input, one, banana, twelve, greedily,
		)
	})
}
