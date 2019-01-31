// Copyright 2019 The Cockroach Authors.
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
