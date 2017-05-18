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

package log

import "testing"

func TestCrashReportingFormatSave(t *testing.T) {
	r1 := "i am hidden"
	r2 := Safe{V: "i am public"}
	r3 := Safe{V: &r2}
	f1, f2, f3 := format(r1), format(r2), format(r3)
	exp1, exp2 := "string", r2.V.(string)
	exp3 := "&{V:i am public}"
	if f1 != exp1 {
		t.Errorf("wanted %s, got %s", exp1, f1)
	}
	if f2 != exp2 {
		t.Errorf("wanted %s, got %s", exp2, f2)
	}
	if f3 != exp3 {
		t.Errorf("wanted %s, got %s", exp3, f3)
	}
}
