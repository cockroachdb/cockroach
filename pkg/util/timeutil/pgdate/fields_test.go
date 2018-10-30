// Copyright 2018 The Cockroach Authors.
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

package pgdate

import "testing"

func TestFieldSet(t *testing.T) {
	var f fieldSet
	if f.Has(fieldDay) {
		t.Fatal("unexpected day")
	}
	f = newFieldSet(fieldDay, fieldHour)
	if !f.Has(fieldDay) {
		t.Fatal("expected day")
	}
	if !f.Has(fieldHour) {
		t.Fatal("expected hour")
	}

	if !f.HasAll(newFieldSet(fieldDay, fieldHour)) {
		t.Fatal("expected day and hour")
	}
	if f.HasAll(newFieldSet(fieldDay, fieldSecond)) {
		t.Fatal("should not have matched")
	}
	if f != f.Add(fieldHour) {
		t.Fatal("setting existing field should be no-op")
	}
	f = f.Clear(fieldHour)
	if f.Has(fieldHour) {
		t.Fatal("unexpected hour")
	}
}
