// Copyright 2018 The Cockroach Authors.
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
