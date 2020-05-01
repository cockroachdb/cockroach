// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright (C) 2013-2018 by Maxim Bublis <b@codemonkey.ru>
// Use of this source code is governed by a MIT-style
// license that can be found in licenses/MIT-gofrs.txt.

// This code originated in github.com/gofrs/uuid.

package uuid

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
)

func TestUUID(t *testing.T) {
	t.Run("Bytes", testUUIDBytes)
	t.Run("String", testUUIDString)
	t.Run("Version", testUUIDVersion)
	t.Run("Variant", testUUIDVariant)
	t.Run("SetVersion", testUUIDSetVersion)
	t.Run("SetVariant", testUUIDSetVariant)
}

func testUUIDBytes(t *testing.T) {
	got := codecTestUUID.GetBytes()
	want := codecTestData
	if !bytes.Equal(got, want) {
		t.Errorf("%v.GetBytes() = %x, want %x", codecTestUUID, got, want)
	}
}

func testUUIDString(t *testing.T) {
	got := NamespaceDNS.String()
	want := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	if got != want {
		t.Errorf("%v.String() = %q, want %q", NamespaceDNS, got, want)
	}
}

func testUUIDVersion(t *testing.T) {
	u := UUID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	if got, want := u.Version(), V1; got != want {
		t.Errorf("%v.Version() == %d, want %d", u, got, want)
	}
}

func testUUIDVariant(t *testing.T) {
	tests := []struct {
		u    UUID
		want byte
	}{
		{
			u:    UUID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			want: VariantNCS,
		},
		{
			u:    UUID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			want: VariantRFC4122,
		},
		{
			u:    UUID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			want: VariantMicrosoft,
		},
		{
			u:    UUID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			want: VariantFuture,
		},
	}
	for _, tt := range tests {
		if got := tt.u.Variant(); got != tt.want {
			t.Errorf("%v.Variant() == %d, want %d", tt.u, got, tt.want)
		}
	}
}

func testUUIDSetVersion(t *testing.T) {
	u := UUID{}
	want := V4
	u.SetVersion(want)
	if got := u.Version(); got != want {
		t.Errorf("%v.Version() == %d after SetVersion(%d)", u, got, want)
	}
}

func testUUIDSetVariant(t *testing.T) {
	variants := []byte{
		VariantNCS,
		VariantRFC4122,
		VariantMicrosoft,
		VariantFuture,
	}
	for _, want := range variants {
		u := UUID{}
		u.SetVariant(want)
		if got := u.Variant(); got != want {
			t.Errorf("%v.Variant() == %d after SetVariant(%d)", u, got, want)
		}
	}
}

func TestMust(t *testing.T) {
	sentinel := fmt.Errorf("uuid: sentinel error")
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("did not panic, want %v", sentinel)
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("panicked with %T, want error (%v)", r, sentinel)
		}
		if !errors.Is(err, sentinel) {
			t.Fatalf("panicked with %v, want %v", err, sentinel)
		}
	}()
	fn := func() (UUID, error) {
		return Nil, sentinel
	}
	Must(fn())
}

func TestTimeFromTimestamp(t *testing.T) {
	tests := []struct {
		t    Timestamp
		want time.Time
	}{
		// a zero timestamp represents October 15, 1582 at midnight UTC
		{t: Timestamp(0), want: time.Date(1582, 10, 15, 0, 0, 0, 0, time.UTC)},
		// a one value is 100ns later
		{t: Timestamp(1), want: time.Date(1582, 10, 15, 0, 0, 0, 100, time.UTC)},
		// 10 million 100ns intervals later is one second
		{t: Timestamp(10000000), want: time.Date(1582, 10, 15, 0, 0, 1, 0, time.UTC)},
		{t: Timestamp(60 * 10000000), want: time.Date(1582, 10, 15, 0, 1, 0, 0, time.UTC)},
		{t: Timestamp(60 * 60 * 10000000), want: time.Date(1582, 10, 15, 1, 0, 0, 0, time.UTC)},
		{t: Timestamp(24 * 60 * 60 * 10000000), want: time.Date(1582, 10, 16, 0, 0, 0, 0, time.UTC)},
		{t: Timestamp(365 * 24 * 60 * 60 * 10000000), want: time.Date(1583, 10, 15, 0, 0, 0, 0, time.UTC)},
		// maximum timestamp value in a UUID is the year 5236
		{t: Timestamp(uint64(1<<60 - 1)), want: time.Date(5236, 03, 31, 21, 21, 0, 684697500, time.UTC)},
	}
	for _, tt := range tests {
		got, _ := tt.t.Time()
		if !got.Equal(tt.want) {
			t.Errorf("%v.Time() == %v, want %v", tt.t, got, tt.want)
		}
	}
}

func TestTimestampFromV1(t *testing.T) {
	tests := []struct {
		u       UUID
		want    Timestamp
		wanterr bool
	}{
		{u: Must(NewV4()), wanterr: true},
		{u: Must(FromString("00000000-0000-1000-0000-000000000000")), want: 0},
		{u: Must(FromString("424f137e-a2aa-11e8-98d0-529269fb1459")), want: 137538640775418750},
		{u: Must(FromString("ffffffff-ffff-1fff-ffff-ffffffffffff")), want: Timestamp(1<<60 - 1)},
	}
	for _, tt := range tests {
		got, goterr := TimestampFromV1(tt.u)
		if tt.wanterr && goterr == nil {
			t.Errorf("TimestampFromV1(%v) want error, got %v", tt.u, got)
		} else if tt.want != got {
			t.Errorf("TimestampFromV1(%v) got %v, want %v", tt.u, got, tt.want)
		}
	}
}

func TestDeterministicV4(t *testing.T) {
	// Test sortedness by enumerating everything in a small `n`.
	var previous, current UUID
	for i := 0; i < 10; i++ {
		current.DeterministicV4(uint64(i), uint64(10))
		if bytes.Compare(previous[:], current[:]) >= 0 {
			t.Errorf(`%s should be less than %s`, previous, current)
		}
		copy(previous[:], current[:])
	}

	// Test uniqueness by enumerating adjacent `i`s in a big `n`.
	previous, current = UUID{}, UUID{}
	for i := 0; i < 10; i++ {
		current.DeterministicV4(uint64(i), math.MaxUint64)
		if bytes.Compare(previous[:], current[:]) >= 0 {
			t.Errorf(`%s should be less than %s`, previous, current)
		}
		copy(previous[:], current[:])
	}
}
