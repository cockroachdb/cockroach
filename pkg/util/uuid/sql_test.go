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
	"encoding/json"
	"fmt"
	"testing"
)

func TestSQL(t *testing.T) {
	t.Run("Value", testSQLValue)
	t.Run("Scan", func(t *testing.T) {
		t.Run("Binary", testSQLScanBinary)
		t.Run("String", testSQLScanString)
		t.Run("Text", testSQLScanText)
		t.Run("Unsupported", testSQLScanUnsupported)
		t.Run("Nil", testSQLScanNil)
	})
}

func testSQLValue(t *testing.T) {
	v, err := codecTestUUID.Value()
	if err != nil {
		t.Fatal(err)
	}
	got, ok := v.(string)
	if !ok {
		t.Fatalf("Value() returned %T, want string", v)
	}
	if want := codecTestUUID.String(); got != want {
		t.Errorf("Value() == %q, want %q", got, want)
	}
}

func testSQLScanBinary(t *testing.T) {
	got := UUID{}
	err := got.Scan(codecTestData)
	if err != nil {
		t.Fatal(err)
	}
	if got != codecTestUUID {
		t.Errorf("Scan(%x): got %v, want %v", codecTestData, got, codecTestUUID)
	}
}

func testSQLScanString(t *testing.T) {
	s := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	got := UUID{}
	err := got.Scan(s)
	if err != nil {
		t.Fatal(err)
	}
	if got != codecTestUUID {
		t.Errorf("Scan(%q): got %v, want %v", s, got, codecTestUUID)
	}
}

func testSQLScanText(t *testing.T) {
	text := []byte("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	got := UUID{}
	err := got.Scan(text)
	if err != nil {
		t.Fatal(err)
	}
	if got != codecTestUUID {
		t.Errorf("Scan(%q): got %v, want %v", text, got, codecTestUUID)
	}
}

func testSQLScanUnsupported(t *testing.T) {
	unsupported := []interface{}{
		true,
		42,
	}
	for _, v := range unsupported {
		got := UUID{}
		err := got.Scan(v)
		if err == nil {
			t.Errorf("Scan(%T) succeeded, got %v", v, got)
		}
	}
}

func testSQLScanNil(t *testing.T) {
	got := UUID{}
	err := got.Scan(nil)
	if err == nil {
		t.Errorf("Scan(nil) succeeded, got %v", got)
	}
}

func TestNullUUID(t *testing.T) {
	t.Run("Value", func(t *testing.T) {
		t.Run("Nil", testNullUUIDValueNil)
		t.Run("Valid", testNullUUIDValueValid)
	})

	t.Run("Scan", func(t *testing.T) {
		t.Run("Nil", testNullUUIDScanNil)
		t.Run("Valid", testNullUUIDScanValid)
		t.Run("UUID", testNullUUIDScanUUID)
	})

	t.Run("MarshalJSON", func(t *testing.T) {
		t.Run("Nil", testNullUUIDMarshalJSONNil)
		t.Run("Null", testNullUUIDMarshalJSONNull)
		t.Run("Valid", testNullUUIDMarshalJSONValid)
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
		t.Run("Nil", testNullUUIDUnmarshalJSONNil)
		t.Run("Null", testNullUUIDUnmarshalJSONNull)
		t.Run("Valid", testNullUUIDUnmarshalJSONValid)
		t.Run("Malformed", testNullUUIDUnmarshalJSONMalformed)
	})
}

func testNullUUIDValueNil(t *testing.T) {
	nu := NullUUID{}
	got, err := nu.Value()
	if got != nil {
		t.Errorf("null NullUUID.Value returned non-nil driver.Value")
	}
	if err != nil {
		t.Errorf("null NullUUID.Value returned non-nil error")
	}
}

func testNullUUIDValueValid(t *testing.T) {
	nu := NullUUID{
		Valid: true,
		UUID:  codecTestUUID,
	}
	got, err := nu.Value()
	if err != nil {
		t.Fatal(err)
	}
	s, ok := got.(string)
	if !ok {
		t.Errorf("Value() returned %T, want string", got)
	}
	want := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	if s != want {
		t.Errorf("%v.Value() == %s, want %s", nu, s, want)
	}
}

func testNullUUIDScanNil(t *testing.T) {
	u := NullUUID{}
	err := u.Scan(nil)
	if err != nil {
		t.Fatal(err)
	}
	if u.Valid {
		t.Error("NullUUID is valid after Scan(nil)")
	}
	if u.UUID != Nil {
		t.Errorf("NullUUID.UUID is %v after Scan(nil) want Nil", u.UUID)
	}
}

func testNullUUIDScanValid(t *testing.T) {
	s := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	u := NullUUID{}
	err := u.Scan(s)
	if err != nil {
		t.Fatal(err)
	}
	if !u.Valid {
		t.Errorf("Valid == false after Scan(%q)", s)
	}
	if u.UUID != codecTestUUID {
		t.Errorf("UUID == %v after Scan(%q), want %v", u.UUID, s, codecTestUUID)
	}
}

func testNullUUIDScanUUID(t *testing.T) {
	u := NullUUID{}
	err := u.Scan(codecTestUUID)
	if err != nil {
		t.Fatal(err)
	}
	if !u.Valid {
		t.Errorf("Valid == false after scan(%v)", codecTestUUID)
	}
	if u.UUID != codecTestUUID {
		t.Errorf("UUID == %v after Scan(%v), want %v", u.UUID, codecTestUUID, codecTestUUID)
	}
}

func testNullUUIDMarshalJSONNil(t *testing.T) {
	u := NullUUID{Valid: true}

	data, err := u.MarshalJSON()
	if err != nil {
		t.Fatalf("(%#v).MarshalJSON err want: <nil>, got: %v", u, err)
	}

	dataStr := string(data)

	if dataStr != fmt.Sprintf("%q", Nil) {
		t.Fatalf("(%#v).MarshalJSON value want: %s, got: %s", u, Nil, dataStr)
	}
}

func testNullUUIDMarshalJSONValid(t *testing.T) {
	u := NullUUID{
		Valid: true,
		UUID:  codecTestUUID,
	}

	data, err := u.MarshalJSON()
	if err != nil {
		t.Fatalf("(%#v).MarshalJSON err want: <nil>, got: %v", u, err)
	}

	dataStr := string(data)

	if dataStr != fmt.Sprintf("%q", codecTestUUID) {
		t.Fatalf("(%#v).MarshalJSON value want: %s, got: %s", u, codecTestUUID, dataStr)
	}
}

func testNullUUIDMarshalJSONNull(t *testing.T) {
	u := NullUUID{}

	data, err := u.MarshalJSON()
	if err != nil {
		t.Fatalf("(%#v).MarshalJSON err want: <nil>, got: %v", u, err)
	}

	dataStr := string(data)

	if dataStr != "null" {
		t.Fatalf("(%#v).MarshalJSON value want: %s, got: %s", u, "null", dataStr)
	}
}

func testNullUUIDUnmarshalJSONNil(t *testing.T) {
	var u NullUUID

	data := []byte(`"00000000-0000-0000-0000-000000000000"`)

	if err := json.Unmarshal(data, &u); err != nil {
		t.Fatalf("json.Unmarshal err = %v, want <nil>", err)
	}

	if !u.Valid {
		t.Fatalf("u.Valid = false, want true")
	}

	if u.UUID != Nil {
		t.Fatalf("u.UUID = %v, want %v", u.UUID, Nil)
	}
}

func testNullUUIDUnmarshalJSONNull(t *testing.T) {
	var u NullUUID

	data := []byte(`null`)

	if err := json.Unmarshal(data, &u); err != nil {
		t.Fatalf("json.Unmarshal err = %v, want <nil>", err)
	}

	if u.Valid {
		t.Fatalf("u.Valid = true, want false")
	}

	if u.UUID != Nil {
		t.Fatalf("u.UUID = %v, want %v", u.UUID, Nil)
	}
}
func testNullUUIDUnmarshalJSONValid(t *testing.T) {
	var u NullUUID

	data := []byte(`"6ba7b810-9dad-11d1-80b4-00c04fd430c8"`)

	if err := json.Unmarshal(data, &u); err != nil {
		t.Fatalf("json.Unmarshal err = %v, want <nil>", err)
	}

	if !u.Valid {
		t.Fatalf("u.Valid = false, want true")
	}

	if u.UUID != codecTestUUID {
		t.Fatalf("u.UUID = %v, want %v", u.UUID, Nil)
	}
}

func testNullUUIDUnmarshalJSONMalformed(t *testing.T) {
	var u NullUUID

	data := []byte(`257`)

	if err := json.Unmarshal(data, &u); err == nil {
		t.Fatal("json.Unmarshal err = <nil>, want error")
	}
}
