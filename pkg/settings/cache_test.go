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

package settings

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
)

var i1, i2 int

var boolTAccessor = RegisterBoolSetting("bool.t", "", true)
var boolFAccessor = RegisterBoolSetting("bool.f", "", false)
var strFooAccessor = RegisterStringSetting("str.foo", "", "")
var strBarAccessor = RegisterStringSetting("str.bar", "", "bar")
var i1Accessor = RegisterIntSetting("i.1", "", 0)
var i2Accessor = RegisterIntSetting("i.2", "", 5)
var fAccessor = RegisterFloatSetting("f", "", 5.4)
var dAccessor = RegisterDurationSetting("d", "", time.Second)

func init() {
	RegisterCallback(func() {
		i1 = getInt("i.1")
		i2 = getInt("i.2")
	})
}

func TestCache(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		if expected, actual := false, boolFAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, boolTAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "", strFooAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bar", strBarAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 0, i1Accessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 5, i2Accessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		// registering callback should have also run it initially and set default.
		if expected, actual := 0, i1; expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 5, i2; expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 5.4, fAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := time.Second, dAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if actual, ok := TypeOf("i.1"); !ok || IntValue != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", IntValue, actual, ok)
		}
		if actual, ok := TypeOf("f"); !ok || FloatValue != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", FloatValue, actual, ok)
		}
		if actual, ok := TypeOf("dne"); ok {
			t.Fatalf("expected nothing, got %v", actual)
		}
	})

	t.Run("read and write each type", func(t *testing.T) {
		u := MakeUpdater()
		if err := u.Add("bool.t", EncodeBool(false), "b"); err != nil {
			t.Fatal(err)
		}
		if err := u.Add("bool.f", EncodeBool(true), "b"); err != nil {
			t.Fatal(err)
		}
		if err := u.Add("str.foo", "baz", "s"); err != nil {
			t.Fatal(err)
		}
		if err := u.Add("i.2", EncodeInt(3), "i"); err != nil {
			t.Fatal(err)
		}
		if err := u.Add("f", EncodeFloat(3.1), "f"); err != nil {
			t.Fatal(err)
		}
		u.Apply()

		if expected, actual := false, boolTAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, boolFAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "baz", strFooAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 3, i2Accessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 0, i1; expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 3, i2; expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 3.1, fAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := time.Second, dAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// We didn't change this one, so should still see the default.
		if expected, actual := "bar", strBarAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("any setting not included in an Updater reverts to default", func(t *testing.T) {
		{
			u := MakeUpdater()
			if err := u.Add("bool.f", EncodeBool(true), "b"); err != nil {
				t.Fatal(err)
			}
			if err := u.Add("i.1", EncodeInt(1), "i"); err != nil {
				t.Fatal(err)
			}
			if err := u.Add("i.2", EncodeInt(7), "i"); err != nil {
				t.Fatal(err)
			}
			u.Apply()
		}

		if expected, actual := true, boolFAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 1, i1; expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 7, i2; expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		// If the updater doesn't have a key, e.g. if the setting has been deleted,
		// applying it from the cache.
		MakeUpdater().Apply()

		if expected, actual := false, boolFAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 0, i1; expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 5, i2; expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// Reset() clears an existing Updater, as an alternative to discarding it
		// and calling MakeUpdater.
		{
			u := MakeUpdater()
			if err := u.Add("bool.f", EncodeBool(true), "b"); err != nil {
				t.Fatal(err)
			}
			u.Reset()
			u.Apply()
		}

		if expected, actual := false, boolFAccessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("an invalid update to a given setting preserves its previously set value", func(t *testing.T) {
		{
			u := MakeUpdater()
			if err := u.Add("i.2", EncodeInt(9), "i"); err != nil {
				t.Fatal(err)
			}
			u.Apply()
		}
		before := i2Accessor()

		// Applying after attempting to set with wrong type preserves current value.
		{
			u := MakeUpdater()
			// We don't use testutils.IsError, to avoid the import.
			if err := u.Add("i.2", EncodeBool(false), "b"); !testutils.IsError(err,
				"setting 'i.2' defined as type i, not b",
			) {
				t.Fatal(err)
			}
			u.Apply()
		}

		if expected, actual := before, i2Accessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// Applying after attempting to set with invalid format does too.
		{
			u := MakeUpdater()
			if err := u.Add("i.2", EncodeBool(false), "i"); !testutils.IsError(err,
				"strconv.Atoi: parsing \"false\": invalid syntax",
			) {
				t.Fatal(err)
			}
			u.Apply()
		}

		if expected, actual := before, i2Accessor(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})
}
