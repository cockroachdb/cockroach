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

const mb = int64(1024 * 1024)

var boolTA = RegisterBoolSetting("bool.t", "", true)
var boolFA = RegisterBoolSetting("bool.f", "", false)
var strFooA = RegisterStringSetting("str.foo", "", "")
var strBarA = RegisterStringSetting("str.bar", "", "bar")
var i1A = RegisterIntSetting("i.1", "", 0)
var i2A = RegisterIntSetting("i.2", "", 5)
var fA = RegisterFloatSetting("f", "", 5.4)
var dA = RegisterDurationSetting("d", "", time.Second)
var byteSize = RegisterByteSizeSetting("zzz", "", mb)

func TestCache(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		if expected, actual := false, boolFA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, boolTA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "", strFooA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bar", strBarA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(0), i1A.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(5), i2A.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 5.4, fA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := time.Second, dA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := mb, byteSize.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("lookup", func(t *testing.T) {
		if actual, _, ok := Lookup("i.1"); !ok || i1A != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", i1A, actual, ok)
		}
		if actual, _, ok := Lookup("f"); !ok || fA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", fA, actual, ok)
		}
		if actual, _, ok := Lookup("d"); !ok || dA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", dA, actual, ok)
		}
		if actual, _, ok := Lookup("dne"); ok {
			t.Fatalf("expected nothing, got %v", actual)
		}
	})

	t.Run("read and write each type", func(t *testing.T) {
		u := MakeUpdater()
		if err := u.Set("bool.t", EncodeBool(false), "b"); err != nil {
			t.Fatal(err)
		}
		if err := u.Set("bool.f", EncodeBool(true), "b"); err != nil {
			t.Fatal(err)
		}
		if err := u.Set("str.foo", "baz", "s"); err != nil {
			t.Fatal(err)
		}
		if err := u.Set("i.2", EncodeInt(3), "i"); err != nil {
			t.Fatal(err)
		}
		if err := u.Set("f", EncodeFloat(3.1), "f"); err != nil {
			t.Fatal(err)
		}
		if err := u.Set("d", EncodeDuration(2*time.Hour), "d"); err != nil {
			t.Fatal(err)
		}
		if err := u.Set("zzz", EncodeInt(mb*5), "z"); err != nil {
			t.Fatal(err)
		}
		u.Done()

		if expected, actual := false, boolTA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, boolFA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "baz", strFooA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(3), i2A.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 3.1, fA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 2*time.Hour, dA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := mb*5, byteSize.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// We didn't change this one, so should still see the default.
		if expected, actual := "bar", strBarA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("any setting not included in an Updater reverts to default", func(t *testing.T) {
		{
			u := MakeUpdater()
			if err := u.Set("bool.f", EncodeBool(true), "b"); err != nil {
				t.Fatal(err)
			}
			if err := u.Set("i.1", EncodeInt(1), "i"); err != nil {
				t.Fatal(err)
			}
			if err := u.Set("i.2", EncodeInt(7), "i"); err != nil {
				t.Fatal(err)
			}
			u.Done()
		}

		if expected, actual := true, boolFA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		// If the updater doesn't have a key, e.g. if the setting has been deleted,
		// Doneing it from the cache.
		MakeUpdater().Done()

		if expected, actual := false, boolFA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		if expected, actual := false, boolFA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("an invalid update to a given setting preserves its previously set value", func(t *testing.T) {
		{
			u := MakeUpdater()
			if err := u.Set("i.2", EncodeInt(9), "i"); err != nil {
				t.Fatal(err)
			}
			u.Done()
		}
		before := i2A.Get()

		// Doneing after attempting to set with wrong type preserves current value.
		{
			u := MakeUpdater()
			// We don't use testutils.IsError, to avoid the import.
			if err := u.Set("i.2", EncodeBool(false), "b"); !testutils.IsError(err,
				"setting 'i.2' defined as type i, not b",
			) {
				t.Fatal(err)
			}
			u.Done()
		}

		if expected, actual := before, i2A.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// Doneing after attempting to set with invalid format does too.
		{
			u := MakeUpdater()
			if err := u.Set("i.2", EncodeBool(false), "i"); !testutils.IsError(err,
				"strconv.Atoi: parsing \"false\": invalid syntax",
			) {
				t.Fatal(err)
			}
			u.Done()
		}

		if expected, actual := before, i2A.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("mocks", func(t *testing.T) {
		if expected, actual := true, TestingBoolSetting(true).Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "true", TestingStringSetting("true").Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(9), TestingIntSetting(9).Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 9.4, TestingFloatSetting(9.4).Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := time.Hour, TestingDurationSetting(time.Hour).Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := mb*10, TestingByteSizeSetting(mb*10).Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

	})
}
