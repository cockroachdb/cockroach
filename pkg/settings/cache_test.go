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
	"strings"
	"testing"
)

func TestCache(t *testing.T) {
	defaultsBefore := registry
	registry = map[string]value{
		"bool.t":  {typ: BoolValue, b: true},
		"bool.f":  {typ: BoolValue},
		"str.foo": {typ: StringValue},
		"str.bar": {typ: StringValue, s: "bar"},
		"i.1":     {typ: IntValue},
		"i.2":     {typ: IntValue, i: 5},
		"f":       {typ: FloatValue, f: 5.4},
	}
	cache.Lock()
	before := cache.values
	cache.values = nil
	cache.Unlock()

	defer func() {
		registry = defaultsBefore
		cache.Lock()
		cache.values = before
		cache.Unlock()
	}()

	t.Run("defaults", func(t *testing.T) {
		if expected, actual := false, getBool("bool.f"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, getBool("bool.t"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "", getString("str.foo"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bar", getString("str.bar"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 0, getInt("i.1"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 5, getInt("i.2"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 5.4, getFloat("f"); expected != actual {
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
		u := NewUpdater()
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

		if expected, actual := false, getBool("bool.t"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, getBool("bool.f"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "baz", getString("str.foo"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 3, getInt("i.2"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 3.1, getFloat("f"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// We didn't change this one, so should still see the default.
		if expected, actual := "bar", getString("str.bar"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("any setting not included in an Updater reverts to default", func(t *testing.T) {
		{
			u := NewUpdater()
			if err := u.Add("bool.f", EncodeBool(true), "b"); err != nil {
				t.Fatal(err)
			}
			u.Apply()
		}

		if expected, actual := true, getBool("bool.f"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		// If the updater doesn't have a key, e.g. if the setting has been deleted,
		// applying it from the cache.
		NewUpdater().Apply()

		if expected, actual := false, getBool("bool.f"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// Reset() clears an existing Updater, as an alternative to discarding it
		// and calling NewUpdater.
		{
			u := NewUpdater()
			if err := u.Add("bool.f", EncodeBool(true), "b"); err != nil {
				t.Fatal(err)
			}
			u.Reset()
			u.Apply()
		}

		if expected, actual := false, getBool("bool.f"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("an invalid update to a given setting preserves its previously set value", func(t *testing.T) {
		{
			u := NewUpdater()
			if err := u.Add("i.2", EncodeInt(9), "i"); err != nil {
				t.Fatal(err)
			}
			u.Apply()
		}
		before := getInt("i.2")

		// Applying after attempting to set with wrong type preserves current value.
		{
			u := NewUpdater()
			// We don't use testutils.IsError, to avoid the import.
			if err := u.Add("i.2", EncodeBool(false), "b"); err == nil || !strings.Contains(err.Error(),
				"setting 'i.2' defined as type i, not b",
			) {
				t.Fatal(err)
			}
			u.Apply()
		}

		if expected, actual := before, getInt("i.2"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// Applying after attempting to set with invalid format does too.
		{
			u := NewUpdater()
			if err := u.Add("i.2", EncodeBool(false), "i"); err == nil || !strings.Contains(err.Error(),
				"strconv.Atoi: parsing \"false\": invalid syntax",
			) {
				t.Fatal(err)
			}
			u.Apply()
		}

		if expected, actual := before, getInt("i.2"); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})
}
