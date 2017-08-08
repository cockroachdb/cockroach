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

package settings_test

import (
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/pkg/errors"
)

type dummy struct {
	msg1       string
	growsbyone string
}

func (d *dummy) Unmarshal(data []byte) error {
	s := string(data)
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return errors.Errorf("expected two parts, not %v", parts)
	}
	*d = dummy{
		msg1: parts[0], growsbyone: parts[1],
	}
	return nil
}

func (d *dummy) Marshal() ([]byte, error) {
	if c := d.msg1 + d.growsbyone; strings.Contains(c, ".") {
		return nil, errors.New("must not contain dots: " + c)
	}
	return []byte(d.msg1 + "." + d.growsbyone), nil
}

var dummyTransformer = func(old []byte, update *string) ([]byte, interface{}, error) {
	var oldD dummy

	// If no old value supplied, fill in the default.
	if old == nil {
		oldD.msg1 = "default"
		oldD.growsbyone = "-"
		var err error
		old, err = oldD.Marshal()
		if err != nil {
			return nil, nil, err
		}
	}
	if err := oldD.Unmarshal(old); err != nil {
		return nil, nil, err
	}

	if update == nil {
		// Round-trip the existing value, but only if it passes sanity checks.
		b, err := oldD.Marshal()
		if err != nil {
			return nil, nil, err
		}
		return b, &oldD, err
	}

	// We have a new proposed update to the value, validate it.
	if len(*update) != len(oldD.growsbyone)+1 {
		return nil, nil, errors.New("dashes component must grow by exactly one")
	}
	newD := oldD
	newD.growsbyone = *update
	b, err := newD.Marshal()
	return b, &newD, err
}

const mb = int64(1024 * 1024)

var changes = struct {
	boolTA   int
	strFooA  int
	i1A      int
	fA       int
	dA       int
	eA       int
	byteSize int
	mA       int
}{}

var r = settings.NewRegistry()

var boolTA = r.RegisterBoolSetting("bool.t", "", true).OnChange(func() { changes.boolTA++ })
var boolFA = r.RegisterBoolSetting("bool.f", "", false)
var strFooA = r.RegisterStringSetting("str.foo", "", "").OnChange(func() { changes.strFooA++ })
var strBarA = r.RegisterStringSetting("str.bar", "", "bar")
var i1A = r.RegisterIntSetting("i.1", "", 0).OnChange(func() { changes.i1A++ })
var i2A = r.RegisterIntSetting("i.2", "", 5)
var fA = r.RegisterFloatSetting("f", "", 5.4).OnChange(func() { changes.fA++ })
var dA = r.RegisterDurationSetting("d", "", time.Second).OnChange(func() { changes.dA++ })
var eA = r.RegisterEnumSetting("e", "", "foo", map[int64]string{1: "foo", 2: "bar", 3: "baz"}).
	OnChange(func() { changes.eA++ })
var byteSize = r.RegisterByteSizeSetting("zzz", "", mb).OnChange(func() { changes.byteSize++ })
var mA = r.RegisterStateMachineSetting("statemachine", "foo", dummyTransformer).OnChange(func() { changes.mA++ })

func init() {
	r.RegisterBoolSetting("sekretz", "", false).Hide()
}

var strVal = r.RegisterValidatedStringSetting(
	"str.val", "", "", func(v string) error {
		for _, c := range v {
			if !unicode.IsLetter(c) {
				return errors.Errorf("not all runes of %s are letters: %c", v, c)
			}
		}
		return nil
	})
var dVal = r.RegisterNonNegativeDurationSetting("dVal", "", time.Second)
var fVal = r.RegisterNonNegativeFloatSetting("fVal", "", 5.4)
var byteSizeVal = r.RegisterValidatedByteSizeSetting(
	"byteSize.Val", "", mb, func(v int64) error {
		if v < 0 {
			return errors.Errorf("bytesize cannot be negative")
		}
		return nil
	})
var iVal = r.RegisterValidatedIntSetting(
	"i.Val", "", 0, func(v int64) error {
		if v < 0 {
			return errors.Errorf("int cannot be negative")
		}
		return nil
	})

func TestCache(t *testing.T) {
	t.Run("StateMachineSetting", func(t *testing.T) {
		mB := r.RegisterStateMachineSetting("local.m", "foo", dummyTransformer)
		if exp, act := "&{default -}", mB.String(); exp != act {
			t.Fatalf("wanted %q, got %q", exp, act)
		}
		growsTooFast := "grows too fast"
		if _, _, err := mB.Validate(nil, &growsTooFast); !testutils.IsError(err, "must grow by exactly one") {
			t.Fatal(err)
		}
		hasDots := "a."
		if _, _, err := mB.Validate(nil, &hasDots); !testutils.IsError(err, "must not contain dots") {
			t.Fatal(err)
		}
		ab := "ab"
		if _, _, err := mB.Validate(nil, &ab); err != nil {
			t.Fatal(err)
		}
		if _, _, err := mB.Validate([]byte("takes.precedence"), &ab); !testutils.IsError(err, "must grow by exactly one") {
			t.Fatal(err)
		}
		precedenceX := "precedencex"
		if _, _, err := mB.Validate([]byte("takes.precedence"), &precedenceX); err != nil {
			t.Fatal(err)
		}
		u := settings.MakeResettingUpdater(r)
		if err := u.Set("local.m", "default.XX", "m"); err != nil {
			t.Fatal(err)
		}
		u.Done()
		if exp, act := "&{default XX}", mB.String(); exp != act {
			t.Fatalf("wanted %q, got %q", exp, act)
		}
	})

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
		if expected, actual := "", strVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(0), i1A.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(5), i2A.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(0), iVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 5.4, fA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 5.4, fVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := time.Second, dA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := time.Second, dVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := mb, byteSize.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := mb, byteSizeVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(1), eA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "default.-", mA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("lookup", func(t *testing.T) {
		if actual, ok := r.Lookup("i.1"); !ok || i1A != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", i1A, actual, ok)
		}
		if actual, ok := r.Lookup("i.Val"); !ok || iVal != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", iVal, actual, ok)
		}
		if actual, ok := r.Lookup("f"); !ok || fA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", fA, actual, ok)
		}
		if actual, ok := r.Lookup("fVal"); !ok || fVal != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", fVal, actual, ok)
		}
		if actual, ok := r.Lookup("d"); !ok || dA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", dA, actual, ok)
		}
		if actual, ok := r.Lookup("dVal"); !ok || dVal != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", dVal, actual, ok)
		}
		if actual, ok := r.Lookup("e"); !ok || eA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", eA, actual, ok)
		}
		if actual, ok := r.Lookup("statemachine"); !ok || mA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", mA, actual, ok)
		}
		if actual, ok := r.Lookup("dne"); ok {
			t.Fatalf("expected nothing, got %v", actual)
		}
	})

	t.Run("read and write each type", func(t *testing.T) {
		u := settings.MakeResettingUpdater(r)
		if expected, actual := 0, changes.boolTA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("bool.t", settings.EncodeBool(false), "b"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.boolTA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("bool.f", settings.EncodeBool(true), "b"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 0, changes.strFooA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("str.foo", "baz", "s"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.strFooA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("str.val", "valid", "s"); err != nil {
			t.Fatal(err)
		}
		if err := u.Set("i.2", settings.EncodeInt(3), "i"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 0, changes.fA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("f", settings.EncodeFloat(3.1), "f"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.fA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("fVal", settings.EncodeFloat(3.1), "f"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 0, changes.dA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("d", settings.EncodeDuration(2*time.Hour), "d"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.dA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("dVal", settings.EncodeDuration(2*time.Hour), "d"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 0, changes.byteSize; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("zzz", settings.EncodeInt(mb*5), "z"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.byteSize; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("byteSize.Val", settings.EncodeInt(mb*5), "z"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 0, changes.mA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("statemachine", "default.AB", "m"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.mA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if expected, actual := 0, changes.eA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set("e", settings.EncodeInt(2), "e"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.eA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if expected, err := "strconv.Atoi: parsing \"notAValidValue\": invalid syntax",
			u.Set("e", "notAValidValue", "e"); !testutils.IsError(err, expected) {
			t.Fatalf("expected '%s' != actual error '%s'", expected, err)
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
		if expected, actual := "valid", strVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(3), i2A.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 3.1, fA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 3.1, fVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 2*time.Hour, dA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 2*time.Hour, dVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(2), eA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := mb*5, byteSize.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := mb*5, byteSizeVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "default.AB", mA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// We didn't change this one, so should still see the default.
		if expected, actual := "bar", strBarA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("any setting not included in an Updater reverts to default", func(t *testing.T) {
		{
			u := settings.MakeResettingUpdater(r)
			if err := u.Set("bool.f", settings.EncodeBool(true), "b"); err != nil {
				t.Fatal(err)
			}
			if expected, actual := 0, changes.i1A; expected != actual {
				t.Fatalf("expected %d, got %d", expected, actual)
			}
			if err := u.Set("i.1", settings.EncodeInt(1), "i"); err != nil {
				t.Fatal(err)
			}
			if expected, actual := 1, changes.i1A; expected != actual {
				t.Fatalf("expected %d, got %d", expected, actual)
			}
			if err := u.Set("i.2", settings.EncodeInt(7), "i"); err != nil {
				t.Fatal(err)
			}
			if err := u.Set("i.Val", settings.EncodeInt(1), "i"); err != nil {
				t.Fatal(err)
			}
			u.Done()
		}

		if expected, actual := true, boolFA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		// If the updater doesn't have a key, e.g. if the setting has been deleted,
		// Doneing it from the cache.
		settings.MakeResettingUpdater(r).Done()

		if expected, actual := 2, changes.boolTA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}

		if expected, actual := 2, changes.i1A; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}

		if expected, actual := false, boolFA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		if expected, actual := false, boolFA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("an invalid update to a given setting preserves its previously set value", func(t *testing.T) {
		{
			u := settings.MakeResettingUpdater(r)
			if err := u.Set("i.2", settings.EncodeInt(9), "i"); err != nil {
				t.Fatal(err)
			}
			u.Done()
		}
		before := i2A.Get()

		// Doneing after attempting to set with wrong type preserves the current
		// value.
		{
			u := settings.MakeResettingUpdater(r)
			if err := u.Set("i.2", settings.EncodeBool(false), "b"); !testutils.IsError(err,
				"setting 'i.2' defined as type i, not b",
			) {
				t.Fatal(err)
			}
			u.Done()
		}

		if expected, actual := before, i2A.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// Doneing after attempting to set with the wrong type preserves the
		// current value.
		{
			u := settings.MakeResettingUpdater(r)
			if err := u.Set("i.2", settings.EncodeBool(false), "i"); !testutils.IsError(err,
				"strconv.Atoi: parsing \"false\": invalid syntax",
			) {
				t.Fatal(err)
			}
			u.Done()
		}

		if expected, actual := before, i2A.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// Doneing after attempting to set with invalid value preserves the
		// current value.
		beforestrVal := strVal.Get()
		{
			u := settings.MakeResettingUpdater(r)
			if err := u.Set("str.val", "abc2def", "s"); !testutils.IsError(err,
				"not all runes of abc2def are letters: 2",
			) {
				t.Fatal(err)
			}
			u.Done()
		}
		if expected, actual := beforestrVal, strVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeDVal := dVal.Get()
		{
			u := settings.MakeResettingUpdater(r)
			if err := u.Set("dVal", settings.EncodeDuration(-time.Hour), "d"); !testutils.IsError(err,
				"cannot set dVal to a negative duration: -1h0m0s",
			) {
				t.Fatal(err)
			}
			u.Done()
		}
		if expected, actual := beforeDVal, dVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeByteSizeVal := byteSizeVal.Get()
		{
			u := settings.MakeResettingUpdater(r)
			if err := u.Set("byteSize.Val", settings.EncodeInt(-mb), "z"); !testutils.IsError(err,
				"bytesize cannot be negative",
			) {
				t.Fatal(err)
			}
			u.Done()
		}
		if expected, actual := beforeByteSizeVal, byteSizeVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeFVal := fVal.Get()
		{
			u := settings.MakeResettingUpdater(r)
			if err := u.Set("fVal", settings.EncodeFloat(-1.1), "f"); !testutils.IsError(err,
				"cannot set fVal to a negative value: -1.1",
			) {
				t.Fatal(err)
			}
			u.Done()
		}
		if expected, actual := beforeFVal, fVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeIVal := iVal.Get()
		{
			u := settings.MakeResettingUpdater(r)
			if err := u.Set("i.Val", settings.EncodeInt(-1), "i"); !testutils.IsError(err,
				"int cannot be negative",
			) {
				t.Fatal(err)
			}
			u.Done()
		}
		if expected, actual := beforeIVal, iVal.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeMarsh := mA.Get()
		{
			u := settings.MakeResettingUpdater(r)
			if err := u.Set("statemachine", "too.many.dots", "m"); !testutils.IsError(err,
				"expected two parts",
			) {
				t.Fatal(err)
			}
			u.Done()
		}
		if expected, actual := beforeMarsh, mA.Get(); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("mocks", func(t *testing.T) {
		{
			f := settings.TestingSetBool(&boolFA, true)
			if expected, actual := true, boolFA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
			f()
			if expected, actual := false, boolFA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
		}

		{
			f := settings.TestingSetString(&strBarA, "override")
			if expected, actual := "override", strBarA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
			f()
			if expected, actual := "bar", strBarA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
		}

		{
			f := settings.TestingSetInt(&i1A, 64)
			if expected, actual := int64(64), i1A.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
			f()
			if expected, actual := int64(0), i1A.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
		}

		{
			f := settings.TestingSetFloat(&fA, 6.7)
			if expected, actual := 6.7, fA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
			f()
			if expected, actual := 5.4, fA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
		}

		{
			f := settings.TestingSetDuration(&dA, 10*time.Hour)
			if expected, actual := 10*time.Hour, dA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
			f()
			if expected, actual := time.Second, dA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
		}

		{
			f := settings.TestingSetEnum(&eA, 3)
			if expected, actual := int64(3), eA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
			f()
			if expected, actual := int64(1), eA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
		}

		{
			f := settings.TestingSetByteSize(&byteSize, mb*7)
			if expected, actual := mb*7, byteSize.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
			f()
			if expected, actual := mb, byteSize.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
		}

		{
			f := settings.TestingSetStatemachine(&mA, settings.TransformerFn(
				func(_ []byte, _ *string) ([]byte, interface{}, error) {
					return []byte("encfoo"), "foo", nil
				},
			))
			if expected, actual := "encfoo", mA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
			f()
			if expected, actual := "default.-", mA.Get(); expected != actual {
				t.Fatalf("expected %v, got %v", expected, actual)
			}
		}
	})
}

func TestHide(t *testing.T) {
	keys := make(map[string]struct{})
	for _, k := range r.Keys() {
		keys[k] = struct{}{}
	}
	if _, ok := keys["bool.t"]; !ok {
		t.Errorf("expected 'bool.t' to be unhidden")
	}
	if _, ok := keys["sekretz"]; ok {
		t.Errorf("expected 'sekretz' to be hidden")
	}
}
