// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settings_test

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

const maxSettings = 256

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

func (d *dummy) MarshalTo(data []byte) (int, error) {
	encoded, err := d.Marshal()
	if err != nil {
		return 0, err
	}
	return copy(data, encoded), nil
}

func (d *dummy) Size() int {
	encoded, _ := d.Marshal()
	return len(encoded)
}

// implement the protoutil.Message interface
func (d *dummy) ProtoMessage() {}
func (d *dummy) Reset()        { *d = dummy{} }
func (d *dummy) String() string {
	return fmt.Sprintf("&{%s %s}", d.msg1, d.growsbyone)
}

var dummyTransformer = func(sv *settings.Values, old []byte, update *string) ([]byte, interface{}, error) {
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
	if err := protoutil.Unmarshal(old, &oldD); err != nil {
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

var boolTA = settings.RegisterBoolSetting("bool.t", "desc", true)
var boolFA = settings.RegisterBoolSetting("bool.f", "desc", false)
var strFooA = settings.RegisterStringSetting("str.foo", "desc", "")
var strBarA = settings.RegisterStringSetting("str.bar", "desc", "bar")
var i1A = settings.RegisterIntSetting("i.1", "desc", 0)
var i2A = settings.RegisterIntSetting("i.2", "desc", 5)
var fA = settings.RegisterFloatSetting("f", "desc", 5.4)
var dA = settings.RegisterDurationSetting("d", "desc", time.Second)
var eA = settings.RegisterEnumSetting("e", "desc", "foo", map[int64]string{1: "foo", 2: "bar", 3: "baz"})
var byteSize = settings.RegisterByteSizeSetting("zzz", "desc", mb)
var mA = settings.RegisterStateMachineSetting("statemachine", "foo", dummyTransformer)

func init() {
	settings.RegisterBoolSetting("sekretz", "desc", false).SetConfidential()
}

var strVal = settings.RegisterValidatedStringSetting(
	"str.val", "desc", "", func(sv *settings.Values, v string) error {
		for _, c := range v {
			if !unicode.IsLetter(c) {
				return errors.Errorf("not all runes of %s are letters: %c", v, c)
			}
		}
		return nil
	})
var dVal = settings.RegisterNonNegativeDurationSetting("dVal", "desc", time.Second)
var fVal = settings.RegisterNonNegativeFloatSetting("fVal", "desc", 5.4)
var byteSizeVal = settings.RegisterValidatedByteSizeSetting(
	"byteSize.Val", "desc", mb, func(v int64) error {
		if v < 0 {
			return errors.Errorf("bytesize cannot be negative")
		}
		return nil
	})
var iVal = settings.RegisterValidatedIntSetting(
	"i.Val", "desc", 0, func(v int64) error {
		if v < 0 {
			return errors.Errorf("int cannot be negative")
		}
		return nil
	})

func TestCache(t *testing.T) {
	sv := &settings.Values{}
	sv.Init(settings.TestOpaque)

	boolTA.SetOnChange(sv, func() { changes.boolTA++ })
	strFooA.SetOnChange(sv, func() { changes.strFooA++ })
	i1A.SetOnChange(sv, func() { changes.i1A++ })
	fA.SetOnChange(sv, func() { changes.fA++ })
	dA.SetOnChange(sv, func() { changes.dA++ })
	eA.SetOnChange(sv, func() { changes.eA++ })
	byteSize.SetOnChange(sv, func() { changes.byteSize++ })
	mA.SetOnChange(sv, func() { changes.mA++ })

	t.Run("StateMachineSetting", func(t *testing.T) {
		mB := settings.RegisterStateMachineSetting("local.m", "foo", dummyTransformer)
		if exp, act := "&{default -}", mB.String(sv); exp != act {
			t.Fatalf("wanted %q, got %q", exp, act)
		}
		growsTooFast := "grows too fast"
		if _, _, err := mB.Validate(sv, nil, &growsTooFast); !testutils.IsError(err, "must grow by exactly one") {
			t.Fatal(err)
		}
		hasDots := "a."
		if _, _, err := mB.Validate(sv, nil, &hasDots); !testutils.IsError(err, "must not contain dots") {
			t.Fatal(err)
		}
		ab := "ab"
		if _, _, err := mB.Validate(sv, nil, &ab); err != nil {
			t.Fatal(err)
		}
		if _, _, err := mB.Validate(sv, []byte("takes.precedence"), &ab); !testutils.IsError(err, "must grow by exactly one") {
			t.Fatal(err)
		}
		precedenceX := "precedencex"
		if _, _, err := mB.Validate(sv, []byte("takes.precedence"), &precedenceX); err != nil {
			t.Fatal(err)
		}
		u := settings.NewUpdater(sv)
		if err := u.Set("local.m", "default.XX", "m"); err != nil {
			t.Fatal(err)
		}
		u.ResetRemaining()
		if exp, act := "&{default XX}", mB.String(sv); exp != act {
			t.Fatalf("wanted %q, got %q", exp, act)
		}
	})

	t.Run("defaults", func(t *testing.T) {
		if expected, actual := false, boolFA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, boolTA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "", strFooA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bar", strBarA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "", strVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(0), i1A.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(5), i2A.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(0), iVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 5.4, fA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 5.4, fVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := time.Second, dA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := time.Second, dVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := mb, byteSize.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := mb, byteSizeVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(1), eA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "default.-", mA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("lookup", func(t *testing.T) {
		if actual, ok := settings.Lookup("i.1"); !ok || i1A != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", i1A, actual, ok)
		}
		if actual, ok := settings.Lookup("i.Val"); !ok || iVal != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", iVal, actual, ok)
		}
		if actual, ok := settings.Lookup("f"); !ok || fA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", fA, actual, ok)
		}
		if actual, ok := settings.Lookup("fVal"); !ok || fVal != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", fVal, actual, ok)
		}
		if actual, ok := settings.Lookup("d"); !ok || dA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", dA, actual, ok)
		}
		if actual, ok := settings.Lookup("dVal"); !ok || dVal != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", dVal, actual, ok)
		}
		if actual, ok := settings.Lookup("e"); !ok || eA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", eA, actual, ok)
		}
		if actual, ok := settings.Lookup("statemachine"); !ok || mA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", mA, actual, ok)
		}
		if actual, ok := settings.Lookup("dne"); ok {
			t.Fatalf("expected nothing, got %v", actual)
		}
	})

	t.Run("read and write each type", func(t *testing.T) {
		u := settings.NewUpdater(sv)
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
		u.ResetRemaining()

		if expected, actual := false, boolTA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, boolFA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "baz", strFooA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "valid", strVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(3), i2A.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 3.1, fA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 3.1, fVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 2*time.Hour, dA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := 2*time.Hour, dVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(2), eA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := mb*5, byteSize.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := mb*5, byteSizeVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "default.AB", mA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// We didn't change this one, so should still see the default.
		if expected, actual := "bar", strBarA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("any setting not included in an Updater reverts to default", func(t *testing.T) {
		{
			u := settings.NewUpdater(sv)
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
			u.ResetRemaining()
		}

		if expected, actual := true, boolFA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		// If the updater doesn't have a key, e.g. if the setting has been deleted,
		// Doneing it from the cache.
		settings.NewUpdater(sv).ResetRemaining()

		if expected, actual := 2, changes.boolTA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}

		if expected, actual := 2, changes.i1A; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}

		if expected, actual := false, boolFA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		if expected, actual := false, boolFA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("an invalid update to a given setting preserves its previously set value", func(t *testing.T) {
		{
			u := settings.NewUpdater(sv)
			if err := u.Set("i.2", settings.EncodeInt(9), "i"); err != nil {
				t.Fatal(err)
			}
			u.ResetRemaining()
		}
		before := i2A.Get(sv)

		// Doneing after attempting to set with wrong type preserves the current
		// value.
		{
			u := settings.NewUpdater(sv)
			if err := u.Set("i.2", settings.EncodeBool(false), "b"); !testutils.IsError(err,
				"setting 'i.2' defined as type i, not b",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining()
		}

		if expected, actual := before, i2A.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// Doneing after attempting to set with the wrong type preserves the
		// current value.
		{
			u := settings.NewUpdater(sv)
			if err := u.Set("i.2", settings.EncodeBool(false), "i"); !testutils.IsError(err,
				"strconv.Atoi: parsing \"false\": invalid syntax",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining()
		}

		if expected, actual := before, i2A.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// Doneing after attempting to set with invalid value preserves the
		// current value.
		beforestrVal := strVal.Get(sv)
		{
			u := settings.NewUpdater(sv)
			if err := u.Set("str.val", "abc2def", "s"); !testutils.IsError(err,
				"not all runes of abc2def are letters: 2",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining()
		}
		if expected, actual := beforestrVal, strVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeDVal := dVal.Get(sv)
		{
			u := settings.NewUpdater(sv)
			if err := u.Set("dVal", settings.EncodeDuration(-time.Hour), "d"); !testutils.IsError(err,
				"cannot set dVal to a negative duration: -1h0m0s",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining()
		}
		if expected, actual := beforeDVal, dVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeByteSizeVal := byteSizeVal.Get(sv)
		{
			u := settings.NewUpdater(sv)
			if err := u.Set("byteSize.Val", settings.EncodeInt(-mb), "z"); !testutils.IsError(err,
				"bytesize cannot be negative",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining()
		}
		if expected, actual := beforeByteSizeVal, byteSizeVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeFVal := fVal.Get(sv)
		{
			u := settings.NewUpdater(sv)
			if err := u.Set("fVal", settings.EncodeFloat(-1.1), "f"); !testutils.IsError(err,
				"cannot set fVal to a negative value: -1.1",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining()
		}
		if expected, actual := beforeFVal, fVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeIVal := iVal.Get(sv)
		{
			u := settings.NewUpdater(sv)
			if err := u.Set("i.Val", settings.EncodeInt(-1), "i"); !testutils.IsError(err,
				"int cannot be negative",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining()
		}
		if expected, actual := beforeIVal, iVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeMarsh := mA.Get(sv)
		{
			u := settings.NewUpdater(sv)
			if err := u.Set("statemachine", "too.many.dots", "m"); !testutils.IsError(err,
				"expected two parts",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining()
		}
		if expected, actual := beforeMarsh, mA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

}

func TestHide(t *testing.T) {
	keys := make(map[string]struct{})
	for _, k := range settings.Keys() {
		keys[k] = struct{}{}
	}
	if _, ok := keys["bool.t"]; !ok {
		t.Errorf("expected 'bool.t' to be unhidden")
	}
	if _, ok := keys["sekretz"]; ok {
		t.Errorf("expected 'sekretz' to be hidden")
	}
}

func TestOnChangeWithMaxSettings(t *testing.T) {
	// Register maxSettings settings to ensure that no errors occur.
	maxName, err := batchRegisterSettings(t, t.Name(), maxSettings-1-len(settings.Keys()))
	if err != nil {
		t.Errorf("expected no error to register 128 settings, but get error : %s", err)
	}

	// Change the max slotIdx setting to ensure that no errors occur.
	sv := &settings.Values{}
	sv.Init(settings.TestOpaque)
	var changes int
	intSetting, ok := settings.Lookup(maxName)
	if !ok {
		t.Errorf("expected lookup of %s to succeed", maxName)
	}
	intSetting.SetOnChange(sv, func() { changes++ })

	u := settings.NewUpdater(sv)
	if err := u.Set(maxName, settings.EncodeInt(9), "i"); err != nil {
		t.Fatal(err)
	}

	if changes != 1 {
		t.Errorf("expected the max slot setting changed")
	}
}

func TestMaxSettingsPanics(t *testing.T) {
	var origRegistry = make(map[string]settings.Setting)
	for k, v := range settings.Registry {
		origRegistry[k] = v
	}
	defer func() {
		settings.Registry = origRegistry
	}()

	// Register too many settings which will cause a panic which is caught and converted to an error.
	_, err := batchRegisterSettings(t, t.Name(), maxSettings-len(settings.Keys()))
	expectedErr := "too many settings; increase maxSettings"
	if err == nil || err.Error() != expectedErr {
		t.Errorf("expected error %v, but got %v", expectedErr, err)
	}

}

func batchRegisterSettings(t *testing.T, keyPrefix string, count int) (name string, err error) {
	defer func() {
		// Catch panic and convert it to an error.
		if r := recover(); r != nil {
			// Check exactly what the panic was and create error.
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.Errorf("unknown panic: %v", x)
			}
		}
	}()
	for i := 0; i < count; i++ {
		name = fmt.Sprintf("%s_%3d", keyPrefix, i)
		settings.RegisterValidatedIntSetting(name, "desc", 0, nil)
	}
	return name, err
}

var overrideBool = settings.RegisterBoolSetting("override.bool", "desc", true)
var overrideInt = settings.RegisterIntSetting("override.int", "desc", 0)
var overrideDuration = settings.RegisterDurationSetting("override.duration", "desc", time.Second)
var overrideFloat = settings.RegisterFloatSetting("override.float", "desc", 1.0)

func TestOverride(t *testing.T) {
	sv := &settings.Values{}
	sv.Init(settings.TestOpaque)

	// Test override for bool setting.
	require.Equal(t, true, overrideBool.Get(sv))
	overrideBool.Override(sv, false)
	require.Equal(t, false, overrideBool.Get(sv))
	u := settings.NewUpdater(sv)
	u.ResetRemaining()
	require.Equal(t, false, overrideBool.Get(sv))

	// Test override for int setting.
	require.Equal(t, int64(0), overrideInt.Get(sv))
	overrideInt.Override(sv, 42)
	require.Equal(t, int64(42), overrideInt.Get(sv))
	u.ResetRemaining()
	require.Equal(t, int64(42), overrideInt.Get(sv))

	// Test override for duration setting.
	require.Equal(t, time.Second, overrideDuration.Get(sv))
	overrideDuration.Override(sv, 42*time.Second)
	require.Equal(t, 42*time.Second, overrideDuration.Get(sv))
	u.ResetRemaining()
	require.Equal(t, 42*time.Second, overrideDuration.Get(sv))

	// Test override for float setting.
	require.Equal(t, 1.0, overrideFloat.Get(sv))
	overrideFloat.Override(sv, 42.0)
	require.Equal(t, 42.0, overrideFloat.Get(sv))
	u.ResetRemaining()
	require.Equal(t, 42.0, overrideFloat.Get(sv))
}
