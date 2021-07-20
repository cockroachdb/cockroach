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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// dummyVersion mocks out the dependency on the ClusterVersion type. It has a
// msg1 prefix, and a growsbyone component that grows by one character on each
// update (which is internally validated and asserted against). They're
// separated by a '.' in string form. Neither component can contain a '.'
// internally.
type dummyVersion struct {
	msg1       string
	growsbyone string
}

var _ settings.ClusterVersionImpl = &dummyVersion{}

func (d *dummyVersion) ClusterVersionImpl() {}

// Unmarshal is part of the protoutil.Message interface.
func (d *dummyVersion) Unmarshal(data []byte) error {
	s := string(data)
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return errors.Errorf("expected two parts, not %v", parts)
	}
	*d = dummyVersion{
		msg1: parts[0], growsbyone: parts[1],
	}
	return nil
}

// Marshal is part of the protoutil.Message interface.
func (d *dummyVersion) Marshal() ([]byte, error) {
	if c := d.msg1 + d.growsbyone; strings.Contains(c, ".") {
		return nil, errors.Newf("must not contain dots: %s", c)
	}
	return []byte(d.msg1 + "." + d.growsbyone), nil
}

// MarshalTo is part of the protoutil.Message interface.
func (d *dummyVersion) MarshalTo(data []byte) (int, error) {
	encoded, err := d.Marshal()
	if err != nil {
		return 0, err
	}
	return copy(data, encoded), nil
}

// MarshalToSizedBuffer is part of the protoutil.Message interface.
func (d *dummyVersion) MarshalToSizedBuffer(data []byte) (int, error) {
	encoded, err := d.Marshal()
	if err != nil {
		return 0, err
	}
	return copy(data, encoded), nil
}

// Size is part of the protoutil.Message interface.
func (d *dummyVersion) Size() int {
	encoded, _ := d.Marshal()
	return len(encoded)
}

// Implement the rest of the protoutil.Message interface.
func (d *dummyVersion) ProtoMessage()  {}
func (d *dummyVersion) Reset()         { *d = dummyVersion{} }
func (d *dummyVersion) String() string { return fmt.Sprintf("&{%s %s}", d.msg1, d.growsbyone) }

type dummyVersionSettingImpl struct{}

var _ settings.VersionSettingImpl = &dummyVersionSettingImpl{}

func (d *dummyVersionSettingImpl) Decode(val []byte) (settings.ClusterVersionImpl, error) {
	var oldD dummyVersion
	if err := protoutil.Unmarshal(val, &oldD); err != nil {
		return nil, err
	}
	return &oldD, nil
}

func (d *dummyVersionSettingImpl) Validate(
	ctx context.Context, sv *settings.Values, oldV, newV []byte,
) ([]byte, error) {
	var oldD dummyVersion
	if err := protoutil.Unmarshal(oldV, &oldD); err != nil {
		return nil, err
	}

	var newD dummyVersion
	if err := protoutil.Unmarshal(newV, &newD); err != nil {
		return nil, err
	}

	// We have a new proposed update to the value, validate it.
	if len(newD.growsbyone) != len(oldD.growsbyone)+1 {
		return nil, errors.New("dashes component must grow by exactly one")
	}

	return newD.Marshal()
}

func (d *dummyVersionSettingImpl) ValidateBinaryVersions(
	ctx context.Context, sv *settings.Values, val []byte,
) error {
	var updateVal dummyVersion
	return protoutil.Unmarshal(val, &updateVal)
}

func (d *dummyVersionSettingImpl) SettingsListDefault() string {
	panic("unimplemented")
}

const mb = int64(1024 * 1024)

var changes = struct {
	boolTA   int
	strFooA  int
	i1A      int
	fA       int
	dA       int
	duA      int
	eA       int
	byteSize int
}{}

var boolTA = settings.RegisterBoolSetting("bool.t", "desc", true)
var boolFA = settings.RegisterBoolSetting("bool.f", "desc", false)
var strFooA = settings.RegisterStringSetting("str.foo", "desc", "")
var strBarA = settings.RegisterStringSetting("str.bar", "desc", "bar")
var i1A = settings.RegisterIntSetting("i.1", "desc", 0)
var i2A = settings.RegisterIntSetting("i.2", "desc", 5)
var fA = settings.RegisterFloatSetting("f", "desc", 5.4)
var dA = settings.RegisterDurationSetting("d", "desc", time.Second)
var duA = settings.RegisterPublicDurationSettingWithExplicitUnit("d_with_explicit_unit", "desc", time.Second, settings.NonNegativeDuration)
var _ = settings.RegisterDurationSetting("d_with_maximum", "desc", time.Second, settings.NonNegativeDurationWithMaximum(time.Hour))
var eA = settings.RegisterEnumSetting("e", "desc", "foo", map[int64]string{1: "foo", 2: "bar", 3: "baz"})
var byteSize = settings.RegisterByteSizeSetting("zzz", "desc", mb)
var mA = settings.TestingRegisterVersionSetting("v.1", "desc", &dummyVersionSettingImpl{})

func init() {
	settings.RegisterBoolSetting("sekretz", "desc", false).SetReportable(false)
	settings.RegisterBoolSetting("rezervedz", "desc", false).SetVisibility(settings.Reserved)
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
var dVal = settings.RegisterDurationSetting("dVal", "desc", time.Second, settings.NonNegativeDuration)
var fVal = settings.RegisterFloatSetting("fVal", "desc", 5.4, settings.NonNegativeFloat)
var byteSizeVal = settings.RegisterByteSizeSetting(
	"byteSize.Val", "desc", mb, func(v int64) error {
		if v < 0 {
			return errors.Errorf("bytesize cannot be negative")
		}
		return nil
	})
var iVal = settings.RegisterIntSetting(
	"i.Val", "desc", 0, func(v int64) error {
		if v < 0 {
			return errors.Errorf("int cannot be negative")
		}
		return nil
	})

func TestValidation(t *testing.T) {
	ctx := context.Background()
	sv := &settings.Values{}
	sv.Init(ctx, settings.TestOpaque)

	u := settings.NewUpdater(sv)
	t.Run("d_with_maximum", func(t *testing.T) {
		err := u.Set(ctx, "d_with_maximum", "1h", "d")
		require.NoError(t, err)
		err = u.Set(ctx, "d_with_maximum", "0h", "d")
		require.NoError(t, err)
		err = u.Set(ctx, "d_with_maximum", "30m", "d")
		require.NoError(t, err)

		err = u.Set(ctx, "d_with_maximum", "-1m", "d")
		require.Error(t, err)
		err = u.Set(ctx, "d_with_maximum", "1h1s", "d")
		require.Error(t, err)
	})
}

func TestCache(t *testing.T) {
	ctx := context.Background()
	sv := &settings.Values{}
	sv.Init(ctx, settings.TestOpaque)

	boolTA.SetOnChange(sv, func(context.Context) { changes.boolTA++ })
	strFooA.SetOnChange(sv, func(context.Context) { changes.strFooA++ })
	i1A.SetOnChange(sv, func(context.Context) { changes.i1A++ })
	fA.SetOnChange(sv, func(context.Context) { changes.fA++ })
	dA.SetOnChange(sv, func(context.Context) { changes.dA++ })
	duA.SetOnChange(sv, func(context.Context) { changes.duA++ })
	eA.SetOnChange(sv, func(context.Context) { changes.eA++ })
	byteSize.SetOnChange(sv, func(context.Context) { changes.byteSize++ })

	t.Run("VersionSetting", func(t *testing.T) {
		ctx := context.Background()
		u := settings.NewUpdater(sv)
		mB := settings.TestingRegisterVersionSetting("local.m", "foo", &dummyVersionSettingImpl{})
		// Version settings don't have defaults, so we need to start by setting
		// it to something.
		defaultDummyV := dummyVersion{msg1: "default", growsbyone: "X"}
		if err := setDummyVersion(defaultDummyV, mB, sv); err != nil {
			t.Fatal(err)
		}

		if exp, act := "&{default X}", mB.String(sv); exp != act {
			t.Fatalf("wanted %q, got %q", exp, act)
		}

		growsTooFast := []byte("default.grows too fast")
		curVal := []byte(mB.Encoded(sv))
		if _, err := mB.Validate(ctx, sv, curVal, growsTooFast); !testutils.IsError(err,
			"must grow by exactly one") {
			t.Fatal(err)
		}

		hasDots := []byte("default.a.b.c")
		if _, err := mB.Validate(ctx, sv, curVal, hasDots); !testutils.IsError(err,
			"expected two parts") {
			t.Fatal(err)
		}

		ab := []byte("default.ab")
		if _, err := mB.Validate(ctx, sv, curVal, ab); err != nil {
			t.Fatal(err)
		}

		if _, err := mB.Validate(ctx, sv, []byte("takes.precedence"), ab); !testutils.IsError(err,
			"must grow by exactly one") {
			t.Fatal(err)
		}

		precedenceX := []byte("takes.precedencex")
		if _, err := mB.Validate(ctx, sv, []byte("takes.precedence"), precedenceX); err != nil {
			t.Fatal(err)
		}

		newDummyV := dummyVersion{msg1: "default", growsbyone: "XX"}
		if err := setDummyVersion(newDummyV, mB, sv); err != nil {
			t.Fatal(err)
		}
		u.ResetRemaining(ctx)
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
		if expected, actual := time.Second, duA.Get(sv); expected != actual {
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
		// Note that we don't test the state-machine setting for a default, since it
		// doesn't have one and it would crash.
	})

	t.Run("lookup", func(t *testing.T) {
		if actual, ok := settings.Lookup("i.1", settings.LookupForLocalAccess); !ok || i1A != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", i1A, actual, ok)
		}
		if actual, ok := settings.Lookup("i.Val", settings.LookupForLocalAccess); !ok || iVal != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", iVal, actual, ok)
		}
		if actual, ok := settings.Lookup("f", settings.LookupForLocalAccess); !ok || fA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", fA, actual, ok)
		}
		if actual, ok := settings.Lookup("fVal", settings.LookupForLocalAccess); !ok || fVal != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", fVal, actual, ok)
		}
		if actual, ok := settings.Lookup("d", settings.LookupForLocalAccess); !ok || dA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", dA, actual, ok)
		}
		if actual, ok := settings.Lookup("dVal", settings.LookupForLocalAccess); !ok || dVal != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", dVal, actual, ok)
		}
		if actual, ok := settings.Lookup("e", settings.LookupForLocalAccess); !ok || eA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", eA, actual, ok)
		}
		if actual, ok := settings.Lookup("v.1", settings.LookupForLocalAccess); !ok || mA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", mA, actual, ok)
		}
		if actual, ok := settings.Lookup("d_with_explicit_unit", settings.LookupForLocalAccess); !ok || duA != actual {
			t.Fatalf("expected %v, got %v (exists: %v)", duA, actual, ok)
		}
		if actual, ok := settings.Lookup("dne", settings.LookupForLocalAccess); ok {
			t.Fatalf("expected nothing, got %v", actual)
		}
	})

	t.Run("read and write each type", func(t *testing.T) {
		u := settings.NewUpdater(sv)
		if expected, actual := 0, changes.boolTA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "bool.t", settings.EncodeBool(false), "b"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.boolTA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "bool.f", settings.EncodeBool(true), "b"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 0, changes.strFooA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "str.foo", "baz", "s"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.strFooA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "str.val", "valid", "s"); err != nil {
			t.Fatal(err)
		}
		if err := u.Set(ctx, "i.2", settings.EncodeInt(3), "i"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 0, changes.fA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "f", settings.EncodeFloat(3.1), "f"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.fA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "fVal", settings.EncodeFloat(3.1), "f"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 0, changes.dA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "d", settings.EncodeDuration(2*time.Hour), "d"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.dA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if expected, actual := 0, changes.duA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "d_with_explicit_unit", settings.EncodeDuration(2*time.Hour), "d"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.duA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "dVal", settings.EncodeDuration(2*time.Hour), "d"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 0, changes.byteSize; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "zzz", settings.EncodeInt(mb*5), "z"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.byteSize; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "byteSize.Val", settings.EncodeInt(mb*5), "z"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 0, changes.eA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if err := u.Set(ctx, "e", settings.EncodeInt(2), "e"); err != nil {
			t.Fatal(err)
		}
		if expected, actual := 1, changes.eA; expected != actual {
			t.Fatalf("expected %d, got %d", expected, actual)
		}
		if expected, err := "strconv.Atoi: parsing \"notAValidValue\": invalid syntax",
			u.Set(ctx, "e", "notAValidValue", "e"); !testutils.IsError(err, expected) {
			t.Fatalf("expected '%s' != actual error '%s'", expected, err)
		}
		defaultDummyV := dummyVersion{msg1: "default", growsbyone: "AB"}
		if err := setDummyVersion(defaultDummyV, mA, sv); err != nil {
			t.Fatal(err)
		}
		u.ResetRemaining(ctx)

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
		if expected, actual := 2*time.Hour, duA.Get(sv); expected != actual {
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
		if expected, actual := "default.AB", mA.Encoded(sv); expected != actual {
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
			if err := u.Set(ctx, "bool.f", settings.EncodeBool(true), "b"); err != nil {
				t.Fatal(err)
			}
			if expected, actual := 0, changes.i1A; expected != actual {
				t.Fatalf("expected %d, got %d", expected, actual)
			}
			if err := u.Set(ctx, "i.1", settings.EncodeInt(1), "i"); err != nil {
				t.Fatal(err)
			}
			if expected, actual := 1, changes.i1A; expected != actual {
				t.Fatalf("expected %d, got %d", expected, actual)
			}
			if err := u.Set(ctx, "i.2", settings.EncodeInt(7), "i"); err != nil {
				t.Fatal(err)
			}
			if err := u.Set(ctx, "i.Val", settings.EncodeInt(1), "i"); err != nil {
				t.Fatal(err)
			}
			u.ResetRemaining(ctx)
		}

		if expected, actual := true, boolFA.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
		// If the updater doesn't have a key, e.g. if the setting has been deleted,
		// Doneing it from the cache.
		settings.NewUpdater(sv).ResetRemaining(ctx)

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
			if err := u.Set(ctx, "i.2", settings.EncodeInt(9), "i"); err != nil {
				t.Fatal(err)
			}
			u.ResetRemaining(ctx)
		}
		before := i2A.Get(sv)

		// Doneing after attempting to set with wrong type preserves the current
		// value.
		{
			u := settings.NewUpdater(sv)
			if err := u.Set(ctx, "i.2", settings.EncodeBool(false), "b"); !testutils.IsError(err,
				"setting 'i.2' defined as type i, not b",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining(ctx)
		}

		if expected, actual := before, i2A.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// Doneing after attempting to set with the wrong type preserves the
		// current value.
		{
			u := settings.NewUpdater(sv)
			if err := u.Set(ctx, "i.2", settings.EncodeBool(false), "i"); !testutils.IsError(err,
				"strconv.Atoi: parsing \"false\": invalid syntax",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining(ctx)
		}

		if expected, actual := before, i2A.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		// Doneing after attempting to set with invalid value preserves the
		// current value.
		beforestrVal := strVal.Get(sv)
		{
			u := settings.NewUpdater(sv)
			if err := u.Set(ctx, "str.val", "abc2def", "s"); !testutils.IsError(err,
				"not all runes of abc2def are letters: 2",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining(ctx)
		}
		if expected, actual := beforestrVal, strVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeDVal := dVal.Get(sv)
		{
			u := settings.NewUpdater(sv)
			if err := u.Set(ctx, "dVal", settings.EncodeDuration(-time.Hour), "d"); !testutils.IsError(err,
				"cannot be set to a negative duration: -1h0m0s",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining(ctx)
		}
		if expected, actual := beforeDVal, dVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeByteSizeVal := byteSizeVal.Get(sv)
		{
			u := settings.NewUpdater(sv)
			if err := u.Set(ctx, "byteSize.Val", settings.EncodeInt(-mb), "z"); !testutils.IsError(err,
				"bytesize cannot be negative",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining(ctx)
		}
		if expected, actual := beforeByteSizeVal, byteSizeVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeFVal := fVal.Get(sv)
		{
			u := settings.NewUpdater(sv)
			if err := u.Set(ctx, "fVal", settings.EncodeFloat(-1.1), "f"); !testutils.IsError(err,
				"cannot set to a negative value: -1.1",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining(ctx)
		}
		if expected, actual := beforeFVal, fVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}

		beforeIVal := iVal.Get(sv)
		{
			u := settings.NewUpdater(sv)
			if err := u.Set(ctx, "i.Val", settings.EncodeInt(-1), "i"); !testutils.IsError(err,
				"int cannot be negative",
			) {
				t.Fatal(err)
			}
			u.ResetRemaining(ctx)
		}
		if expected, actual := beforeIVal, iVal.Get(sv); expected != actual {
			t.Fatalf("expected %v, got %v", expected, actual)
		}
	})

}

func TestIsReportable(t *testing.T) {
	if v, ok := settings.Lookup("bool.t", settings.LookupForLocalAccess); !ok || !settings.TestingIsReportable(v) {
		t.Errorf("expected 'bool.t' to be marked as isReportable() = true")
	}
	if v, ok := settings.Lookup("sekretz", settings.LookupForLocalAccess); !ok || settings.TestingIsReportable(v) {
		t.Errorf("expected 'sekretz' to be marked as isReportable() = false")
	}
}

func TestOnChangeWithMaxSettings(t *testing.T) {
	ctx := context.Background()
	// Register MaxSettings settings to ensure that no errors occur.
	maxName, err := batchRegisterSettings(t, t.Name(), settings.MaxSettings-settings.NumRegisteredSettings())
	if err != nil {
		t.Fatalf("expected no error to register %d settings, but get error: %v", settings.MaxSettings, err)
	}

	// Change the max slotIdx setting to ensure that no errors occur.
	sv := &settings.Values{}
	sv.Init(ctx, settings.TestOpaque)
	var changes int
	s, ok := settings.Lookup(maxName, settings.LookupForLocalAccess)
	if !ok {
		t.Fatalf("expected lookup of %s to succeed", maxName)
	}
	intSetting, ok := s.(*settings.IntSetting)
	if !ok {
		t.Fatalf("expected int setting, got %T", s)
	}
	intSetting.SetOnChange(sv, func(ctx context.Context) { changes++ })

	u := settings.NewUpdater(sv)
	if err := u.Set(ctx, maxName, settings.EncodeInt(9), "i"); err != nil {
		t.Fatal(err)
	}

	if changes != 1 {
		t.Errorf("expected the max slot setting changed")
	}
}

func TestMaxSettingsPanics(t *testing.T) {
	defer settings.TestingSaveRegistry()()

	// Register too many settings which will cause a panic which is caught and converted to an error.
	_, err := batchRegisterSettings(t, t.Name(),
		settings.MaxSettings-settings.NumRegisteredSettings()+1)
	expectedErr := "too many settings; increase MaxSettings"
	if !testutils.IsError(err, expectedErr) {
		t.Errorf("expected error %v, but got %v", expectedErr, err)
	}

}

func batchRegisterSettings(t *testing.T, keyPrefix string, count int) (name string, err error) {
	defer func() {
		// Catch panic and convert it to an error.
		if r := recover(); r != nil {
			if panicErr, ok := r.(error); ok {
				err = errors.WithStackDepth(panicErr, 1)
			} else {
				err = errors.NewWithDepthf(1, "panic: %v", r)
			}
		}
	}()
	for i := 0; i < count; i++ {
		name = fmt.Sprintf("%s_%3d", keyPrefix, i)
		settings.RegisterIntSetting(name, "desc", 0)
	}
	return name, err
}

var overrideBool = settings.RegisterBoolSetting("override.bool", "desc", true)
var overrideInt = settings.RegisterIntSetting("override.int", "desc", 0)
var overrideDuration = settings.RegisterDurationSetting("override.duration", "desc", time.Second)
var overrideFloat = settings.RegisterFloatSetting("override.float", "desc", 1.0)

func TestOverride(t *testing.T) {
	ctx := context.Background()
	sv := &settings.Values{}
	sv.Init(ctx, settings.TestOpaque)

	// Test override for bool setting.
	require.Equal(t, true, overrideBool.Get(sv))
	overrideBool.Override(ctx, sv, false)
	require.Equal(t, false, overrideBool.Get(sv))
	u := settings.NewUpdater(sv)
	u.ResetRemaining(ctx)
	require.Equal(t, false, overrideBool.Get(sv))

	// Test override for int setting.
	require.Equal(t, int64(0), overrideInt.Get(sv))
	overrideInt.Override(ctx, sv, 42)
	require.Equal(t, int64(42), overrideInt.Get(sv))
	u.ResetRemaining(ctx)
	require.Equal(t, int64(42), overrideInt.Get(sv))

	// Test override for duration setting.
	require.Equal(t, time.Second, overrideDuration.Get(sv))
	overrideDuration.Override(ctx, sv, 42*time.Second)
	require.Equal(t, 42*time.Second, overrideDuration.Get(sv))
	u.ResetRemaining(ctx)
	require.Equal(t, 42*time.Second, overrideDuration.Get(sv))

	// Test override for float setting.
	require.Equal(t, 1.0, overrideFloat.Get(sv))
	overrideFloat.Override(ctx, sv, 42.0)
	require.Equal(t, 42.0, overrideFloat.Get(sv))
	u.ResetRemaining(ctx)
	require.Equal(t, 42.0, overrideFloat.Get(sv))
}

func setDummyVersion(dv dummyVersion, vs *settings.VersionSetting, sv *settings.Values) error {
	// This is a bit round about because the VersionSetting doesn't get updated
	// through the updater, like most other settings. In order to set it, we set
	// the internal encoded state by hand.
	encoded, err := protoutil.Marshal(&dv)
	if err != nil {
		return err
	}
	vs.SetInternal(context.Background(), sv, encoded)
	return nil
}
