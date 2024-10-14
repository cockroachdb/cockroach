// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

var cantBeTrue = settings.WithValidateBool(func(sv *settings.Values, b bool) error {
	if b {
		return fmt.Errorf("it cant be true")
	}
	return nil
})

var cantBeFalse = settings.WithValidateBool(func(sv *settings.Values, b bool) error {
	if !b {
		return fmt.Errorf("it cant be false")
	}
	return nil
})

func TestValidationOptions(t *testing.T) {
	type subTest struct {
		val         interface{}
		opt         settings.SettingOption
		expectedErr string
	}
	type testCase struct {
		testLabel string
		settingFn func(n int, val interface{}, opt settings.SettingOption) settings.Setting
		subTests  []subTest
	}
	testCases := []testCase{
		{
			testLabel: "duration",
			settingFn: func(n int, dval interface{}, opt settings.SettingOption) settings.Setting {
				val := dval.(time.Duration)
				return settings.RegisterDurationSetting(settings.SystemOnly, settings.InternalKey(fmt.Sprintf("test-%d", n)), "desc",
					val, opt)
			},
			subTests: []subTest{
				{val: time.Duration(-1), opt: settings.PositiveDuration, expectedErr: "cannot be set to a non-positive duration: -1ns"},
				{val: time.Duration(0), opt: settings.PositiveDuration, expectedErr: "cannot be set to a non-positive duration: 0s"},
				{val: time.Duration(1), opt: settings.PositiveDuration, expectedErr: ""},
				{val: time.Duration(-1), opt: settings.NonNegativeDuration, expectedErr: "cannot be set to a negative duration: -1ns"},
				{val: time.Duration(0), opt: settings.NonNegativeDuration, expectedErr: ""},
				{val: time.Duration(1), opt: settings.NonNegativeDuration, expectedErr: ""},
				{val: time.Duration(-1), opt: settings.DurationWithMinimum(10), expectedErr: "cannot be set to a negative duration: -1ns"},
				{val: time.Duration(1), opt: settings.DurationWithMinimum(10), expectedErr: "cannot be set to a value smaller than 10ns"},
				{val: time.Duration(10), opt: settings.DurationWithMinimum(10), expectedErr: ""},
				{val: time.Duration(11), opt: settings.DurationWithMinimum(10), expectedErr: ""},
				{val: time.Duration(-11), opt: settings.DurationWithMinimum(-10), expectedErr: "cannot be set to a value smaller than -10ns"},
				{val: time.Duration(-10), opt: settings.DurationWithMinimum(-10), expectedErr: ""},
				{val: time.Duration(0), opt: settings.DurationWithMinimum(-10), expectedErr: ""},
				{val: time.Duration(10), opt: settings.DurationWithMinimum(-10), expectedErr: ""},
				{val: time.Duration(-1), opt: settings.DurationWithMinimumOrZeroDisable(10), expectedErr: "cannot be set to a negative duration: -1ns"},
				{val: time.Duration(0), opt: settings.DurationWithMinimumOrZeroDisable(10), expectedErr: ""},
				{val: time.Duration(1), opt: settings.DurationWithMinimumOrZeroDisable(10), expectedErr: "cannot be set to a value smaller than 10ns"},
				{val: time.Duration(10), opt: settings.DurationWithMinimumOrZeroDisable(10), expectedErr: ""},
				{val: time.Duration(11), opt: settings.DurationWithMinimumOrZeroDisable(10), expectedErr: ""},
				{val: time.Duration(-1), opt: settings.NonNegativeDurationWithMaximum(10), expectedErr: `expected value in range \[0s, 10ns\], got: -1ns`},
				{val: time.Duration(0), opt: settings.NonNegativeDurationWithMaximum(10), expectedErr: ""},
				{val: time.Duration(1), opt: settings.NonNegativeDurationWithMaximum(10), expectedErr: ""},
				{val: time.Duration(10), opt: settings.NonNegativeDurationWithMaximum(10), expectedErr: ""},
				{val: time.Duration(11), opt: settings.NonNegativeDurationWithMaximum(10), expectedErr: `expected value in range \[0s, 10ns\], got: 11ns`},
				{val: time.Duration(0), opt: settings.DurationInRange(10, 20), expectedErr: `expected value in range \[10ns, 20ns\], got: 0s`},
				{val: time.Duration(10), opt: settings.DurationInRange(10, 20), expectedErr: ""},
				{val: time.Duration(11), opt: settings.DurationInRange(10, 20), expectedErr: ""},
				{val: time.Duration(20), opt: settings.DurationInRange(10, 20), expectedErr: ""},
				{val: time.Duration(21), opt: settings.DurationInRange(10, 20), expectedErr: `expected value in range \[10ns, 20ns\], got: 21ns`},
			},
		},
		{
			testLabel: "float",
			settingFn: func(n int, fval interface{}, opt settings.SettingOption) settings.Setting {
				val := fval.(float64)
				return settings.RegisterFloatSetting(settings.SystemOnly, settings.InternalKey(fmt.Sprintf("test-%d", n)), "desc",
					val, opt)
			},
			subTests: []subTest{
				{val: -1.0, opt: settings.PositiveFloat, expectedErr: "cannot set to a non-positive value: -1.000000"},
				{val: 0.0, opt: settings.PositiveFloat, expectedErr: "cannot set to a non-positive value: 0.000000"},
				{val: 1.0, opt: settings.PositiveFloat, expectedErr: ""},
				{val: -1.0, opt: settings.NonNegativeFloat, expectedErr: "cannot set to a negative value: -1.000000"},
				{val: 0.0, opt: settings.NonNegativeFloat, expectedErr: ""},
				{val: 1.0, opt: settings.NonNegativeFloat, expectedErr: ""},
				{val: -1.0, opt: settings.NonZeroFloat, expectedErr: ""},
				{val: 0.0, opt: settings.NonZeroFloat, expectedErr: "cannot set to zero value"},
				{val: 1.0, opt: settings.NonZeroFloat, expectedErr: ""},
				{val: -1.0, opt: settings.FloatWithMinimum(10), expectedErr: "cannot set to a negative value: -1.000000"},
				{val: 1.0, opt: settings.FloatWithMinimum(10), expectedErr: "cannot set to a value lower than 10.000000: 1.000000"},
				{val: 10.0, opt: settings.FloatWithMinimum(10), expectedErr: ""},
				{val: 11.0, opt: settings.FloatWithMinimum(10), expectedErr: ""},
				{val: -11.0, opt: settings.FloatWithMinimum(-10), expectedErr: "cannot set to a value lower than -10.000000: -11.000000"},
				{val: -10.0, opt: settings.FloatWithMinimum(-10), expectedErr: ""},
				{val: 0.0, opt: settings.FloatWithMinimum(-10), expectedErr: ""},
				{val: 10.0, opt: settings.FloatWithMinimum(-10), expectedErr: ""},
				{val: -1.0, opt: settings.FloatWithMinimumOrZeroDisable(10), expectedErr: "cannot set to a negative value: -1.000000"},
				{val: 0.0, opt: settings.FloatWithMinimumOrZeroDisable(10), expectedErr: ""},
				{val: 1.0, opt: settings.FloatWithMinimumOrZeroDisable(10), expectedErr: "cannot set to a value lower than 10.000000: 1.000000"},
				{val: 10.0, opt: settings.FloatWithMinimumOrZeroDisable(10), expectedErr: ""},
				{val: 11.0, opt: settings.FloatWithMinimumOrZeroDisable(10), expectedErr: ""},
				{val: -1.0, opt: settings.NonNegativeFloatWithMaximum(10), expectedErr: `expected value in range \[0.000000, 10.000000\], got: -1.000000`},
				{val: 0.0, opt: settings.NonNegativeFloatWithMaximum(10), expectedErr: ""},
				{val: 1.0, opt: settings.NonNegativeFloatWithMaximum(10), expectedErr: ""},
				{val: 10.0, opt: settings.NonNegativeFloatWithMaximum(10), expectedErr: ""},
				{val: 11.0, opt: settings.NonNegativeFloatWithMaximum(10), expectedErr: `expected value in range \[0.000000, 10.000000\], got: 11.000000`},
				{val: 0.0, opt: settings.Fraction, expectedErr: ""},
				{val: 0.5, opt: settings.Fraction, expectedErr: ""},
				{val: 1.0, opt: settings.Fraction, expectedErr: ""},
				{val: 1.1, opt: settings.Fraction, expectedErr: `expected value in range \[0.000000, 1.000000\], got: 1.100000`},
				{val: -1.0, opt: settings.Fraction, expectedErr: `expected value in range \[0.000000, 1.000000\], got: -1.000000`},
				{val: 0.0, opt: settings.FractionUpperExclusive, expectedErr: ""},
				{val: 0.5, opt: settings.FractionUpperExclusive, expectedErr: ""},
				{val: 1.0, opt: settings.FractionUpperExclusive, expectedErr: `expected value in range \[0.000000, 1.000000\), got: 1.000000`},
				{val: 1.1, opt: settings.FractionUpperExclusive, expectedErr: `expected value in range \[0.000000, 1.000000\), got: 1.100000`},
				{val: -1.0, opt: settings.FractionUpperExclusive, expectedErr: `expected value in range \[0.000000, 1.000000\), got: -1.000000`},
				{val: 0.0, opt: settings.FloatInRange(10, 20), expectedErr: `expected value in range \[10.000000, 20.000000\], got: 0.000000`},
				{val: 10.0, opt: settings.FloatInRange(10, 20), expectedErr: ""},
				{val: 11.0, opt: settings.FloatInRange(10, 20), expectedErr: ""},
				{val: 20.0, opt: settings.FloatInRange(10, 20), expectedErr: ""},
				{val: 21.0, opt: settings.FloatInRange(10, 20), expectedErr: `expected value in range \[10.000000, 20.000000\], got: 21.000000`},
				{val: 0.0, opt: settings.FloatInRangeUpperExclusive(10, 20), expectedErr: `expected value in range \[10.000000, 20.000000\), got: 0.000000`},
				{val: 10.0, opt: settings.FloatInRangeUpperExclusive(10, 20), expectedErr: ""},
				{val: 11.0, opt: settings.FloatInRangeUpperExclusive(10, 20), expectedErr: ""},
				{val: 20.0, opt: settings.FloatInRangeUpperExclusive(10, 20), expectedErr: `expected value in range \[10.000000, 20.000000\), got: 20.000000`},
				{val: 21.0, opt: settings.FloatInRangeUpperExclusive(10, 20), expectedErr: `expected value in range \[10.000000, 20.000000\), got: 21.000000`},
			},
		},
		{
			testLabel: "int",
			settingFn: func(n int, ival interface{}, opt settings.SettingOption) settings.Setting {
				val := ival.(int)
				return settings.RegisterIntSetting(settings.SystemOnly, settings.InternalKey(fmt.Sprintf("test-%d", n)), "desc",
					int64(val), opt)
			},
			subTests: []subTest{
				{val: -1, opt: settings.PositiveInt, expectedErr: "cannot be set to a non-positive value: -1"},
				{val: 0, opt: settings.PositiveInt, expectedErr: "cannot be set to a non-positive value: 0"},
				{val: 1, opt: settings.PositiveInt, expectedErr: ""},
				{val: -1, opt: settings.NonNegativeInt, expectedErr: "cannot be set to a negative value: -1"},
				{val: 0, opt: settings.NonNegativeInt, expectedErr: ""},
				{val: 1, opt: settings.NonNegativeInt, expectedErr: ""},
				{val: -1, opt: settings.IntWithMinimum(10), expectedErr: "cannot be set to a negative value: -1"},
				{val: 1, opt: settings.IntWithMinimum(10), expectedErr: "cannot be set to a value lower than 10: 1"},
				{val: 10, opt: settings.IntWithMinimum(10), expectedErr: ""},
				{val: 11, opt: settings.IntWithMinimum(10), expectedErr: ""},
				{val: -11, opt: settings.IntWithMinimum(-10), expectedErr: "cannot be set to a value lower than -10: -11"},
				{val: -10, opt: settings.IntWithMinimum(-10), expectedErr: ""},
				{val: 0, opt: settings.IntWithMinimum(-10), expectedErr: ""},
				{val: 10, opt: settings.IntWithMinimum(-10), expectedErr: ""},
				{val: -1, opt: settings.NonNegativeIntWithMaximum(10), expectedErr: `expected value in range \[0, 10\], got: -1`},
				{val: 0, opt: settings.NonNegativeIntWithMaximum(10), expectedErr: ""},
				{val: 1, opt: settings.NonNegativeIntWithMaximum(10), expectedErr: ""},
				{val: 10, opt: settings.NonNegativeIntWithMaximum(10), expectedErr: ""},
				{val: 11, opt: settings.NonNegativeIntWithMaximum(10), expectedErr: `expected value in range \[0, 10\], got: 11`},
				{val: 0, opt: settings.IntInRange(10, 20), expectedErr: `expected value in range \[10, 20\], got: 0`},
				{val: 10, opt: settings.IntInRange(10, 20), expectedErr: ""},
				{val: 11, opt: settings.IntInRange(10, 20), expectedErr: ""},
				{val: 20, opt: settings.IntInRange(10, 20), expectedErr: ""},
				{val: 21, opt: settings.IntInRange(10, 20), expectedErr: `expected value in range \[10, 20\], got: 21`},
				{val: 0, opt: settings.IntInRangeOrZeroDisable(10, 20), expectedErr: ""},
				{val: 1, opt: settings.IntInRangeOrZeroDisable(10, 20), expectedErr: `expected value in range \[10, 20\] or 0 to disable, got: 1`},
				{val: 10, opt: settings.IntInRangeOrZeroDisable(10, 20), expectedErr: ""},
				{val: 11, opt: settings.IntInRangeOrZeroDisable(10, 20), expectedErr: ""},
				{val: 20, opt: settings.IntInRangeOrZeroDisable(10, 20), expectedErr: ""},
				{val: 21, opt: settings.IntInRangeOrZeroDisable(10, 20), expectedErr: `expected value in range \[10, 20\] or 0 to disable, got: 21`},
			},
		},
		{
			testLabel: "bytesize",
			settingFn: func(n int, ival interface{}, opt settings.SettingOption) settings.Setting {
				val := ival.(int)
				return settings.RegisterByteSizeSetting(settings.SystemOnly, settings.InternalKey(fmt.Sprintf("test-%d", n)), "desc",
					int64(val), opt)
			},
			subTests: []subTest{
				{val: -1, opt: settings.ByteSizeWithMinimum(10), expectedErr: "cannot be set to a value lower than 10 B"},
				{val: 1, opt: settings.ByteSizeWithMinimum(10), expectedErr: "cannot be set to a value lower than 10 B"},
				{val: 10, opt: settings.ByteSizeWithMinimum(10), expectedErr: ""},
				{val: 11, opt: settings.ByteSizeWithMinimum(10), expectedErr: ""},
			},
		},
		{
			testLabel: "bool",
			settingFn: func(n int, bval interface{}, opt settings.SettingOption) settings.Setting {
				val := bval.(bool)
				b := settings.RegisterBoolSetting(settings.SystemOnly, settings.InternalKey(fmt.Sprintf("test-%d", n)), "desc",
					val, opt)
				// We explicitly check here to test validation which does not happen on initialization.
				err := b.Validate(&settings.Values{}, val)
				if err != nil {
					panic(err)
				}
				return b
			},
			subTests: []subTest{
				{val: true, opt: cantBeTrue, expectedErr: "it cant be true"},
				{val: false, opt: cantBeTrue, expectedErr: ""},
				{val: true, opt: cantBeFalse, expectedErr: ""},
				{val: false, opt: cantBeFalse, expectedErr: "it cant be false"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testLabel, func(t *testing.T) {
			defer settings.TestingSaveRegistry()()

			for i, subTest := range tc.subTests {
				t.Run(strconv.Itoa(i), func(t *testing.T) {
					err := func() (resErr error) {
						defer func() {
							if r := recover(); r != nil {
								if rErr, ok := r.(error); ok {
									resErr = rErr
									return
								}
								panic(r)
							}
						}()
						_ = tc.settingFn(i, subTest.val, subTest.opt)
						return nil
					}()
					if err != nil {
						if subTest.expectedErr == "" {
							t.Errorf("unexpected error: %v", err)
						} else if !testutils.IsError(err, subTest.expectedErr) {
							t.Errorf("expected error %q, got %v", subTest.expectedErr, err)
						}
					} else if subTest.expectedErr != "" {
						t.Errorf("expected error %q, got nil", subTest.expectedErr)
					}
				})
			}
		})
	}
}
