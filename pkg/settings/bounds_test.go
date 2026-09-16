// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/stretchr/testify/require"
)

// TestIntSettingBounds verifies that the bounds encoded by IntSetting
// validation options are surfaced via Bounds() on the resulting *IntSetting.
func TestIntSettingBounds(t *testing.T) {
	defer settings.TestingSaveRegistry()()

	t.Run("IntInRange sets both bounds", func(t *testing.T) {
		s := settings.RegisterIntSetting(
			settings.SystemOnly, "test.int.in_range", "desc", 15,
			settings.IntInRange(10, 20),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.NotNil(t, max)
		require.Equal(t, int64(10), *min)
		require.Equal(t, int64(20), *max)
	})

	t.Run("IntWithMinimum sets only min", func(t *testing.T) {
		s := settings.RegisterIntSetting(
			settings.SystemOnly, "test.int.with_minimum", "desc", 5,
			settings.IntWithMinimum(5),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.Nil(t, max)
		require.Equal(t, int64(5), *min)
	})

	t.Run("IntInRangeOrZeroDisable sets both bounds", func(t *testing.T) {
		s := settings.RegisterIntSetting(
			settings.SystemOnly, "test.int.zero_disable", "desc", 10,
			settings.IntInRangeOrZeroDisable(10, 20),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.NotNil(t, max)
		require.Equal(t, int64(10), *min)
		require.Equal(t, int64(20), *max)
	})

	t.Run("NonNegativeIntWithMaximum sets [0, max]", func(t *testing.T) {
		s := settings.RegisterIntSetting(
			settings.SystemOnly, "test.int.nn_max", "desc", 5,
			settings.NonNegativeIntWithMaximum(100),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.NotNil(t, max)
		require.Equal(t, int64(0), *min)
		require.Equal(t, int64(100), *max)
	})

	t.Run("PositiveInt sets no bounds", func(t *testing.T) {
		s := settings.RegisterIntSetting(
			settings.SystemOnly, "test.int.positive", "desc", 1,
			settings.PositiveInt,
		)
		min, max := s.Bounds()
		require.Nil(t, min)
		require.Nil(t, max)
	})
}

// TestByteSizeSettingBounds verifies bounds on ByteSizeSetting, which embeds
// IntSetting.
func TestByteSizeSettingBounds(t *testing.T) {
	defer settings.TestingSaveRegistry()()

	t.Run("ByteSizeWithMinimum sets only min", func(t *testing.T) {
		s := settings.RegisterByteSizeSetting(
			settings.SystemOnly, "test.bs.with_minimum", "desc", 1024,
			settings.ByteSizeWithMinimum(1024),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.Nil(t, max)
		require.Equal(t, int64(1024), *min)
	})

	t.Run("ByteSizeWithMaximum sets only max", func(t *testing.T) {
		s := settings.RegisterByteSizeSetting(
			settings.SystemOnly, "test.bs.with_maximum", "desc", 1024,
			settings.ByteSizeWithMaximum(65536),
		)
		min, max := s.Bounds()
		require.Nil(t, min)
		require.NotNil(t, max)
		require.Equal(t, int64(65536), *max)
	})
}

// TestDurationSettingBounds verifies bounds on DurationSetting.
func TestDurationSettingBounds(t *testing.T) {
	defer settings.TestingSaveRegistry()()

	t.Run("DurationInRange sets both bounds", func(t *testing.T) {
		s := settings.RegisterDurationSetting(
			settings.SystemOnly, "test.dur.in_range", "desc", time.Second,
			settings.DurationInRange(time.Second, time.Minute),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.NotNil(t, max)
		require.Equal(t, time.Second, *min)
		require.Equal(t, time.Minute, *max)
	})

	t.Run("DurationWithMinimum sets only min", func(t *testing.T) {
		s := settings.RegisterDurationSetting(
			settings.SystemOnly, "test.dur.with_minimum", "desc", time.Second,
			settings.DurationWithMinimum(time.Second),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.Nil(t, max)
		require.Equal(t, time.Second, *min)
	})

	t.Run("DurationWithMinimumOrZeroDisable sets only min", func(t *testing.T) {
		s := settings.RegisterDurationSetting(
			settings.SystemOnly, "test.dur.zero_disable", "desc", time.Second,
			settings.DurationWithMinimumOrZeroDisable(time.Second),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.Nil(t, max)
		require.Equal(t, time.Second, *min)
	})

	t.Run("NonNegativeDurationWithMaximum sets [0, max]", func(t *testing.T) {
		s := settings.RegisterDurationSetting(
			settings.SystemOnly, "test.dur.nn_max", "desc", time.Second,
			settings.NonNegativeDurationWithMaximum(time.Hour),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.NotNil(t, max)
		require.Equal(t, time.Duration(0), *min)
		require.Equal(t, time.Hour, *max)
	})

	t.Run("PositiveDuration sets no bounds", func(t *testing.T) {
		s := settings.RegisterDurationSetting(
			settings.SystemOnly, "test.dur.positive", "desc", time.Second,
			settings.PositiveDuration,
		)
		min, max := s.Bounds()
		require.Nil(t, min)
		require.Nil(t, max)
	})

	t.Run("DurationSettingWithExplicitUnit inherits bounds", func(t *testing.T) {
		s := settings.RegisterDurationSettingWithExplicitUnit(
			settings.SystemOnly, "test.dur.explicit", "desc", time.Second,
			settings.DurationInRange(time.Second, time.Minute),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.NotNil(t, max)
		require.Equal(t, time.Second, *min)
		require.Equal(t, time.Minute, *max)
	})
}

// TestFloatSettingBounds verifies bounds on FloatSetting.
func TestFloatSettingBounds(t *testing.T) {
	defer settings.TestingSaveRegistry()()

	t.Run("FloatInRange sets both bounds", func(t *testing.T) {
		s := settings.RegisterFloatSetting(
			settings.SystemOnly, "test.f.in_range", "desc", 0.5,
			settings.FloatInRange(0.0, 1.0),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.NotNil(t, max)
		require.Equal(t, 0.0, *min)
		require.Equal(t, 1.0, *max)
	})

	t.Run("FloatInRangeUpperExclusive sets both bounds", func(t *testing.T) {
		s := settings.RegisterFloatSetting(
			settings.SystemOnly, "test.f.upper_exclusive", "desc", 0.5,
			settings.FloatInRangeUpperExclusive(0.0, 1.0),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.NotNil(t, max)
		require.Equal(t, 0.0, *min)
		require.Equal(t, 1.0, *max)
	})

	t.Run("FloatWithMinimum sets only min", func(t *testing.T) {
		s := settings.RegisterFloatSetting(
			settings.SystemOnly, "test.f.with_minimum", "desc", 1.5,
			settings.FloatWithMinimum(1.5),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.Nil(t, max)
		require.Equal(t, 1.5, *min)
	})

	t.Run("FloatWithMinimumOrZeroDisable sets only min", func(t *testing.T) {
		s := settings.RegisterFloatSetting(
			settings.SystemOnly, "test.f.zero_disable", "desc", 1.5,
			settings.FloatWithMinimumOrZeroDisable(1.5),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.Nil(t, max)
		require.Equal(t, 1.5, *min)
	})

	t.Run("NonNegativeFloatWithMaximum sets [0, max]", func(t *testing.T) {
		s := settings.RegisterFloatSetting(
			settings.SystemOnly, "test.f.nn_max", "desc", 0.5,
			settings.NonNegativeFloatWithMaximum(0.99),
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.NotNil(t, max)
		require.Equal(t, 0.0, *min)
		require.Equal(t, 0.99, *max)
	})

	t.Run("Fraction sets [0, 1]", func(t *testing.T) {
		s := settings.RegisterFloatSetting(
			settings.SystemOnly, "test.f.fraction", "desc", 0.5,
			settings.Fraction,
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.NotNil(t, max)
		require.Equal(t, 0.0, *min)
		require.Equal(t, 1.0, *max)
	})

	t.Run("FractionUpperExclusive sets [0, 1]", func(t *testing.T) {
		s := settings.RegisterFloatSetting(
			settings.SystemOnly, "test.f.fraction_ue", "desc", 0.5,
			settings.FractionUpperExclusive,
		)
		min, max := s.Bounds()
		require.NotNil(t, min)
		require.NotNil(t, max)
		require.Equal(t, 0.0, *min)
		require.Equal(t, 1.0, *max)
	})

	t.Run("PositiveFloat sets no bounds", func(t *testing.T) {
		s := settings.RegisterFloatSetting(
			settings.SystemOnly, "test.f.positive", "desc", 1.0,
			settings.PositiveFloat,
		)
		min, max := s.Bounds()
		require.Nil(t, min)
		require.Nil(t, max)
	})
}
