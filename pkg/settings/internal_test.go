// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var b = RegisterBoolSetting(SystemOnly, "b", "desc", true)

func TestIgnoreDefaults(t *testing.T) {
	ctx := context.Background()
	sv := &Values{}
	sv.Init(ctx, TestOpaque)

	ignoreAllUpdates = true
	defer func() { ignoreAllUpdates = false }()
	u := NewUpdater(sv)
	require.NoError(t, u.Set(ctx, b.InternalKey(), EncodedValue{Value: EncodeBool(false), Type: "b"}))
	require.Equal(t, true, b.Get(sv))

	ignoreAllUpdates = false
	u = NewUpdater(sv)
	require.NoError(t, u.Set(ctx, b.InternalKey(), EncodedValue{Value: EncodeBool(false), Type: "b"}))
	require.Equal(t, false, b.Get(sv))
}

var sensitiveSetting = RegisterStringSetting(
	ApplicationLevel,
	"my.sensitive.setting",
	"description",
	"",
	Sensitive,
)

func TestSensitiveSetting(t *testing.T) {
	ctx := context.Background()
	sv := &Values{}
	sv.Init(ctx, TestOpaque)

	require.True(t, sensitiveSetting.isSensitive())
	u := NewUpdater(sv)
	require.NoError(t, u.Set(ctx, sensitiveSetting.InternalKey(), EncodedValue{Value: "foo", Type: "s"}))

	for _, canViewSensitive := range []bool{true, false} {
		t.Run(fmt.Sprintf("%s=%v", "canViewSensitive", canViewSensitive), func(t *testing.T) {
			for _, redactionEnabled := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s=%v", "redactionEnabled", redactionEnabled), func(t *testing.T) {
					redactSensitiveSettingsEnabled.Override(ctx, sv, redactionEnabled)
					underTest, ok := LookupForDisplayByKey(sensitiveSetting.InternalKey(), ForSystemTenant, canViewSensitive)
					require.True(t, ok)

					expectedValue := "foo"
					var expectedType Setting
					if canViewSensitive {
						expectedType = &StringSetting{}
					} else {
						expectedType = &MaskedSetting{}
						if redactionEnabled {
							expectedValue = "<redacted>"
						}
					}

					require.IsType(t, expectedType, underTest)
					actual := underTest.String(sv)
					require.Equal(t, expectedValue, actual)
				})
			}
		})
	}
}
