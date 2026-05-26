// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var unsafeSetting = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"my.unsafe.setting", "unused", false,
	settings.WithUnsafe)
var otherUnsafeSetting = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"my.other.unsafe.setting", "unused", false,
	settings.WithUnsafe)

func TestSetUnsafeClusterSettingInterlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	unsafeSettingName := unsafeSetting.Name()

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ts := s.ApplicationLayer()

	firstSession := ts.SQLConn(t)

	// RESET on unsafe settings never get an error.
	_, err := firstSession.Exec(fmt.Sprintf("RESET CLUSTER SETTING %s", unsafeSettingName))
	require.NoError(t, err)

	// Try changing the setting. We're expecting an error.
	_, err = firstSession.Exec(fmt.Sprintf("SET CLUSTER SETTING %s = true", unsafeSettingName))
	require.Error(t, err)
	getKey := func(err error) string {
		require.Contains(t, err.Error(), "may cause cluster instability")
		var pqErr *pq.Error
		ok := errors.As(err, &pqErr) // Change this if/when we change the driver for tests.
		require.True(t, ok)
		require.True(t, strings.HasPrefix(pqErr.Detail, "key:"), pqErr.Detail)
		return strings.TrimPrefix(pqErr.Detail, "key: ")
	}
	key := getKey(err)

	// Now set the key and try again. We're not expecting an error any more.
	_, err = firstSession.Exec("SET unsafe_setting_interlock_key = $1", key)
	require.NoError(t, err)
	_, err = firstSession.Exec(fmt.Sprintf("SET CLUSTER SETTING %s = true", unsafeSettingName))
	require.NoError(t, err)

	// We can reuse the key twice.
	_, err = firstSession.Exec(fmt.Sprintf("SET CLUSTER SETTING %s = true", unsafeSettingName))
	require.NoError(t, err)

	otherSession := ts.SQLConn(t)

	// The first key produced in each session is different from other sessions.
	_, err = otherSession.Exec(fmt.Sprintf("SET CLUSTER SETTING %s = true", unsafeSettingName))
	require.Error(t, err)
	otherFirstKey := getKey(err)
	require.NotEqual(t, key, otherFirstKey)

	// We also can't use a key from one session with another.
	_, err = firstSession.Exec("SET unsafe_setting_interlock_key = $1", key)
	require.NoError(t, err)

	_, err = otherSession.Exec(fmt.Sprintf("SET CLUSTER SETTING %s = true", unsafeSettingName))
	require.Error(t, err)
	stillFirstKey := getKey(err)
	require.Equal(t, otherFirstKey, stillFirstKey)

	// A different cluster setting generates a different key.
	_, err = otherSession.Exec(fmt.Sprintf("SET CLUSTER SETTING %s = true", otherUnsafeSetting.Name()))
	require.Error(t, err)
	otherSettingKey := getKey(err)
	require.NotEqual(t, otherSettingKey, otherFirstKey)
}
