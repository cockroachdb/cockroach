package skip_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestExpirationTooFarPanics(t *testing.T) {
	tooFar := timeutil.Now().Add(31 * 24 * time.Hour).Format("2006-01-02")
	require.Panics(t, func() {
		skip.WithIssueUntil(t, 0, skip.Expiration(tooFar))
	})
}

func TestExpirationBadFormatPanics(t *testing.T) {
	require.Panics(t, func() {
		skip.WithIssueUntil(t, 0, skip.Expiration("2006-1-23 5"))
	})
}

func TestSkipWithIssueUntilSkips(t *testing.T) {
	alwaysSkip := timeutil.Now().Add(29 * 24 * time.Hour).Format("2006-01-02")
	skip.WithIssueUntil(t, 0, skip.Expiration(alwaysSkip))
	require.True(t, false)
}

func TestSkipWithIssueUntilRespectsExpiration(t *testing.T) {
	expired := timeutil.Now().Add(-24 * time.Hour).Format("2006-01-02")
	shouldFail := true
	defer func() {
		if shouldFail {
			t.Fatal("shouldFail still true despite skip expiration")
		}
	}()
	skip.WithIssueUntil(t, 0, skip.Expiration(expired))
	shouldFail = false
}
