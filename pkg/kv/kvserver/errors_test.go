// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/benignerror"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestErrorFormatting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var e error = decommissionPurgatoryError{errors.New("hello")}
	require.Equal(t, "hello", redact.Sprint(e).Redact().StripMarkers())
	e = rangeMergePurgatoryError{errors.New("hello")}
	require.Equal(t, "hello", redact.Sprint(e).Redact().StripMarkers())
}

func TestError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	err := errors.Newf("actual error")
	err = errors.WithSecondaryError(newDescChangedError(&roachpb.RangeDescriptor{}, &roachpb.RangeDescriptor{}), err)
	fmt.Println(err)
	err = errors.Wrapf(err, "change replicas of r%d failed", 9)
	err = benignerror.New(err)
	//err = decommissionPurgatoryError{error: err}
	require.True(t, errors.Is(err, errMarkCanRetryReplicationChangeWithUpdatedDesc))
}
