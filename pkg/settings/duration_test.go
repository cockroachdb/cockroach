// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settings

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNonNegativeDurationWithMinimum(t *testing.T) {
	validatorOpt := NonNegativeDurationWithMinimum(time.Minute)
	validator := validatorOpt.validateDurationFn
	require.NoError(t, validator(time.Minute))
	require.NoError(t, validator(2*time.Minute))
	require.Error(t, validator(59*time.Second))
	require.Error(t, validator(-1*time.Second))
}
