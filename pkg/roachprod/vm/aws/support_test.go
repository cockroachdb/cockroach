// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aws

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestWriteStartupScriptTemplate mainly tests the startup script tpl compiles.
func TestWriteStartupScriptTemplate(t *testing.T) {
	file, err := writeStartupScript("vm_name", "", vm.Zfs, false, false, "ubuntu")
	require.NoError(t, err)

	f, err := os.ReadFile(file)
	require.NoError(t, err)

	echotest.Require(t, string(f), datapathutils.TestDataPath(t, "startup_script"))
}

func TestMaybeAWSCapacityError(t *testing.T) {
	const stderr = `An error occurred (InsufficientInstanceCapacity) when calling the RunInstances operation: We currently do not have sufficient c6i.2xlarge capacity in the Availability Zone you requested (us-east-2a). Try us-east-2b or us-east-2c.`

	err := maybeAWSCapacityError(errors.New("run-instances failed"), stderr)
	capacityErr := requireCreateCapacityError(t, err)
	require.Equal(t, vm.CreateCapacityClassZone, capacityErr.CapacityClass)
	require.Equal(t, ProviderName, capacityErr.Provider)
	require.Equal(t, "c6i.2xlarge", capacityErr.MachineType)
	require.Equal(t, []string{"us-east-2a"}, capacityErr.FailedZones)
	require.Equal(t, []string{"us-east-2b", "us-east-2c"}, capacityErr.SuggestedZones)
}

func TestMaybeAWSCapacityErrorIgnoresCapacityWordsWithoutCode(t *testing.T) {
	cause := errors.New("run-instances failed")
	require.Equal(t, cause, maybeAWSCapacityError(cause, "insufficient capacity in availability zone"))
}

func TestAnnotateAWSCapacityErrorAddsMetadata(t *testing.T) {
	err := maybeAWSCapacityError(errors.New("run-instances failed"), "InsufficientInstanceCapacity")
	err = annotateAWSCapacityError(err, "us-east-2a", "c6i.2xlarge")

	capacityErr := requireCreateCapacityError(t, err)
	require.Equal(t, "c6i.2xlarge", capacityErr.MachineType)
	require.Equal(t, []string{"us-east-2a"}, capacityErr.FailedZones)
}

func TestNewAWSSpotTerminalCapacityErrorIgnoresNonCapacityStatusWithCapacityMessage(t *testing.T) {
	err := newAWSSpotTerminalCapacityError(
		"us-east-2a",
		"c6i.2xlarge",
		"sir-123",
		"i-123",
		"failed",
		"bad-parameters",
		"Launch specification is invalid; capacity cannot be checked.",
	)

	require.NoError(t, err)
}

func TestProcessSpotInstanceRequestStatusTerminalState(t *testing.T) {
	for _, tc := range []struct {
		code, message string
		capacity      bool
	}{
		{"capacity-not-available", "There is no Spot capacity available.", true},
		{"bad-parameters", "Launch specification is invalid.", false},
	} {
		fulfilled, err := processSpotInstanceRequestStatus(
			nil, spotInstanceRequestsOutputForTest("failed", tc.code, tc.message),
			"sir-123", "i-123", "us-east-2a", "c6i.2xlarge",
		)

		require.False(t, fulfilled)
		var capacityErr *vm.CreateCapacityError
		require.Equal(t, tc.capacity, errors.As(err, &capacityErr))
		if tc.capacity {
			require.Equal(t, "c6i.2xlarge", capacityErr.MachineType)
			require.Equal(t, []string{"us-east-2a"}, capacityErr.FailedZones)
		}
	}
}

func requireCreateCapacityError(t *testing.T, err error) *vm.CreateCapacityError {
	t.Helper()
	var capacityErr *vm.CreateCapacityError
	require.True(t, errors.As(err, &capacityErr))
	return capacityErr
}

func spotInstanceRequestsOutputForTest(
	state string, statusCode string, statusMessage string,
) DescribeSpotInstanceRequestsOutput {
	var describeOutput DescribeSpotInstanceRequestsOutput
	_ = json.Unmarshal([]byte(fmt.Sprintf(
		`{"SpotInstanceRequests":[{"SpotInstanceRequestId":"sir-123","InstanceId":"i-123","State":%q,"Status":{"Code":%q,"Message":%q}}]}`,
		state, statusCode, statusMessage,
	)), &describeOutput)
	return describeOutput
}
