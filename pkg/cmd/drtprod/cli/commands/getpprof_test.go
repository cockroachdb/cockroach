// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package commands

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_downloadFile(t *testing.T) {
	ctx := context.Background()
	mockClusterName := "mock-cluster"
	pprofCommandExecutor = nil
	t.Run("expect failure due to invalid beforeTime", func(t *testing.T) {
		err := downloadFile(ctx, mockClusterName, "invalid")
		require.NotNil(t, err)
		require.Equal(t, "parsing time \"invalid\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"invalid\" as \"2006\"", err.Error())
	})
	t.Run("expect no download with empty response from ls -ltr", func(t *testing.T) {
		invokeCount := 0
		pprofCommandExecutor = func(ctx context.Context, cmd string, args ...string) (string, string, error) {
			invokeCount++
			require.Equal(t, "ssh mock-cluster -- ls -ltr logs/pprof_dump/cpuprof.*.pprof", strings.Join(args, " "))
			return "", "", nil
		}
		require.Nil(t, downloadFile(ctx, mockClusterName, ""))
		require.Equal(t, 1, invokeCount)
	})
	t.Run("expect ls command to fail", func(t *testing.T) {
		invokeCount := 0
		pprofCommandExecutor = func(ctx context.Context, cmd string, args ...string) (string, string, error) {
			invokeCount++
			require.Equal(t, "ssh mock-cluster -- ls -ltr logs/pprof_dump/cpuprof.*.pprof", strings.Join(args, " "))
			return "", "", fmt.Errorf("failed to connect")
		}
		err := downloadFile(ctx, mockClusterName, "")
		require.NotNil(t, err)
		require.NotNil(t, "failed to connect", err.Error())
		require.Equal(t, 1, invokeCount)
	})
	t.Run("expect response for single cluster", func(t *testing.T) {
		invokeCount := 0
		pprofCommandExecutor = func(ctx context.Context, cmd string, args ...string) (string, string, error) {
			invokeCount++
			require.Equal(t, "roachprod", cmd)
			if strings.Join(args, " ") == "ssh mock-cluster:2 -- ls -ltr logs/pprof_dump/cpuprof.*.pprof" {
				return getSingleClusterMockResponse(), "", nil
			}
			if strings.Join(args, " ") == "get mock-cluster:2 logs/pprof_dump/cpuprof.2024-09-27T10_07_33.427.85.pprof mock-cluster_2_cpuprof.2024-09-27T10_07_33.427.85.pprof" {
				return "", "", nil
			}
			require.True(t, false, "unexpected command executor invocation %s %s", cmd, args)
			return "", "", nil
		}
		err := downloadFile(ctx, fmt.Sprintf("%s:%d", mockClusterName, 2), "")
		require.Nil(t, err)
		require.Equal(t, 2, invokeCount)
	})
	t.Run("expect download command to fail", func(t *testing.T) {
		invokeCount := 0
		pprofCommandExecutor = func(ctx context.Context, cmd string, args ...string) (string, string, error) {
			invokeCount++
			require.Equal(t, "roachprod", cmd)
			if strings.Join(args, " ") == "ssh mock-cluster:2 -- ls -ltr logs/pprof_dump/cpuprof.*.pprof" {
				return getSingleClusterMockResponse(), "", nil
			}
			if strings.Join(args, " ") == "get mock-cluster:2 logs/pprof_dump/cpuprof.2024-09-27T10_07_33.427.85.pprof mock-cluster_2_cpuprof.2024-09-27T10_07_33.427.85.pprof" {
				return "", "", fmt.Errorf("failed to download")
			}
			require.True(t, false, "unexpected command executor invocation %s %s", cmd, args)
			return "", "", nil
		}
		err := downloadFile(ctx, fmt.Sprintf("%s:%d", mockClusterName, 2), "")
		require.NotNil(t, err)
		require.Equal(t, "failed to download", err.Error())
		require.Equal(t, 2, invokeCount)
	})
	t.Run("expect response for multi cluster", func(t *testing.T) {
		invokeCount := 0
		pprofCommandExecutor = func(ctx context.Context, cmd string, args ...string) (string, string, error) {
			invokeCount++
			require.Equal(t, "roachprod", cmd)
			if strings.Join(args, " ") == "ssh mock-cluster -- ls -ltr logs/pprof_dump/cpuprof.*.pprof" {
				return getMultiClusterMockResponse(), "", nil
			}
			if strings.Join(args, " ") == "get mock-cluster:3 logs/pprof_dump/cpuprof.2024-09-27T10_07_36.382.84.pprof mock-cluster_3_cpuprof.2024-09-27T10_07_36.382.84.pprof" {
				return "", "", nil
			}
			require.True(t, false, "unexpected command executor invocation %s %s", cmd, args)
			return "", "", nil
		}
		err := downloadFile(ctx, mockClusterName, "")
		require.Nil(t, err)
		require.Equal(t, 2, invokeCount)
	})
	t.Run("expect response for multi cluster with exact beforeTime set", func(t *testing.T) {
		// Here the beforeTime is exactly equal to the time in the file
		invokeCount := 0
		pprofCommandExecutor = func(ctx context.Context, cmd string, args ...string) (string, string, error) {
			invokeCount++
			require.Equal(t, "roachprod", cmd)
			if strings.Join(args, " ") == "ssh mock-cluster -- ls -ltr logs/pprof_dump/cpuprof.*.pprof" {
				return getMultiClusterMockResponse(), "", nil
			}
			if strings.Join(args, " ") == "get mock-cluster:2 logs/pprof_dump/cpuprof.2024-09-27T10_05_29.355.79.pprof mock-cluster_2_cpuprof.2024-09-27T10_05_29.355.79.pprof" {
				return "", "", nil
			}
			require.True(t, false, "unexpected command executor invocation %s %s", cmd, args)
			return "", "", nil
		}
		err := downloadFile(ctx, mockClusterName, "2024-09-27T10:05:29Z")
		require.Nil(t, err)
		require.Equal(t, 2, invokeCount)
	})
	t.Run("expect response for multi cluster with greater beforeTime set", func(t *testing.T) {
		// Here the beforeTime is greater than the time in the file
		invokeCount := 0
		pprofCommandExecutor = func(ctx context.Context, cmd string, args ...string) (string, string, error) {
			invokeCount++
			require.Equal(t, "roachprod", cmd)
			if strings.Join(args, " ") == "ssh mock-cluster -- ls -ltr logs/pprof_dump/cpuprof.*.pprof" {
				return getMultiClusterMockResponse(), "", nil
			}
			if strings.Join(args, " ") == "get mock-cluster:1 logs/pprof_dump/cpuprof.2024-09-27T10_05_22.234.69.pprof mock-cluster_1_cpuprof.2024-09-27T10_05_22.234.69.pprof" {
				return "", "", nil
			}
			require.True(t, false, "unexpected command executor invocation %s %s", cmd, args)
			return "", "", nil
		}
		err := downloadFile(ctx, mockClusterName, "2024-09-27T10:05:23Z")
		require.Nil(t, err)
		require.Equal(t, 2, invokeCount)
	})
	t.Run("expect no get invocation for lesser beforeTime set", func(t *testing.T) {
		// Here the beforeTime is lesser than the time in all the files
		invokeCount := 0
		pprofCommandExecutor = func(ctx context.Context, cmd string, args ...string) (string, string, error) {
			invokeCount++
			require.Equal(t, "roachprod", cmd)
			if strings.Join(args, " ") == "ssh mock-cluster -- ls -ltr logs/pprof_dump/cpuprof.*.pprof" {
				return getMultiClusterMockResponse(), "", nil
			}
			require.True(t, false, "unexpected command executor invocation %s %s", cmd, args)
			return "", "", nil
		}
		err := downloadFile(ctx, mockClusterName, "2006-01-02T15:04:05Z")
		require.Nil(t, err)
		require.Equal(t, 1, invokeCount)
	})
}

func getSingleClusterMockResponse() string {
	return `-rw-r----- 1 ubuntu ubuntu 219534 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_22.234.69.pprof
-rw-r----- 1 ubuntu ubuntu 211461 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_32.456.77.pprof
-rw-r----- 1 ubuntu ubuntu 207410 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_42.752.79.pprof
-rw-r----- 1 ubuntu ubuntu 248365 Sep 27 10:06 logs/pprof_dump/cpuprof.2024-09-27T10_06_33.042.82.pprof
-rw-r----- 1 ubuntu ubuntu 248365 Sep 27 10:06 logs/pprof_dump/cpuprof.2024-09-Invalid_06_33.042.82.pprof
-rw-r----- 1 ubuntu ubuntu 231194 Sep 27 10:07 logs/pprof_dump/cpuprof.2024-09-27T10_07_33.427.85.pprof`
}

func getMultiClusterMockResponse() string {
	return `drt-chaos:[1 2 3]: ls -ltr logs/pprof_dump/cpu.......
   1: 	<ok>
	-rw-r----- 1 ubuntu ubuntu 219534 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_22.234.69.pprof
	-rw-r----- 1 ubuntu ubuntu 211461 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_32.456.77.pprof
	-rw-r----- 1 ubuntu ubuntu 207410 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_42.752.79.pprof
	-rw-r----- 1 ubuntu ubuntu 248365 Sep 27 10:06 logs/pprof_dump/cpuprof.2024-09-27T10_06_33.042.82.pprof
	-rw-r----- 1 ubuntu ubuntu 231194 Sep 27 10:07 logs/pprof_dump/cpuprof.2024-09-27T10_07_33.427.85.pprof

   2: 	<ok>
	-rw-r----- 1 ubuntu ubuntu 264445 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_18.913.67.pprof
	-rw-r----- 1 ubuntu ubuntu 248579 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_29.355.79.pprof
	-rw-r----- 1 ubuntu ubuntu 255149 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_39.811.84.pprof
	-rw-r----- 1 ubuntu ubuntu 237958 Sep 27 10:07 logs/pprof_dump/cpuprof.2024-09-27T10_06_50.128.87.pprof

   3: 	<ok>
	-rw-r----- 1 ubuntu ubuntu 236330 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_35.796.82.pprof
	-rw-r----- 1 ubuntu ubuntu 236114 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_46.036.83.pprof
	-rw-r----- 1 ubuntu ubuntu 233042 Sep 27 10:07 logs/pprof_dump/cpuprof.2024-09-27T10_07_36.382.84.pprof`
}
