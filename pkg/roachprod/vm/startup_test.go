// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenericStartupArgs(t *testing.T) {
	t.Run("WithVMName", func(t *testing.T) {
		args := DefaultStartupArgs(WithVMName("test"))
		require.Equal(t, args.VMName, "test")
	})
	t.Run("WithSharedUser", func(t *testing.T) {
		args := DefaultStartupArgs(WithSharedUser("test"))
		require.Equal(t, args.SharedUser, "test")
	})
	t.Run("WithStartupLogs", func(t *testing.T) {
		args := DefaultStartupArgs(WithStartupLogs("test"))
		require.Equal(t, args.StartupLogs, "test")
	})
	t.Run("WithOSInitializedFile", func(t *testing.T) {
		args := DefaultStartupArgs(WithOSInitializedFile("test"))
		require.Equal(t, args.OSInitializedFile, "test")
	})
	t.Run("WithDiskInitializeFile", func(t *testing.T) {
		args := DefaultStartupArgs(WithDisksInitializedFile("test"))
		require.Equal(t, args.DisksInitializedFile, "test")
	})
	t.Run("WithZfs", func(t *testing.T) {
		args := DefaultStartupArgs(WithZfs(true))
		require.Equal(t, args.Zfs, true)
	})
	t.Run("WithFIPS", func(t *testing.T) {
		args := DefaultStartupArgs(WithEnableFIPS(true))
		require.Equal(t, args.EnableFIPS, true)
	})
	t.Run("WithEnableCron", func(t *testing.T) {
		args := DefaultStartupArgs(WithEnableCron(true))
		require.Equal(t, args.EnableCron, true)
	})
	t.Run("WithChronyServers", func(t *testing.T) {
		args := DefaultStartupArgs(WithChronyServers([]string{"test"}))
		require.Equal(t, args.ChronyServers, []string{"test"})
	})
	t.Run("WithNodeExporterPort", func(t *testing.T) {
		args := DefaultStartupArgs(WithNodeExporterPort(1234))
		if args.NodeExporterPort != 1234 {
			t.Fatalf("expected NodeExporterPort to be 1234, got %d", args.NodeExporterPort)
		}
	})
}
