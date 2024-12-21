// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package cli

import "github.com/spf13/cobra"

// register registers all roachprod subcommands.
// Add your commands here
func (cr *commandRegistry) register() {
	cr.addCommand([]*cobra.Command{
		cr.buildCreateCmd(),
		cr.buildGrowCmd(),
		cr.buildShrinkCmd(),
		cr.buildResetCmd(),
		cr.buildDestroyCmd(),
		cr.buildExtendCmd(),
		cr.buildLoadBalancerCmd(),
		cr.buildListCmd(),
		cr.buildSyncCmd(),
		cr.buildGCCmd(),
		cr.buildSetupSSHCmd(),
		cr.buildStatusCmd(),
		cr.buildMonitorCmd(),
		cr.buildStartCmd(),
		cr.buildUpdateTargetsCmd(),
		cr.buildStopCmd(),
		cr.buildStartInstanceCmd(),
		cr.buildStopInstanceCmd(),
		cr.buildDeployCmd(),
		cr.buildInitCmd(),
		cr.buildRunCmd(),
		cr.buildSignalCmd(),
		cr.buildWipeCmd(),
		cr.buildDestroyDNSCmd(),
		cr.buildReformatCmd(),
		cr.buildInstallCmd(),
		cr.buildDistributeCertsCmd(),
		cr.buildSshKeysCmd(),
		cr.buildPutCmd(),
		cr.buildGetCmd(),
		cr.buildStageCmd(),
		cr.buildStageURLCmd(),
		cr.buildDownloadCmd(),
		cr.buildSQLCmd(),
		cr.buildIPCmd(),
		cr.buildPGUrlCmd(),
		cr.buildAdminurlCmd(),
		cr.buildLogsCmd(),
		cr.buildPprofCmd(),
		cr.buildCachedHostsCmd(),
		cr.buildVersionCmd(),
		cr.buildGetProvidersCmd(),
		cr.buildGrafanaStartCmd(),
		cr.buildGrafanaStopCmd(),
		cr.buildGrafanaDumpCmd(),
		cr.buildGrafanaURLCmd(),
		cr.buildGrafanaAnnotationCmd(),
		cr.buildRootStorageCmd(),
		cr.buildSnapshotCmd(),
		cr.buildUpdateCmd(),
		cr.buildJaegerStartCmd(),
		cr.buildJaegerStopCmd(),
		cr.buildJaegerURLCmd(),
		cr.buildSideEyeRootCmd(),
		cr.buildFluentBitStartCmd(),
		cr.buildFluentBitStopCmd(),
		cr.buildOpentelemetryStartCmd(),
		cr.buildOpentelemetryStopCmd(),
		cr.buildFetchLogsCmd(),
		cr.buildGetLatestPProfCmd(),
	})
}
