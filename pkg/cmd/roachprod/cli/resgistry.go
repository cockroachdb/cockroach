// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package cli

import "github.com/spf13/cobra"

// register registers all roachprod subcommands.
// Add your commands here
func register() []*cobra.Command {
	return []*cobra.Command{
		createCmd,
		growCmd,
		shrinkCmd,
		resetCmd,
		destroyCmd,
		extendCmd,
		getLoadBalancerCmd(),
		listCmd,
		gcCmd,
		setupSSHCmd,
		statusCmd,
		monitorCmd,
		startCmd,
		updateTargetsCmd,
		stopCmd,
		startInstanceCmd,
		stopInstanceCmd,
		deployCmd,
		initCmd,
		runCmd,
		signalCmd,
		wipeCmd,
		destroyDNSCmd,
		reformatCmd,
		installCmd,
		distributeCertsCmd,
		sshKeysCmd,
		putCmd,
		getCmd,
		stageCmd,
		stageURLCmd,
		downloadCmd,
		sqlCmd,
		ipCmd,
		pgurlCmd,
		adminurlCmd,
		logsCmd,
		pprofCmd,
		cachedHostsCmd,
		versionCmd,
		getProvidersCmd,
		grafanaStartCmd,
		grafanaStopCmd,
		grafanaDumpCmd,
		grafanaURLCmd,
		grafanaAnnotationCmd,
		rootStorageCmd,
		snapshotCmd,
		updateCmd,
		jaegerStartCmd,
		jaegerStopCmd,
		jaegerURLCmd,
		sideEyeRootCmd,
		fluentBitStartCmd,
		fluentBitStopCmd,
		opentelemetryStartCmd,
		opentelemetryStopCmd,
		fetchLogsCmd,
		getLatestPProf,
	}
}
