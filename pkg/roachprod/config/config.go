// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package config

import (
	"context"
	"os"
	"os/user"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var (
	// Binary TODO(peter): document
	Binary = "cockroach"
	// SlackToken TODO(peter): document
	SlackToken string
	// OSUser TODO(peter): document
	OSUser *user.User
	// Quiet is used to disable fancy progress output.
	Quiet = false
	// The default roachprod logger.
	// N.B. When roachprod is used via CLI, this logger is used for all output.
	//	When roachprod is used via API (e.g. from roachtest), this logger is used only in the few cases,
	//	during bootstrapping, at which time the caller has not yet had a chance to configure a custom logger.
	Logger *logger.Logger
	// MaxConcurrency specifies the maximum number of operations
	// to execute on nodes concurrently, set to zero for infinite.
	MaxConcurrency = 32
	// CockroachDevLicense is used by both roachprod and tools that import it.
	CockroachDevLicense = envutil.EnvOrDefaultString("COCKROACH_DEV_LICENSE", "")
)

func init() {
	var err error
	OSUser, err = user.Current()
	if err != nil {
		log.Fatalf(context.Background(), "Unable to determine OS user: %v", err)
	}

	loggerCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr}
	var loggerError error
	Logger, loggerError = loggerCfg.NewLogger("")
	if loggerError != nil {
		log.Fatalf(context.Background(), "unable to configure logger: %v", loggerError)
	}
}

const (
	// DefaultDebugDir is used to stash debug information.
	DefaultDebugDir = "${HOME}/.roachprod/debug"

	// EmailDomain is used to form the full account name for GCE and Slack.
	EmailDomain = "@cockroachlabs.com"

	// Local is the prefix used to identify local clusters.
	// It is also used as the zone for local clusters.
	Local = "local"

	// ClustersDir is the directory where we cache information about clusters.
	ClustersDir = "${HOME}/.roachprod/clusters"

	// DefaultLockPath is the path to the lock file used to synchronize access to
	// shared roachprod resources.
	DefaultLockPath = "$HOME/.roachprod/LOCK"

	// DNSDir is the directory where we cache local cluster DNS information.
	DNSDir = "${HOME}/.roachprod/dns"

	// SharedUser is the linux username for shared use on all vms.
	SharedUser = "ubuntu"

	// MemoryMax is passed to systemd-run; the cockroach process is killed if it
	// uses more than this percentage of the host's memory.
	MemoryMax = "95%"

	// DefaultSQLPort is the default port on which the cockroach process is
	// listening for SQL connections.
	DefaultSQLPort = 26257

	// DefaultAdminUIPort is the default port on which the cockroach process is
	// listening for HTTP connections for the Admin UI.
	DefaultAdminUIPort = 26258

	// DefaultOpenPortStart is the default starting range used to find open ports.
	DefaultOpenPortStart = 29000

	// DefaultNumFilesLimit is the default limit on the number of files that can
	// be opened by the process.
	DefaultNumFilesLimit = 65 << 13
)

// DefaultEnvVars returns default environment variables used in conjunction with CLI and MakeClusterSettings.
// These can be overriden by specifying different values (last one wins).
// See 'generateStartCmd' which sets 'ENV_VARS' for the systemd startup script (start.sh).
func DefaultEnvVars() []string {
	return []string{
		// We set the following environment variable to pretend that the
		// current development build is the next binary release (which
		// disables version offsetting). In upgrade tests, we're interested
		// in testing the upgrade logic that users would actually run when
		// they upgrade from one release to another.
		"COCKROACH_TESTING_FORCE_RELEASE_BRANCH=true",
	}
}

// IsLocalClusterName returns true if the given name is a valid name for a local
// cluster.
//
// Local cluster names are either "local" or start with a "local-" prefix.
func IsLocalClusterName(clusterName string) bool {
	return localClusterRegex.MatchString(clusterName)
}

var localClusterRegex = regexp.MustCompile(`^local(|-[a-zA-Z0-9\-]+)$`)
