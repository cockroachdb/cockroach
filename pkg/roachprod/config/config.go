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

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

var (
	// Binary TODO(peter): document
	Binary = "cockroach"
	// SlackToken TODO(peter): document
	SlackToken string
	// OSUser TODO(peter): document
	OSUser *user.User
	// Quiet is used to disable fancy progress output.
	Quiet = true
	// MaxConcurrency specifies the maximum number of operations
	// to execute on nodes concurrently, set to zero for infinite.
	MaxConcurrency = 32
	// CockroachDevLicense is used by both roachprod and tools that import it.
	CockroachDevLicense = envutil.EnvOrDefaultString("COCKROACH_DEV_LICENSE", "")

	// SSHPublicKeyPath is the path to the public key that is expected
	// to exist in order to set up new roachprod clusters.
	SSHPublicKeyPath = os.ExpandEnv("${HOME}/.ssh/id_rsa.pub")
)

func init() {
	var err error
	OSUser, err = user.Current()
	if err != nil {
		log.Fatalf(context.Background(), "Unable to determine OS user: %v", err)
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

	// DefaultNumFilesLimit is the default limit on the number of files that can
	// be opened by the process.
	DefaultNumFilesLimit = 65 << 13
)

// DefaultEnvVars returns default environment variables used in conjunction with CLI and MakeClusterSettings.
// These can be overriden by specifying different values (last one wins).
// See 'generateStartCmd' which sets 'ENV_VARS' for the systemd startup script (start.sh).
func DefaultEnvVars() []string {
	return []string{
		// Get rid of an annoying popup in the UI.
		"COCKROACH_UI_RELEASE_NOTES_SIGNUP_DISMISSED=true",
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

// SSHPublicKey returns the contents of the default public key
// expected by roachprod.
func SSHPublicKey() (string, error) {
	sshKey, err := os.ReadFile(SSHPublicKeyPath)
	if err != nil {
		if oserror.IsNotExist(err) {
			return "", errors.Wrapf(err, "please run ssh-keygen externally to create your %s file", SSHPublicKeyPath)
		}
		return "", errors.Wrap(err, "failed to read public SSH key")
	}

	return string(sshKey), nil
}
