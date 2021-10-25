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
	"log"
	"os/user"
)

var (
	// Binary TODO(peter): document
	Binary = "cockroach"
	// SlackToken TODO(peter): document
	SlackToken string
	// OSUser TODO(peter): document
	OSUser *user.User
)

func init() {
	var err error
	OSUser, err = user.Current()
	if err != nil {
		log.Panic("Unable to determine OS user", err)
	}
}

const (
	// DefaultDebugDir is used to stash debug information.
	DefaultDebugDir = "${HOME}/.roachprod/debug"

	// EmailDomain is used to form the full account name for GCE and Slack.
	EmailDomain = "@cockroachlabs.com"

	// Local is the name of the local cluster.
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
)
