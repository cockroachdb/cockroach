// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

// InfraProvider is the API for GCP resources that are used as shared
// infrastructure by clusters on other other clouds.
type InfraProvider interface {
	// GetUserAuthorizedKeys retrieves reads a list of user public keys from the
	// gcloud cockroach-ephemeral project and returns them formatted for use in
	// an authorized_keys file.
	GetUserAuthorizedKeys() (AuthorizedKeys, error)
	// SyncDNS replaces the configured DNS zone with the supplied hosts.
	SyncDNS(l *logger.Logger, vms vm.List) error
	// DNSDomain returns the configured DNS domain for public DNS A records.
	DNSDomain() string
}

// Infrastructure is the process level InfraProvider.
var Infrastructure InfraProvider
