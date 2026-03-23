// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/errors"
)

// InstanceUnavailableError is returned when one or more SQL instances required
// for the distributed merge are unavailable. It is marked as a permanent job
// error because waitForRequiredInstances already retries with exponential
// backoff before surfacing this error, so restarting the entire job would be
// wasteful.
type InstanceUnavailableError struct {
	UnavailableInstances []base.SQLInstanceID
}

// NewInstanceUnavailableError creates an InstanceUnavailableError for the given
// instances, marked as a permanent job error.
func NewInstanceUnavailableError(unavailable []base.SQLInstanceID) error {
	return jobs.MarkAsPermanentJobError(
		&InstanceUnavailableError{UnavailableInstances: unavailable},
	)
}

// Error implements the error interface.
func (e *InstanceUnavailableError) Error() string {
	return fmt.Sprintf("distributed merge requires SST files from instances %v which are unavailable",
		e.UnavailableInstances)
}

// CheckRequiredInstancesAvailable checks that all SQL instances referenced in
// SST URIs are available. Returns nil if all required instances are available,
// or an InstanceUnavailableError (wrapped as a permanent job error) if any are
// unavailable.
func CheckRequiredInstancesAvailable(
	ssts []execinfrapb.BulkMergeSpec_SST, availableInstances []sqlinstance.InstanceInfo,
) error {
	if len(ssts) == 0 {
		return nil
	}

	// Extract required instance IDs from SST URIs.
	requiredInstances, err := extractRequiredInstances(ssts)
	if err != nil {
		return errors.Wrap(err, "parsing SST URIs for instance availability check")
	}

	if len(requiredInstances) == 0 {
		// No nodelocal URIs found; nothing to check.
		return nil
	}

	// Build a set of available instance IDs.
	availableSet := make(map[base.SQLInstanceID]struct{}, len(availableInstances))
	for _, inst := range availableInstances {
		availableSet[inst.InstanceID] = struct{}{}
	}

	// Find unavailable instances.
	var unavailable []base.SQLInstanceID
	for instanceID := range requiredInstances {
		if _, ok := availableSet[instanceID]; !ok {
			unavailable = append(unavailable, instanceID)
		}
	}

	if len(unavailable) == 0 {
		return nil
	}

	return NewInstanceUnavailableError(unavailable)
}

// extractRequiredInstances parses SST URIs and returns a set of unique
// SQLInstanceIDs that own nodelocal storage. Non-nodelocal URIs are ignored.
func extractRequiredInstances(
	ssts []execinfrapb.BulkMergeSpec_SST,
) (map[base.SQLInstanceID]struct{}, error) {
	required := make(map[base.SQLInstanceID]struct{})
	for _, sst := range ssts {
		if sst.URI == "" {
			continue
		}
		// Only check nodelocal URIs; skip cloud storage URIs.
		if !strings.HasPrefix(sst.URI, "nodelocal://") {
			continue
		}
		instanceID, err := nodelocal.ParseInstanceID(sst.URI)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing instance ID from URI %q", sst.URI)
		}
		// Instance ID 0 means "self" (local instance), which is always available.
		if instanceID != 0 {
			required[instanceID] = struct{}{}
		}
	}
	return required, nil
}
