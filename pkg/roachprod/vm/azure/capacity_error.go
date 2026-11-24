// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

var azureCapacityErrorCodes = []string{
	"AllocationFailed",
	"OverconstrainedAllocationRequest",
	"OverconstrainedZonalAllocationRequest",
	"SkuNotAvailable",
	"ZonalAllocationFailed",
}

func maybeAzureCapacityError(cause error, zone Zone, machineType string) error {
	if cause == nil {
		return nil
	}
	var capacityErr *vm.CreateCapacityError
	if errors.As(cause, &capacityErr) {
		return cause
	}
	if !isAzureCapacityError(cause.Error()) {
		return cause
	}
	return &vm.CreateCapacityError{
		CapacityClass: vm.CreateCapacityClassZone,
		Provider:      ProviderName,
		MachineType:   machineType,
		FailedZones:   []string{zone.String()},
		Cause:         cause,
	}
}

func isAzureCapacityError(details string) bool {
	for _, code := range azureCapacityErrorCodes {
		if strings.Contains(details, code) {
			return true
		}
	}

	lower := strings.ToLower(details)
	if strings.Contains(lower, "currently not available") &&
		(strings.Contains(lower, "vm size") || strings.Contains(lower, "sku")) {
		return true
	}
	return strings.Contains(lower, "capacity") &&
		(strings.Contains(lower, "allocation") || strings.Contains(lower, "zone") ||
			strings.Contains(lower, "vm size") || strings.Contains(lower, "sku"))
}
