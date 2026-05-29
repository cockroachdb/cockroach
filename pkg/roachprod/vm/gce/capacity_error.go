// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

var (
	gceZoneTokenRE          = regexp.MustCompile(`\b[a-z]+(?:-[a-z0-9]+)+-[a-z]\b`)
	gceMetadataZoneRE       = regexp.MustCompile(`(?m)^\s*zone:\s*([a-z]+(?:-[a-z0-9]+)+-[a-z])\s*$`)
	gceMetadataZonesAvailRE = regexp.MustCompile(`(?m)^\s*zonesAvailable:\s*(.+?)\s*$`)
	gceMetadataVMTypeRE     = regexp.MustCompile(`(?m)^\s*vmType:\s*([^\s]+)\s*$`)
	gceResourcePathZoneRE   = regexp.MustCompile(`/zones/([a-z]+(?:-[a-z0-9]+)+-[a-z])\b`)
)

func maybeGCECapacityError(cause error, details []byte) error {
	if cause == nil {
		return nil
	}
	var capacityErr *vm.CreateCapacityError
	if errors.As(cause, &capacityErr) {
		return cause
	}
	capacityErr = parseGCECapacityError(string(details))
	if capacityErr == nil {
		return cause
	}
	capacityErr.Cause = cause
	return capacityErr
}

func annotateGCECapacityError(err error, zone string) error {
	var capacityErr *vm.CreateCapacityError
	if !errors.As(err, &capacityErr) {
		return err
	}
	if zone != "" && len(capacityErr.FailedZones) == 0 {
		capacityErr.FailedZones = []string{zone}
	}
	return err
}

func parseGCECapacityError(details string) *vm.CreateCapacityError {
	// GCE formats these capacity failures as resource availability errors.
	// See: https://docs.cloud.google.com/compute/docs/troubleshooting/troubleshooting-resource-availability
	if !(strings.Contains(details, "ZONE_RESOURCE_POOL_EXHAUSTED") ||
		strings.Contains(details, "resource_availability") ||
		strings.Contains(details, "does not have enough resources available")) {
		return nil
	}

	capacityErr := &vm.CreateCapacityError{
		CapacityClass: vm.CreateCapacityClassZone,
		Provider:      ProviderName,
	}
	if match := gceMetadataVMTypeRE.FindStringSubmatch(details); len(match) == 2 {
		capacityErr.MachineType = strings.TrimSpace(match[1])
	}
	if match := gceMetadataZoneRE.FindStringSubmatch(details); len(match) == 2 {
		capacityErr.FailedZones = append(capacityErr.FailedZones, match[1])
	} else if match := gceResourcePathZoneRE.FindStringSubmatch(details); len(match) == 2 {
		capacityErr.FailedZones = append(capacityErr.FailedZones, match[1])
	}
	if match := gceMetadataZonesAvailRE.FindStringSubmatch(details); len(match) == 2 {
		capacityErr.SuggestedZones = uniqueZones(gceZoneTokenRE.FindAllString(match[1], -1))
	}

	capacityErr.FailedZones = uniqueZones(capacityErr.FailedZones)
	return capacityErr
}

func uniqueZones(zones []string) []string {
	if len(zones) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(zones))
	unique := make([]string, 0, len(zones))
	for _, zone := range zones {
		zone = strings.TrimSpace(zone)
		if zone == "" {
			continue
		}
		if _, ok := seen[zone]; ok {
			continue
		}
		seen[zone] = struct{}{}
		unique = append(unique, zone)
	}
	return unique
}
