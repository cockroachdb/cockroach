// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aws

import (
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

var (
	awsZoneTokenRE    = regexp.MustCompile(`\b[a-z]{2}-[a-z]+-\d[a-z]\b`)
	awsInstanceTypeRE = regexp.MustCompile(`\b[a-z][0-9][a-z0-9]*\.[a-z0-9]+(?:\.[a-z0-9]+)?\b`)
)

func maybeAWSCapacityError(cause error, details string) error {
	if cause == nil {
		return nil
	}
	var capacityErr *vm.CreateCapacityError
	if errors.As(cause, &capacityErr) {
		return cause
	}
	if !isAWSCapacityError(details) {
		return cause
	}
	failedZones := awsZonesInRequestedAvailabilityZone(details)
	return newAWSCapacityError(cause, firstAWSInstanceType(details), failedZones, awsSuggestedZones(details, failedZones))
}

func annotateAWSCapacityError(err error, zone string, machineType string) error {
	var capacityErr *vm.CreateCapacityError
	if !errors.As(err, &capacityErr) {
		return err
	}
	if capacityErr.MachineType == "" {
		capacityErr.MachineType = machineType
	}
	if zone != "" && len(capacityErr.FailedZones) == 0 {
		capacityErr.FailedZones = []string{zone}
	}
	return err
}

func newAWSSpotCapacityTimeoutError(
	zone string, machineType string, spotRequestID string, timeout time.Duration,
) error {
	msg := "spot instance request was not fulfilled"
	if spotRequestID != "" {
		msg = "spot instance request " + spotRequestID + " was not fulfilled"
	}
	return newAWSCapacityError(errors.Newf("%s within %s", msg, timeout), machineType, zoneSlice(zone), nil)
}

func newAWSSpotTerminalCapacityError(
	zone string,
	machineType string,
	spotRequestID string,
	instanceID string,
	state string,
	statusCode string,
	statusMessage string,
) error {
	if !isAWSSpotCapacityStatus(statusCode, statusMessage) {
		return nil
	}
	msg := "spot instance request"
	if spotRequestID != "" {
		msg += " " + spotRequestID
	}
	if instanceID != "" {
		msg += " for instance " + instanceID
	}
	msg += " is not active"
	if state != "" {
		msg += " with state " + state
	}
	if statusCode != "" {
		msg += " and status " + statusCode
	}
	if statusMessage != "" {
		msg += ": " + statusMessage
	}
	return newAWSCapacityError(errors.Newf("%s", msg), machineType, zoneSlice(zone), nil)
}

func newAWSCapacityError(
	cause error, machineType string, failedZones []string, suggestedZones []string,
) *vm.CreateCapacityError {
	return &vm.CreateCapacityError{
		CapacityClass:  vm.CreateCapacityClassZone,
		Provider:       ProviderName,
		MachineType:    machineType,
		FailedZones:    failedZones,
		SuggestedZones: suggestedZones,
		Cause:          cause,
	}
}

func isAWSSpotCapacityStatus(statusCode string, _ string) bool {
	// Spot capacity status codes are documented by Amazon EC2.
	// See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-request-status.html#spot-instance-request-status-codes
	code := strings.ToLower(statusCode)
	return code == "capacity-not-available" || code == "instance-terminated-no-capacity"
}

func isAWSCapacityError(details string) bool {
	// RunInstances capacity failures are documented by Amazon EC2.
	// See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/troubleshooting-launch.html#troubleshooting-launch-capacity
	return strings.Contains(details, "InsufficientInstanceCapacity")
}

func firstAWSInstanceType(details string) string {
	return awsInstanceTypeRE.FindString(details)
}

func zoneSlice(zone string) []string {
	if zone == "" {
		return nil
	}
	return []string{zone}
}

func awsZonesInRequestedAvailabilityZone(details string) []string {
	lower := strings.ToLower(details)
	idx := strings.Index(lower, "availability zone")
	if idx == -1 {
		return nil
	}
	next := details[idx:]
	if period := strings.Index(next, "."); period != -1 {
		next = next[:period]
	}
	return uniqueAWSZones(awsZoneTokenRE.FindAllString(next, -1))
}

func awsSuggestedZones(details string, failedZones []string) []string {
	failed := make(map[string]struct{}, len(failedZones))
	for _, zone := range failedZones {
		failed[zone] = struct{}{}
	}
	return uniqueAWSZonesExcluding(awsZoneTokenRE.FindAllString(details, -1), failed)
}

func uniqueAWSZones(zones []string) []string {
	return uniqueAWSZonesExcluding(zones, nil)
}

func uniqueAWSZonesExcluding(zones []string, exclude map[string]struct{}) []string {
	if len(zones) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(zones))
	unique := make([]string, 0, len(zones))
	for _, zone := range zones {
		if _, ok := exclude[zone]; ok {
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
