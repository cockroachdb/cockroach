// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"cloud.google.com/go/logging/logadmin"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/encoding/protojson"
)

// ResetWithContext performs a hard reset (reboot) of VMs using the GCP SDK.
// This matches the gcloud implementation which uses `gcloud compute instances reset`.
func (p *Provider) ResetWithContext(ctx context.Context, l *logger.Logger, vms vm.List) error {
	// Organize VMs by project and zone for batch operations
	projectZoneMap, err := buildProjectZoneMap(vms)
	if err != nil {
		return err
	}

	// Set timeout for reset operations
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	g := newLimitedErrorGroupWithContext(ctx)

	for project, zoneMap := range projectZoneMap {
		for zone, names := range zoneMap {
			// Reset each instance in parallel
			for _, name := range names {
				g.GoCtx(func(ctx context.Context) error {
					req := &computepb.ResetInstanceRequest{
						Project:  project,
						Zone:     zone,
						Instance: name,
					}

					op, err := p.computeInstancesClient.Reset(ctx, req)
					if err != nil {
						return errors.Wrapf(err, "failed to reset instance %s in zone %s", name, zone)
					}

					// Wait for the reset operation to complete
					if err := op.Wait(ctx); err != nil {
						return errors.Wrapf(err, "failed to wait for instance reset %s in zone %s", name, zone)
					}

					// Check for operation errors
					if opErr := op.Proto().GetError(); opErr != nil {
						return errors.Newf("instance reset failed for %s in zone %s: %s", name, zone, opErr.String())
					}

					return nil
				})
			}
		}
	}

	return g.Wait()
}

// GetVMSpecsWithContext returns a map from VM.Name to a map of VM attributes using the GCP SDK.
// This matches the gcloud implementation which uses `gcloud compute instances describe`.
func (p *Provider) GetVMSpecsWithContext(
	ctx context.Context, l *logger.Logger, vms vm.List,
) (map[string]map[string]interface{}, error) {
	if p.GetProject() == "" {
		return nil, errors.New("project name cannot be empty")
	}
	if vms == nil {
		return nil, errors.New("vms cannot be nil")
	}

	// Extract the spec of all VMs in parallel and create a map from VM name to spec
	type vmSpecResult struct {
		name string
		spec map[string]interface{}
	}

	results := make(chan vmSpecResult, len(vms))
	g := newLimitedErrorGroupWithContext(ctx)

	for _, vmInstance := range vms {
		g.GoCtx(func(ctx context.Context) error {
			req := &computepb.GetInstanceRequest{
				Project:  p.GetProject(),
				Zone:     vmInstance.Zone,
				Instance: vmInstance.Name,
			}

			instance, err := p.computeInstancesClient.Get(ctx, req)
			if err != nil {
				return errors.Wrapf(err, "error describing instance %s in zone %s", vmInstance.Name, vmInstance.Zone)
			}

			// Convert the protobuf instance to a JSON map to match gcloud's output format
			// This ensures compatibility with code that expects the same structure
			jsonBytes, err := protojson.Marshal(instance)
			if err != nil {
				return errors.Wrapf(err, "failed to marshal instance %s to JSON", vmInstance.Name)
			}

			var vmSpec map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &vmSpec); err != nil {
				return errors.Wrapf(err, "failed to unmarshal instance %s JSON to map", vmInstance.Name)
			}

			// Verify the name field exists
			name, ok := vmSpec["name"].(string)
			if !ok {
				l.Errorf("failed to create spec files for VM\n%v", vmSpec)
				return nil // Continue with other VMs
			}

			results <- vmSpecResult{name: name, spec: vmSpec}
			return nil
		})
	}

	// Wait for all goroutines to complete
	if err := g.Wait(); err != nil {
		close(results)
		return nil, err
	}
	close(results)

	// Collect results into the final map
	vmSpecs := make(map[string]map[string]interface{})
	for result := range results {
		vmSpecs[result.name] = result.spec
	}

	return vmSpecs, nil
}

// buildVMResourceName creates the full GCP resource name for a VM instance.
// Format: projects/{project}/zones/{zone}/instances/{name}
// Extracted for testability.
func buildVMResourceName(project, zone, instanceName string) string {
	return fmt.Sprintf("projects/%s/zones/%s/instances/%s", project, zone, instanceName)
}

// buildPreemptionLogFilter creates a GCP logging filter for preemption events.
// Extracted for testability.
func buildPreemptionLogFilter(project string, vms vm.List) string {
	vmIDFilters := make([]string, len(vms))
	for i, vmNode := range vms {
		resourceName := buildVMResourceName(project, vmNode.Zone, vmNode.Name)
		vmIDFilters[i] = fmt.Sprintf("protoPayload.resourceName=%q", resourceName)
	}

	return fmt.Sprintf(
		`resource.type="gce_instance" AND protoPayload.methodName="compute.instances.preempted" AND (%s)`,
		strings.Join(vmIDFilters, " OR "))
}

// buildHostErrorLogFilter creates a GCP logging filter for host error events.
// Extracted for testability.
func buildHostErrorLogFilter(project string, vms vm.List) string {
	vmIDFilters := make([]string, len(vms))
	for i, vmNode := range vms {
		resourceName := buildVMResourceName(project, vmNode.Zone, vmNode.Name)
		vmIDFilters[i] = fmt.Sprintf("protoPayload.resourceName=%q", resourceName)
	}

	return fmt.Sprintf(
		`resource.type="gce_instance" AND protoPayload.methodName="compute.instances.hostError" AND logName="projects/%s/logs/cloudaudit.googleapis.com%%2Fsystem_event" AND (%s)`,
		project, strings.Join(vmIDFilters, " OR "))
}

// GetPreemptedSpotVMsWithContext checks the preemption status of the given VMs by querying
// the GCP Logging service using the SDK. This matches the gcloud implementation which
// uses `gcloud logging read` to filter preemption events.
func (p *Provider) GetPreemptedSpotVMsWithContext(
	ctx context.Context, l *logger.Logger, vms vm.List, since time.Time,
) ([]vm.PreemptedVM, error) {
	// Validate inputs (matching gcloud implementation)
	projectName := p.GetProject()
	if projectName == "" {
		return nil, errors.New("project name cannot be empty")
	}
	if since.After(timeutil.Now()) {
		return nil, errors.New("since cannot be in the future")
	}
	if vms == nil {
		return nil, errors.New("vms cannot be nil")
	}

	// Build filter for preemption events
	filter := buildPreemptionLogFilter(projectName, vms)

	// Calculate freshness - we look at logs from "since" time, adding 1 hour buffer
	freshness := timeutil.Since(since) + time.Hour

	// Query logs with filter and time range using the provider's logadmin client
	it := p.loggingAdminClient.Entries(ctx,
		logadmin.Filter(filter),
		logadmin.NewestFirst(),
	)

	var preemptedVMs []vm.PreemptedVM
	cutoffTime := since

	// Iterate through log entries
	for {
		entry, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to iterate log entries")
		}

		// Check if log entry is within our time range
		if entry.Timestamp.Before(cutoffTime) {
			// Since we're using NewestFirst(), once we hit entries before cutoff, we're done
			break
		}

		// Skip entries older than our freshness window
		if timeutil.Since(entry.Timestamp) > freshness {
			continue
		}

		// Extract resource name from the proto payload
		// The protoPayload is a map[string]interface{} in the SDK
		resourceName := ""
		if payload, ok := entry.Payload.(map[string]interface{}); ok {
			if rn, ok := payload["resourceName"].(string); ok {
				resourceName = rn
			}
		}

		if resourceName == "" {
			l.Printf("Warning: log entry missing resourceName in protoPayload")
			continue
		}

		preemptedVMs = append(preemptedVMs, vm.PreemptedVM{
			Name:        resourceName,
			PreemptedAt: entry.Timestamp,
		})
	}

	return preemptedVMs, nil
}

// GetHostErrorVMsWithContext checks the host error status of the given VMs by querying
// the GCP Logging service using the SDK. This matches the gcloud implementation which
// uses `gcloud logging read` to filter host error events.
func (p *Provider) GetHostErrorVMsWithContext(
	ctx context.Context, l *logger.Logger, vms vm.List, since time.Time,
) ([]string, error) {
	// Validate inputs (matching gcloud implementation)
	projectName := p.GetProject()
	if projectName == "" {
		return nil, errors.New("project name cannot be empty")
	}
	if since.After(timeutil.Now()) {
		return nil, errors.New("since cannot be in the future")
	}
	if vms == nil {
		return nil, errors.New("vms cannot be nil")
	}

	// Build filter for host error events
	filter := buildHostErrorLogFilter(projectName, vms)

	// Calculate freshness
	freshness := timeutil.Since(since) + time.Hour

	// Query logs with filter and time range using the provider's logadmin client
	it := p.loggingAdminClient.Entries(ctx,
		logadmin.Filter(filter),
		logadmin.NewestFirst(),
	)

	var hostErrorVMs []string
	cutoffTime := since

	// Iterate through log entries
	for {
		entry, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to iterate log entries")
		}

		// Check if log entry is within our time range
		if entry.Timestamp.Before(cutoffTime) {
			break
		}

		// Skip entries older than our freshness window
		if timeutil.Since(entry.Timestamp) > freshness {
			continue
		}

		// Extract resource name from the proto payload
		resourceName := ""
		if payload, ok := entry.Payload.(map[string]interface{}); ok {
			if rn, ok := payload["resourceName"].(string); ok {
				resourceName = rn
			}
		}

		if resourceName == "" {
			l.Printf("Warning: log entry missing resourceName in protoPayload")
			continue
		}

		hostErrorVMs = append(hostErrorVMs, resourceName)
	}

	return hostErrorVMs, nil
}
