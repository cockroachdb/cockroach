// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

// ClusterRegistrar is the dependency interface for the cluster-register
// hook executor. The clusters.Service satisfies this implicitly via Go
// structural typing — no import of the clusters package is needed.
type ClusterRegistrar interface {
	RegisterClusterInternal(
		ctx context.Context, l *logger.Logger, cluster cloudcluster.Cluster,
	) error
	UnregisterClusterInternal(
		ctx context.Context, l *logger.Logger,
		clusterName string,
		provisioningOwner string,
		provisioningIdentifier string,
	) error
}

// clusterRegisterConfig holds the parsed config for a cluster-register hook.
// Parsed from HookDeclaration.Config at execution time.
//
// Example template.yaml:
//
//	hooks:
//	  - name: register-cluster
//	    type: cluster-register
//	    triggers: [post_apply, pre_destroy]
//	    config:
//	      provider: "gce"
//	      remote_user: "ubuntu"
//	      project_expr: ".data.project"
//	      vms_expr: ".data.vms[]"
type clusterRegisterConfig struct {
	Provider    string // cloud provider name (e.g. "gce", "aws")
	RemoteUser  string // SSH user for VMs (e.g. "ubuntu")
	ProjectExpr string // jq expression to extract project from outputs
	VMsExpr     string // jq expression to extract VM list from outputs
}

// parseClusterRegisterConfig extracts and validates config from the
// generic Config map on HookDeclaration.
func parseClusterRegisterConfig(raw map[string]interface{}) (*clusterRegisterConfig, error) {
	if raw == nil {
		return nil, errors.New("cluster-register hook requires a config block")
	}

	getString := func(key string) (string, error) {
		v, ok := raw[key]
		if !ok {
			return "", errors.Newf("config.%s is required", key)
		}
		s, ok := v.(string)
		if !ok {
			return "", errors.Newf("config.%s must be a string, got %T", key, v)
		}
		if s == "" {
			return "", errors.Newf("config.%s must not be empty", key)
		}
		return s, nil
	}

	provider, err := getString("provider")
	if err != nil {
		return nil, err
	}
	remoteUser, err := getString("remote_user")
	if err != nil {
		return nil, err
	}
	projectExpr, err := getString("project_expr")
	if err != nil {
		return nil, err
	}
	vmsExpr, err := getString("vms_expr")
	if err != nil {
		return nil, err
	}

	return &clusterRegisterConfig{
		Provider:    provider,
		RemoteUser:  remoteUser,
		ProjectExpr: projectExpr,
		VMsExpr:     vmsExpr,
	}, nil
}

// ClusterRegisterExecutor handles the cluster-register hook type.
// On post_apply: builds a cluster from provisioning outputs and registers it.
// On pre_destroy: unregisters the cluster by name after verifying ownership.
type ClusterRegisterExecutor struct {
	registrar ClusterRegistrar
}

// NewClusterRegisterExecutor creates a new executor for cluster-register hooks.
func NewClusterRegisterExecutor(registrar ClusterRegistrar) *ClusterRegisterExecutor {
	return &ClusterRegisterExecutor{registrar: registrar}
}

// Execute dispatches to register or unregister based on the trigger.
// Skips silently if ClusterName is not set on the provisioning.
func (e *ClusterRegisterExecutor) Execute(
	ctx context.Context, l *logger.Logger, hctx HookContext,
) error {
	clusterName := hctx.Provisioning.ClusterName
	if clusterName == "" {
		l.Debug("no cluster_name set on provisioning, skipping cluster-register hook",
			slog.String("provisioning_id", hctx.Provisioning.ID.String()),
		)
		return nil
	}

	switch hctx.Trigger {
	case provmodels.TriggerPostApply:
		return e.register(ctx, l, hctx)
	case provmodels.TriggerPreDestroy:
		return e.unregister(ctx, l, hctx.Provisioning)
	default:
		return errors.Newf("cluster-register hook does not support trigger %q", hctx.Trigger)
	}
}

// register builds a cluster from provisioning outputs and registers it.
func (e *ClusterRegisterExecutor) register(
	ctx context.Context, l *logger.Logger, hctx HookContext,
) error {
	cfg, err := parseClusterRegisterConfig(hctx.Declaration.Config)
	if err != nil {
		return errors.Wrap(err, "parse cluster-register config")
	}

	cluster, err := buildClusterFromOutputs(hctx.Provisioning, cfg)
	if err != nil {
		return errors.Wrap(err, "build cluster from provisioning outputs")
	}

	// Validate that every VM carries a non-empty provisioning_identifier
	// label. Without it, the cluster won't be recognized as managed by
	// provisioning and will be unprotected from roachprod destroy and GC.
	for i, machine := range cluster.VMs {
		if machine.Labels[vm.TagProvisioningIdentifier] == "" {
			return errors.Newf(
				"VM %q (index %d) is missing a non-empty %s label in template output",
				machine.Name, i, vm.TagProvisioningIdentifier,
			)
		}
	}

	// Set cluster metadata from provisioning.
	cluster.User = hctx.Provisioning.Owner
	cluster.CreatedAt = hctx.Provisioning.CreatedAt
	cluster.Lifetime = hctx.Provisioning.Lifetime

	// Compute the managed flag so it is present immediately on registration.
	// The VMs carry the provisioning_identifier label (set by the template),
	// so IsProvisioningManaged() returns true.
	cluster.ManagedByProvisioning = cluster.IsProvisioningManaged()

	l.Info("registering cluster from provisioning",
		slog.String("cluster_name", cluster.Name),
		slog.Int("vm_count", len(cluster.VMs)),
		slog.String("provider", cfg.Provider),
	)
	return e.registrar.RegisterClusterInternal(ctx, l, *cluster)
}

// unregister removes the cluster, verifying ownership via provisioning identifier.
func (e *ClusterRegisterExecutor) unregister(
	ctx context.Context, l *logger.Logger, prov provmodels.Provisioning,
) error {
	l.Info("unregistering cluster",
		slog.String("cluster_name", prov.ClusterName),
		slog.String("provisioning_identifier", prov.Identifier),
	)
	return e.registrar.UnregisterClusterInternal(
		ctx, l, prov.ClusterName, prov.Owner, prov.Identifier,
	)
}

// buildClusterFromOutputs constructs a cloudcluster.Cluster from the
// provisioning's terraform outputs using jq expressions from the hook config.
//
// The jq expressions extract:
//   - project: a string identifying the cloud project/account
//   - vms: a list of objects with fixed field names
//
// Each VM object must have these fields:
//   - name (string): VM instance name
//   - zone (string): availability zone
//   - machine_type (string): instance type
//   - provider_id (string): cloud-provider instance ID
//   - private_ip (string): private network IP
//   - public_ip (string): public network IP
//   - preemptible (bool): whether the VM is preemptible/spot
//   - labels (map[string]string): cloud resource labels
func buildClusterFromOutputs(
	prov provmodels.Provisioning, cfg *clusterRegisterConfig,
) (*cloudcluster.Cluster, error) {
	if prov.Outputs == nil {
		return nil, errors.New("provisioning has no outputs")
	}

	// Extract project via jq.
	project, err := evaluateJQSingle(cfg.ProjectExpr, prov.Outputs)
	if err != nil {
		return nil, errors.Wrap(err, "extract project")
	}

	// Extract VMs via jq.
	vmMaps, err := evaluateJQObjects(cfg.VMsExpr, prov.Outputs)
	if err != nil {
		return nil, errors.Wrap(err, "extract VMs")
	}
	if len(vmMaps) == 0 {
		return nil, errors.Newf("jq expression %q returned no VMs", cfg.VMsExpr)
	}

	allVMs := make(vm.List, 0, len(vmMaps))
	for i, m := range vmMaps {
		v, err := mapToVM(m, project, cfg.Provider, cfg.RemoteUser)
		if err != nil {
			return nil, errors.Wrapf(err, "parse VM at index %d", i)
		}
		allVMs = append(allVMs, v)
	}

	// Sort VMs by name for consistency with cloud.CreateCluster
	// (cluster_cloud.go:352).
	sort.Sort(allVMs)

	// Check for duplicate VM names — these would make roachprod ssh
	// targeting ambiguous.
	for i := 1; i < len(allVMs); i++ {
		if allVMs[i].Name == allVMs[i-1].Name {
			return nil, errors.Newf(
				"duplicate VM name %q in template output", allVMs[i].Name,
			)
		}
	}

	// CloudProviders is "<provider>-<project>" — matches the format from
	// vm.Provider.String() and deriveProviderID() in environment.go.
	cloudProviders := []string{fmt.Sprintf("%s-%s", cfg.Provider, project)}

	return &cloudcluster.Cluster{
		Name:           prov.ClusterName,
		VMs:            allVMs,
		CloudProviders: cloudProviders,
	}, nil
}

// mapToVM converts a map from jq output to a vm.VM struct.
// Required fields: name, zone, machine_type, provider_id, private_ip, public_ip.
// Optional fields: preemptible (default false), labels (default empty).
func mapToVM(m map[string]interface{}, project, provider, remoteUser string) (vm.VM, error) {
	name, err := getStringField(m, "name")
	if err != nil {
		return vm.VM{}, err
	}
	zone, err := getStringField(m, "zone")
	if err != nil {
		return vm.VM{}, err
	}
	machineType, err := getStringField(m, "machine_type")
	if err != nil {
		return vm.VM{}, err
	}
	providerID, err := getStringField(m, "provider_id")
	if err != nil {
		return vm.VM{}, err
	}
	privateIP, err := getStringField(m, "private_ip")
	if err != nil {
		return vm.VM{}, err
	}
	publicIP, err := getStringField(m, "public_ip")
	if err != nil {
		return vm.VM{}, err
	}

	preemptible, _ := m["preemptible"].(bool)

	labels := make(map[string]string)
	if rawLabels, ok := m["labels"]; ok {
		if labelMap, ok := rawLabels.(map[string]interface{}); ok {
			for k, v := range labelMap {
				if s, ok := v.(string); ok {
					labels[k] = s
				}
			}
		}
	}

	return vm.VM{
		Name:        name,
		Zone:        zone,
		MachineType: machineType,
		ProviderID:  providerID,
		PrivateIP:   privateIP,
		PublicIP:    publicIP,
		Preemptible: preemptible,
		Project:     project,
		Provider:    provider,
		RemoteUser:  remoteUser,
		Labels:      labels,
	}, nil
}

// getStringField extracts a required string field from a map.
func getStringField(m map[string]interface{}, key string) (string, error) {
	v, ok := m[key]
	if !ok {
		return "", errors.Newf("missing required field %q", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", errors.Newf("field %q must be a string, got %T", key, v)
	}
	return s, nil
}
