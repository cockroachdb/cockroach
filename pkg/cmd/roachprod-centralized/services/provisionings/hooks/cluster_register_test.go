// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"context"
	"testing"
	"time"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockRegistrar records calls to RegisterClusterInternal and UnregisterClusterInternal.
type mockRegistrar struct {
	registerCalls   []cloudcluster.Cluster
	unregisterCalls []unregisterCall
	registerErr     error
	unregisterErr   error
}

type unregisterCall struct {
	clusterName            string
	provisioningOwner      string
	provisioningIdentifier string
}

func (m *mockRegistrar) RegisterClusterInternal(
	ctx context.Context, l *logger.Logger, cluster cloudcluster.Cluster,
) error {
	m.registerCalls = append(m.registerCalls, cluster)
	return m.registerErr
}

func (m *mockRegistrar) UnregisterClusterInternal(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	provisioningOwner string,
	provisioningIdentifier string,
) error {
	m.unregisterCalls = append(m.unregisterCalls, unregisterCall{
		clusterName:            clusterName,
		provisioningOwner:      provisioningOwner,
		provisioningIdentifier: provisioningIdentifier,
	})
	return m.unregisterErr
}

// testOutputs returns the standard test provisioning outputs with two VMs
// (vm-2 listed before vm-1 to verify sorting).
func testOutputs() map[string]interface{} {
	return map[string]interface{}{
		"data": map[string]interface{}{
			"project": "my-project",
			"vms": []interface{}{
				map[string]interface{}{
					"name": "vm-2", "zone": "us-central1-a",
					"machine_type": "n1-standard-4",
					"provider_id":  "inst-002",
					"private_ip":   "10.0.0.2", "public_ip": "35.0.0.2",
					"preemptible": false,
					"labels": map[string]interface{}{
						"provisioning_identifier": "abc123",
						"roachprod":               "true",
					},
				},
				map[string]interface{}{
					"name": "vm-1", "zone": "us-central1-b",
					"machine_type": "n1-standard-4",
					"provider_id":  "inst-001",
					"private_ip":   "10.0.0.1", "public_ip": "35.0.0.1",
					"preemptible": true,
					"labels": map[string]interface{}{
						"provisioning_identifier": "abc123",
						"roachprod":               "true",
					},
				},
			},
		},
	}
}

// testConfig returns the standard cluster-register hook config.
func testConfig() map[string]interface{} {
	return map[string]interface{}{
		"provider":     "gce",
		"remote_user":  "ubuntu",
		"project_expr": ".data.project",
		"vms_expr":     ".data.vms[]",
	}
}

func TestClusterRegisterExecutor_PostApply_ValidOutputs(t *testing.T) {
	reg := &mockRegistrar{}
	exec := NewClusterRegisterExecutor(reg)

	createdAt := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	lifetime := 24 * time.Hour

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "my-cluster",
		Owner:       "testuser",
		Identifier:  "abc123",
		CreatedAt:   createdAt,
		Lifetime:    lifetime,
		Outputs:     testOutputs(),
	}

	hctx := HookContext{
		Trigger:      provmodels.TriggerPostApply,
		Provisioning: prov,
		Declaration: provmodels.HookDeclaration{
			Name:     "register-cluster",
			Type:     "cluster-register",
			Triggers: []provmodels.HookTrigger{provmodels.TriggerPostApply},
			Config:   testConfig(),
		},
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.NoError(t, err)

	require.Len(t, reg.registerCalls, 1)
	cluster := reg.registerCalls[0]

	assert.Equal(t, "my-cluster", cluster.Name)
	assert.Equal(t, "testuser", cluster.User)
	assert.Equal(t, createdAt, cluster.CreatedAt)
	assert.Equal(t, lifetime, cluster.Lifetime)
	assert.True(t, cluster.ManagedByProvisioning)
	assert.Equal(t, []string{"gce-my-project"}, cluster.CloudProviders)

	// VMs should be sorted by name: vm-1 before vm-2.
	require.Len(t, cluster.VMs, 2)
	assert.Equal(t, "vm-1", cluster.VMs[0].Name)
	assert.Equal(t, "vm-2", cluster.VMs[1].Name)

	// Verify VM fields for the first (sorted) VM.
	assert.Equal(t, "us-central1-b", cluster.VMs[0].Zone)
	assert.Equal(t, "n1-standard-4", cluster.VMs[0].MachineType)
	assert.Equal(t, "inst-001", cluster.VMs[0].ProviderID)
	assert.Equal(t, "10.0.0.1", cluster.VMs[0].PrivateIP)
	assert.Equal(t, "35.0.0.1", cluster.VMs[0].PublicIP)
	assert.True(t, cluster.VMs[0].Preemptible)
	assert.Equal(t, "my-project", cluster.VMs[0].Project)
	assert.Equal(t, "gce", cluster.VMs[0].Provider)
	assert.Equal(t, "ubuntu", cluster.VMs[0].RemoteUser)

	// Verify VM fields for the second (sorted) VM.
	assert.Equal(t, "us-central1-a", cluster.VMs[1].Zone)
	assert.False(t, cluster.VMs[1].Preemptible)

	// No unregister calls should have been made.
	assert.Empty(t, reg.unregisterCalls)
}

func TestClusterRegisterExecutor_PostApply_EmptyClusterName(t *testing.T) {
	reg := &mockRegistrar{}
	exec := NewClusterRegisterExecutor(reg)

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "", // empty — should skip
		Owner:       "testuser",
		Outputs:     testOutputs(),
	}

	hctx := HookContext{
		Trigger:      provmodels.TriggerPostApply,
		Provisioning: prov,
		Declaration: provmodels.HookDeclaration{
			Name:     "register-cluster",
			Type:     "cluster-register",
			Triggers: []provmodels.HookTrigger{provmodels.TriggerPostApply},
			Config:   testConfig(),
		},
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.NoError(t, err)

	assert.Empty(t, reg.registerCalls)
	assert.Empty(t, reg.unregisterCalls)
}

func TestClusterRegisterExecutor_PreDestroy(t *testing.T) {
	reg := &mockRegistrar{}
	exec := NewClusterRegisterExecutor(reg)

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "doomed-cluster",
		Owner:       "destroyer",
		Identifier:  "xyz789",
	}

	hctx := HookContext{
		Trigger:      provmodels.TriggerPreDestroy,
		Provisioning: prov,
		Declaration: provmodels.HookDeclaration{
			Name:     "unregister-cluster",
			Type:     "cluster-register",
			Triggers: []provmodels.HookTrigger{provmodels.TriggerPreDestroy},
		},
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.NoError(t, err)

	require.Len(t, reg.unregisterCalls, 1)
	assert.Equal(t, "doomed-cluster", reg.unregisterCalls[0].clusterName)
	assert.Equal(t, "destroyer", reg.unregisterCalls[0].provisioningOwner)
	assert.Equal(t, "xyz789", reg.unregisterCalls[0].provisioningIdentifier)

	assert.Empty(t, reg.registerCalls)
}

func TestClusterRegisterExecutor_PreDestroy_EmptyClusterName(t *testing.T) {
	reg := &mockRegistrar{}
	exec := NewClusterRegisterExecutor(reg)

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "", // empty — should skip
		Owner:       "testuser",
		Identifier:  "abc123",
	}

	hctx := HookContext{
		Trigger:      provmodels.TriggerPreDestroy,
		Provisioning: prov,
		Declaration: provmodels.HookDeclaration{
			Name:     "unregister-cluster",
			Type:     "cluster-register",
			Triggers: []provmodels.HookTrigger{provmodels.TriggerPreDestroy},
		},
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.NoError(t, err)

	assert.Empty(t, reg.registerCalls)
	assert.Empty(t, reg.unregisterCalls)
}

func TestClusterRegisterExecutor_UnsupportedTrigger(t *testing.T) {
	reg := &mockRegistrar{}
	exec := NewClusterRegisterExecutor(reg)

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "my-cluster",
		Owner:       "testuser",
	}

	hctx := HookContext{
		Trigger:      provmodels.TriggerManual,
		Provisioning: prov,
		Declaration: provmodels.HookDeclaration{
			Name:     "register-cluster",
			Type:     "cluster-register",
			Triggers: []provmodels.HookTrigger{provmodels.TriggerManual},
		},
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support trigger")
	assert.Contains(t, err.Error(), "manual")

	assert.Empty(t, reg.registerCalls)
	assert.Empty(t, reg.unregisterCalls)
}

func TestClusterRegisterExecutor_MissingConfig(t *testing.T) {
	reg := &mockRegistrar{}
	exec := NewClusterRegisterExecutor(reg)

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "my-cluster",
		Owner:       "testuser",
		Outputs:     testOutputs(),
	}

	hctx := HookContext{
		Trigger:      provmodels.TriggerPostApply,
		Provisioning: prov,
		Declaration: provmodels.HookDeclaration{
			Name:     "register-cluster",
			Type:     "cluster-register",
			Triggers: []provmodels.HookTrigger{provmodels.TriggerPostApply},
			Config:   nil, // missing config
		},
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a config block")

	assert.Empty(t, reg.registerCalls)
}

func TestClusterRegisterExecutor_InvalidJQExpression(t *testing.T) {
	reg := &mockRegistrar{}
	exec := NewClusterRegisterExecutor(reg)

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "my-cluster",
		Owner:       "testuser",
		Outputs:     testOutputs(),
	}

	badConfig := map[string]interface{}{
		"provider":     "gce",
		"remote_user":  "ubuntu",
		"project_expr": ".[[[invalid",
		"vms_expr":     ".data.vms[]",
	}

	hctx := HookContext{
		Trigger:      provmodels.TriggerPostApply,
		Provisioning: prov,
		Declaration: provmodels.HookDeclaration{
			Name:     "register-cluster",
			Type:     "cluster-register",
			Triggers: []provmodels.HookTrigger{provmodels.TriggerPostApply},
			Config:   badConfig,
		},
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract project")

	assert.Empty(t, reg.registerCalls)
}

func TestClusterRegisterExecutor_ProjectExprMultipleValues(t *testing.T) {
	reg := &mockRegistrar{}
	exec := NewClusterRegisterExecutor(reg)

	// Outputs where the project_expr returns multiple values.
	outputs := map[string]interface{}{
		"data": map[string]interface{}{
			"projects": []interface{}{"project-a", "project-b"},
			"vms": []interface{}{
				map[string]interface{}{
					"name": "vm-1", "zone": "us-central1-a",
					"machine_type": "n1-standard-4",
					"provider_id":  "inst-001",
					"private_ip":   "10.0.0.1", "public_ip": "35.0.0.1",
					"preemptible": false,
				},
			},
		},
	}

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "my-cluster",
		Owner:       "testuser",
		Outputs:     outputs,
	}

	multiProjectConfig := map[string]interface{}{
		"provider":     "gce",
		"remote_user":  "ubuntu",
		"project_expr": ".data.projects[]",
		"vms_expr":     ".data.vms[]",
	}

	hctx := HookContext{
		Trigger:      provmodels.TriggerPostApply,
		Provisioning: prov,
		Declaration: provmodels.HookDeclaration{
			Name:     "register-cluster",
			Type:     "cluster-register",
			Triggers: []provmodels.HookTrigger{provmodels.TriggerPostApply},
			Config:   multiProjectConfig,
		},
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple results")

	assert.Empty(t, reg.registerCalls)
}

// Tests for buildClusterFromOutputs.

func TestBuildClusterFromOutputs_Valid(t *testing.T) {
	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "test-cluster",
		Owner:       "testuser",
		Outputs:     testOutputs(),
	}

	cfg := &clusterRegisterConfig{
		Provider:    "gce",
		RemoteUser:  "ubuntu",
		ProjectExpr: ".data.project",
		VMsExpr:     ".data.vms[]",
	}

	cluster, err := buildClusterFromOutputs(prov, cfg)
	require.NoError(t, err)

	assert.Equal(t, "test-cluster", cluster.Name)
	assert.Equal(t, []string{"gce-my-project"}, cluster.CloudProviders)

	// VMs should be sorted by name.
	require.Len(t, cluster.VMs, 2)
	assert.Equal(t, "vm-1", cluster.VMs[0].Name)
	assert.Equal(t, "vm-2", cluster.VMs[1].Name)

	// Verify project is set on each VM.
	assert.Equal(t, "my-project", cluster.VMs[0].Project)
	assert.Equal(t, "my-project", cluster.VMs[1].Project)

	// Labels from output should be present.
	assert.Equal(t, "abc123", cluster.VMs[0].Labels[vm.TagProvisioningIdentifier])
	assert.Equal(t, "true", cluster.VMs[0].Labels[vm.TagRoachprod])
}

func TestBuildClusterFromOutputs_NoOutputs(t *testing.T) {
	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "test-cluster",
		Outputs:     nil,
	}

	cfg := &clusterRegisterConfig{
		Provider:    "gce",
		RemoteUser:  "ubuntu",
		ProjectExpr: ".data.project",
		VMsExpr:     ".data.vms[]",
	}

	_, err := buildClusterFromOutputs(prov, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no outputs")
}

func TestBuildClusterFromOutputs_EmptyVMs(t *testing.T) {
	outputs := map[string]interface{}{
		"data": map[string]interface{}{
			"project": "my-project",
			"vms":     []interface{}{},
		},
	}

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "test-cluster",
		Outputs:     outputs,
	}

	cfg := &clusterRegisterConfig{
		Provider:    "gce",
		RemoteUser:  "ubuntu",
		ProjectExpr: ".data.project",
		VMsExpr:     ".data.vms[]",
	}

	_, err := buildClusterFromOutputs(prov, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no VMs")
}

func TestBuildClusterFromOutputs_MissingVMField(t *testing.T) {
	outputs := map[string]interface{}{
		"data": map[string]interface{}{
			"project": "my-project",
			"vms": []interface{}{
				map[string]interface{}{
					// "name" is deliberately missing.
					"zone":         "us-central1-a",
					"machine_type": "n1-standard-4",
					"provider_id":  "inst-001",
					"private_ip":   "10.0.0.1",
					"public_ip":    "35.0.0.1",
				},
			},
		},
	}

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "test-cluster",
		Outputs:     outputs,
	}

	cfg := &clusterRegisterConfig{
		Provider:    "gce",
		RemoteUser:  "ubuntu",
		ProjectExpr: ".data.project",
		VMsExpr:     ".data.vms[]",
	}

	_, err := buildClusterFromOutputs(prov, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name")
}

func TestBuildClusterFromOutputs_LabelsOnVMs(t *testing.T) {
	outputs := map[string]interface{}{
		"data": map[string]interface{}{
			"project": "label-project",
			"vms": []interface{}{
				map[string]interface{}{
					"name": "vm-labeled", "zone": "eu-west1-b",
					"machine_type": "e2-medium",
					"provider_id":  "inst-100",
					"private_ip":   "10.1.0.1", "public_ip": "34.1.0.1",
					"preemptible": false,
					"labels": map[string]interface{}{
						"provisioning_identifier": "label-id-42",
						"roachprod":               "true",
						"team":                    "infra",
					},
				},
			},
		},
	}

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "label-cluster",
		Outputs:     outputs,
	}

	cfg := &clusterRegisterConfig{
		Provider:    "gce",
		RemoteUser:  "ubuntu",
		ProjectExpr: ".data.project",
		VMsExpr:     ".data.vms[]",
	}

	cluster, err := buildClusterFromOutputs(prov, cfg)
	require.NoError(t, err)
	require.Len(t, cluster.VMs, 1)

	labels := cluster.VMs[0].Labels
	assert.Equal(t, "label-id-42", labels[vm.TagProvisioningIdentifier])
	assert.Equal(t, "true", labels[vm.TagRoachprod])
	assert.Equal(t, "infra", labels["team"])
	assert.Len(t, labels, 3)
}

func TestClusterRegisterExecutor_PostApply_RegistrarError(t *testing.T) {
	reg := &mockRegistrar{registerErr: errors.New("store unavailable")}
	exec := NewClusterRegisterExecutor(reg)

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "my-cluster",
		Owner:       "testuser",
		Identifier:  "abc123",
		Outputs:     testOutputs(),
	}

	hctx := HookContext{
		Trigger:      provmodels.TriggerPostApply,
		Provisioning: prov,
		Declaration: provmodels.HookDeclaration{
			Name:     "register-cluster",
			Type:     "cluster-register",
			Triggers: []provmodels.HookTrigger{provmodels.TriggerPostApply},
			Config:   testConfig(),
		},
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store unavailable")
}

func TestClusterRegisterExecutor_PreDestroy_UnregistrarError(t *testing.T) {
	reg := &mockRegistrar{unregisterErr: errors.New("unregister failed")}
	exec := NewClusterRegisterExecutor(reg)

	prov := provmodels.Provisioning{
		ID:          uuid.MakeV4(),
		ClusterName: "doomed-cluster",
		Owner:       "destroyer",
		Identifier:  "xyz789",
	}

	hctx := HookContext{
		Trigger:      provmodels.TriggerPreDestroy,
		Provisioning: prov,
		Declaration: provmodels.HookDeclaration{
			Name:     "unregister-cluster",
			Type:     "cluster-register",
			Triggers: []provmodels.HookTrigger{provmodels.TriggerPreDestroy},
		},
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unregister failed")
}
