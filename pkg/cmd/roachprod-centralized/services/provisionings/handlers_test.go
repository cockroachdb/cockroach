// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	provisioningsrepmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/provisionings/mocks"
	environmensmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/mocks"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/hooks"
	provisioningsmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/templates"
	provtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mockOrchestrator implements hooks.IOrchestrator for handler tests.
type mockOrchestrator struct {
	runPostApplyErr error
	called          bool
}

func (m *mockOrchestrator) RunPostApply(
	_ context.Context,
	_ *logger.Logger,
	_ provmodels.Provisioning,
	_ string,
	_ envtypes.ResolvedEnvironment,
) error {
	m.called = true
	return m.runPostApplyErr
}

func (m *mockOrchestrator) RunByType(
	_ context.Context,
	_ *logger.Logger,
	_ provmodels.Provisioning,
	_ provmodels.TemplateMetadata,
	_ string,
	_ envtypes.ResolvedEnvironment,
) error {
	return nil
}

// Compile-time check that mockOrchestrator implements IOrchestrator.
var _ hooks.IOrchestrator = (*mockOrchestrator)(nil)

func makeSnapshotFixture(t *testing.T) ([]byte, string, string) {
	t.Helper()

	templatesDir := t.TempDir()
	tmplDir := filepath.Join(templatesDir, "tmpl-dir")
	require.NoError(t, os.MkdirAll(tmplDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "template.yaml"), []byte(
		"name: tmpl-meta\ndescription: test template\n",
	), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "main.tf"), []byte(`
variable "identifier" { type = string }
output "identifier" { value = var.identifier }
`), 0o644))

	mgr := templates.NewManager(templatesDir)
	archive, checksum, err := mgr.SnapshotTemplate("tmpl-meta")
	require.NoError(t, err)
	return archive, checksum, templatesDir
}

func TestHandleProvision_CanceledContextStillPersistsFailure(t *testing.T) {
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	exec := provisioningsmock.NewIExecutor(t)

	archive, checksum, templatesDir := makeSnapshotFixture(t)
	svc := NewService(repo, envSvc, nil, provtypes.Options{
		TemplatesDir:      templatesDir,
		WorkingDirBase:    t.TempDir(),
		TofuBinary:        "tofu",
		WorkersEnabled:    true,
		DefaultLifetime:   12 * time.Hour,
		LifetimeExtension: 12 * time.Hour,
		GCWatcherInterval: 5 * time.Minute,
	}, templates.NewLocalBackend(), nil)
	svc.executor = exec

	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:               provID,
		Environment:      "env-a",
		TemplateType:     "tmpl-meta",
		TemplateChecksum: checksum,
		TemplateSnapshot: archive,
		Identifier:       "abc123de",
		Owner:            "owner@example.com",
		State:            provmodels.ProvisioningStateNew,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	repo.On("GetProvisioning", ctx, mock.Anything, provID).Return(prov, nil).Once()
	envSvc.On("GetEnvironmentResolved", ctx, mock.Anything, "env-a").
		Return(envtypes.ResolvedEnvironment{Name: "env-a"}, nil).Once()

	// First state update happens with original canceled context.
	repo.On("UpdateProvisioning", mock.MatchedBy(func(c context.Context) bool {
		return c.Err() != nil
	}), mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
		return p.ID == provID &&
			p.State == provmodels.ProvisioningStateInitializing &&
			p.LastStep == "init"
	})).Return(nil).Once()

	// Failure persistence must use context.WithoutCancel(ctx), so context isn't canceled.
	repo.On("UpdateProvisioning", mock.MatchedBy(func(c context.Context) bool {
		return c.Err() == nil
	}), mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
		return p.ID == provID &&
			p.State == provmodels.ProvisioningStateFailed &&
			p.LastStep == "init" &&
			p.Error != "" &&
			strings.Contains(p.Error, "tofu init")
	})).Return(nil).Once()

	exec.On("Init", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("boom")).Once()

	err := svc.HandleProvision(ctx, l, provID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tofu init")
}

// newHandlerTestService creates a Service with a mock executor and the given
// orchestrator. Used by the hook integration tests below.
func newHandlerTestService(
	t *testing.T, templatesDir string, orch hooks.IOrchestrator,
) (
	*Service,
	*provisioningsrepmock.IProvisioningsRepository,
	*environmensmock.IService,
	*provisioningsmock.IExecutor,
) {
	t.Helper()
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	exec := provisioningsmock.NewIExecutor(t)

	svc := NewService(repo, envSvc, nil, provtypes.Options{
		TemplatesDir:      templatesDir,
		WorkingDirBase:    t.TempDir(),
		TofuBinary:        "tofu",
		WorkersEnabled:    true,
		DefaultLifetime:   12 * time.Hour,
		LifetimeExtension: 12 * time.Hour,
		GCWatcherInterval: 5 * time.Minute,
	}, templates.NewLocalBackend(), orch)
	svc.executor = exec
	return svc, repo, envSvc, exec
}

// setupProvisionMocks configures mock expectations for a full HandleProvision
// flow through init → plan → apply → output. Returns the test provisioning.
func setupProvisionMocks(
	t *testing.T,
	repo *provisioningsrepmock.IProvisioningsRepository,
	envSvc *environmensmock.IService,
	exec *provisioningsmock.IExecutor,
	archive []byte,
	checksum string,
) (provmodels.Provisioning, uuid.UUID) {
	t.Helper()
	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:               provID,
		Environment:      "env-a",
		TemplateType:     "tmpl-meta",
		TemplateChecksum: checksum,
		TemplateSnapshot: archive,
		Identifier:       "abc123de",
		Owner:            "owner@example.com",
		State:            provmodels.ProvisioningStateNew,
	}

	// Use mock.Anything for context because failProvision uses
	// context.WithoutCancel + WithTimeout, producing a different ctx type.
	repo.On("GetProvisioning", mock.Anything, mock.Anything, provID).Return(prov, nil).Once()
	envSvc.On("GetEnvironmentResolved", mock.Anything, mock.Anything, "env-a").
		Return(envtypes.ResolvedEnvironment{Name: "env-a"}, nil).Once()

	// Allow all state updates throughout the lifecycle.
	repo.On("UpdateProvisioning", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Tofu steps: init, plan, apply, output — all succeed.
	// Plan is called twice: once for the initial plan, and once for
	// re-plan when the plan file is missing on disk (idempotency check).
	exec.On("Init", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	exec.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(true, json.RawMessage(nil), nil)
	exec.On("Apply", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	exec.On("Output", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]interface{}{"ip": "1.2.3.4"}, nil).Once()

	return prov, provID
}

func TestHandleProvision_HookFailure_FailsProvisioning(t *testing.T) {
	archive, checksum, templatesDir := makeSnapshotFixture(t)
	orch := &mockOrchestrator{runPostApplyErr: errors.New("hook exploded")}
	svc, repo, envSvc, exec := newHandlerTestService(t, templatesDir, orch)
	_, provID := setupProvisionMocks(t, repo, envSvc, exec, archive, checksum)

	err := svc.HandleProvision(context.Background(), logger.DefaultLogger, provID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "post-apply hooks")
	assert.True(t, orch.called, "orchestrator should have been called")

	// Verify the final state update was to failed.
	lastCall := repo.Calls[len(repo.Calls)-1]
	assert.Equal(t, "UpdateProvisioning", lastCall.Method)
	finalProv := lastCall.Arguments.Get(2).(provmodels.Provisioning)
	assert.Equal(t, provmodels.ProvisioningStateFailed, finalProv.State)
	assert.Contains(t, finalProv.Error, "post-apply hooks")
}

func TestHandleProvision_HooksSucceed_Provisioned(t *testing.T) {
	archive, checksum, templatesDir := makeSnapshotFixture(t)
	orch := &mockOrchestrator{} // no error
	svc, repo, envSvc, exec := newHandlerTestService(t, templatesDir, orch)
	_, provID := setupProvisionMocks(t, repo, envSvc, exec, archive, checksum)

	err := svc.HandleProvision(context.Background(), logger.DefaultLogger, provID)
	require.NoError(t, err)
	assert.True(t, orch.called, "orchestrator should have been called")

	// Verify the final state update was to provisioned.
	lastCall := repo.Calls[len(repo.Calls)-1]
	assert.Equal(t, "UpdateProvisioning", lastCall.Method)
	finalProv := lastCall.Arguments.Get(2).(provmodels.Provisioning)
	assert.Equal(t, provmodels.ProvisioningStateProvisioned, finalProv.State)
	assert.Equal(t, "done", finalProv.LastStep)
}
