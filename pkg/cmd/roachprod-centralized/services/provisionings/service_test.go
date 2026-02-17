// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	taskmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	provrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/provisionings"
	provisioningsrepmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/provisionings/mocks"
	environmensmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/mocks"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	ptasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/tasks"
	provtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/types"
	tasksmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/mocks"
	stasktypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func makePrincipal(email string, perms ...string) *auth.Principal {
	permissions := make([]authmodels.Permission, 0, len(perms))
	for _, p := range perms {
		permissions = append(permissions, &authmodels.UserPermission{
			Scope:      "*",
			Permission: p,
		})
	}
	return &auth.Principal{
		User:        &authmodels.User{Email: email},
		Permissions: permissions,
	}
}

func writeTemplateFixture(t *testing.T, root, dirName, templateName, tf string) {
	t.Helper()
	tmplDir := filepath.Join(root, dirName)
	require.NoError(t, os.MkdirAll(tmplDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(tmplDir, "template.yaml"),
		[]byte(fmt.Sprintf("name: %s\ndescription: test template\n", templateName)),
		0o644,
	))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "main.tf"), []byte(tf), 0o644))
}

func newTestService(
	templatesDir string,
	repo provrepo.IProvisioningsRepository,
	envSvc envtypes.IService,
	taskSvc stasktypes.IService,
	workingDirBase string,
) *Service {
	return NewService(repo, envSvc, taskSvc, provtypes.Options{
		TemplatesDir:   templatesDir,
		WorkingDirBase: workingDirBase,
		GCSStateBucket: "unit-test-bucket",
		TofuBinary:     "tofu",
		WorkersEnabled: true,
	})
}

func TestCreateProvisioning_SchedulesTask(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger

	templatesDir := t.TempDir()
	writeTemplateFixture(t, templatesDir, "tmpl-dir", "tmpl-meta", `
variable "identifier" { type = string }
output "identifier" { value = var.identifier }
`)

	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(templatesDir, repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("owner@example.com", provtypes.PermissionCreate)
	input := provtypes.InputCreateDTO{
		Environment:  "env-a",
		TemplateType: "tmpl-meta",
	}

	envSvc.On("GetEnvironment", ctx, mock.Anything, principal, "env-a").
		Return(envmodels.Environment{Name: "env-a"}, nil).Once()
	envSvc.On("GetEnvironmentResolved", ctx, mock.Anything, "env-a").
		Return(envtypes.ResolvedEnvironment{Name: "env-a"}, nil).Once()

	repo.On("StoreProvisioning", ctx, mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
		return p.Environment == "env-a" &&
			p.TemplateType == "tmpl-meta" &&
			p.State == provmodels.ProvisioningStateNew &&
			p.Owner == "owner@example.com" &&
			len(p.Identifier) == provmodels.IdentifierLength &&
			strings.HasPrefix(p.Name, "tmpl-meta-") &&
			len(p.TemplateSnapshot) > 0 &&
			p.TemplateChecksum != ""
	})).Return(nil).Once()

	createdTaskID := uuid.MakeV4()
	taskSvc.On("CreateTask", ctx, mock.Anything, mock.MatchedBy(func(task taskmodels.ITask) bool {
		return task.GetType() == string(ptasks.ProvisioningsTaskProvision) &&
			strings.HasPrefix(task.GetReference(), ptasks.ReferencePrefix)
	})).Run(func(args mock.Arguments) {
		task := args.Get(2).(taskmodels.ITask)
		task.SetID(createdTaskID)
	}).Return(nil, nil).Once()

	prov, taskID, err := svc.CreateProvisioning(ctx, l, principal, input)
	require.NoError(t, err)
	require.NotNil(t, taskID)
	assert.Equal(t, createdTaskID, *taskID)
	assert.Equal(t, "env-a", prov.Environment)
	assert.Equal(t, "tmpl-meta", prov.TemplateType)
	assert.Equal(t, provmodels.ProvisioningStateNew, prov.State)
	assert.Equal(t, "owner@example.com", prov.Owner)
}

func TestCreateProvisioning_MissingRequiredVariable(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger

	templatesDir := t.TempDir()
	writeTemplateFixture(t, templatesDir, "tmpl-dir", "tmpl-meta", `
variable "identifier" { type = string }
variable "instance_status" { type = string }
`)

	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(templatesDir, repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("owner@example.com", provtypes.PermissionCreate)
	input := provtypes.InputCreateDTO{
		Environment:  "env-a",
		TemplateType: "tmpl-meta",
	}

	envSvc.On("GetEnvironment", ctx, mock.Anything, principal, "env-a").
		Return(envmodels.Environment{Name: "env-a"}, nil).Once()
	envSvc.On("GetEnvironmentResolved", ctx, mock.Anything, "env-a").
		Return(envtypes.ResolvedEnvironment{Name: "env-a"}, nil).Once()

	_, taskID, err := svc.CreateProvisioning(ctx, l, principal, input)
	require.Error(t, err)
	assert.Nil(t, taskID)
	assert.Contains(t, err.Error(), "missing required variable(s): instance_status")
	repo.AssertNotCalled(t, "StoreProvisioning", mock.Anything, mock.Anything, mock.Anything)
	taskSvc.AssertNotCalled(t, "CreateTask", mock.Anything, mock.Anything, mock.Anything)
}

func TestCreateProvisioning_IdentifierCollisionRetries(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger

	templatesDir := t.TempDir()
	writeTemplateFixture(t, templatesDir, "tmpl-dir", "tmpl-meta", `
variable "identifier" { type = string }
`)

	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(templatesDir, repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("owner@example.com", provtypes.PermissionCreate)
	input := provtypes.InputCreateDTO{
		Environment:  "env-a",
		TemplateType: "tmpl-meta",
	}

	envSvc.On("GetEnvironment", ctx, mock.Anything, principal, "env-a").
		Return(envmodels.Environment{Name: "env-a"}, nil).Once()
	envSvc.On("GetEnvironmentResolved", ctx, mock.Anything, "env-a").
		Return(envtypes.ResolvedEnvironment{Name: "env-a"}, nil).Once()

	repo.On("StoreProvisioning", ctx, mock.Anything, mock.Anything).
		Return(provrepo.ErrProvisioningAlreadyExists).
		Times(int(provtypes.MaxIdentifierRetries))

	_, taskID, err := svc.CreateProvisioning(ctx, l, principal, input)
	require.Error(t, err)
	assert.ErrorIs(t, err, provtypes.ErrIdentifierCollision)
	assert.Nil(t, taskID)
	taskSvc.AssertNotCalled(t, "CreateTask", mock.Anything, mock.Anything, mock.Anything)
}

func TestDestroyProvisioning_InFlightActiveTaskBlocks(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("owner@example.com", provtypes.PermissionDestroyAll)
	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:      provID,
		Owner:   "owner@example.com",
		State:   provmodels.ProvisioningStatePlanning,
		Outputs: map[string]interface{}{"k": "v"},
		PlanOutput: json.RawMessage(`{
			"format_version":"1.0"
		}`),
	}

	repo.On("GetProvisioning", ctx, mock.Anything, provID).Return(prov, nil).Once()
	taskSvc.On("GetTasks", ctx, mock.Anything, (*auth.Principal)(nil), mock.MatchedBy(func(input stasktypes.InputGetAllTasksDTO) bool {
		if len(input.Filters.Filters) != 2 {
			return false
		}
		var sawRef bool
		var sawState bool
		expectedRef := ptasks.ReferencePrefix + provID.String()
		for _, f := range input.Filters.Filters {
			switch {
			case f.Field == "Reference" && f.Operator == filtertypes.OpEqual:
				ref, ok := f.Value.(string)
				sawRef = ok && ref == expectedRef
			case f.Field == "State" && f.Operator == filtertypes.OpIn:
				states, ok := f.Value.([]string)
				if !ok || len(states) != 2 {
					return false
				}
				sawPending := false
				sawRunning := false
				for _, s := range states {
					if s == string(taskmodels.TaskStatePending) {
						sawPending = true
					}
					if s == string(taskmodels.TaskStateRunning) {
						sawRunning = true
					}
				}
				sawState = sawPending && sawRunning
			}
		}
		return sawRef && sawState
	})).Return([]taskmodels.ITask{
		&taskmodels.Task{ID: uuid.MakeV4(), State: taskmodels.TaskStateRunning},
	}, 1, nil).Once()

	got, taskID, err := svc.DestroyProvisioning(ctx, l, principal, provID)
	require.Error(t, err)
	assert.ErrorIs(t, err, provtypes.ErrTaskInProgress)
	assert.Nil(t, taskID)
	assert.Equal(t, uuid.Nil, got.ID)
	repo.AssertNotCalled(t, "UpdateProvisioning", mock.Anything, mock.Anything, mock.Anything)
	taskSvc.AssertNotCalled(t, "CreateTask", mock.Anything, mock.Anything, mock.Anything)
}

func TestDestroyProvisioning_StaleInFlightSchedulesDestroy(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("owner@example.com", provtypes.PermissionDestroyAll)
	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:      provID,
		Owner:   "owner@example.com",
		State:   provmodels.ProvisioningStatePlanning,
		Outputs: map[string]interface{}{"k": "v"},
		PlanOutput: json.RawMessage(`{
			"format_version":"1.0"
		}`),
	}

	repo.On("GetProvisioning", ctx, mock.Anything, provID).Return(prov, nil).Once()
	taskSvc.On("GetTasks", ctx, mock.Anything, (*auth.Principal)(nil), mock.Anything).
		Return([]taskmodels.ITask{}, 0, nil).Once()
	repo.On("UpdateProvisioning", ctx, mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
		return p.ID == provID && p.State == provmodels.ProvisioningStateDestroying
	})).Return(nil).Once()

	createdTaskID := uuid.MakeV4()
	taskSvc.On("CreateTask", ctx, mock.Anything, mock.MatchedBy(func(task taskmodels.ITask) bool {
		return task.GetType() == string(ptasks.ProvisioningsTaskDestroy) &&
			task.GetReference() == ptasks.ReferencePrefix+provID.String()
	})).Run(func(args mock.Arguments) {
		task := args.Get(2).(taskmodels.ITask)
		task.SetID(createdTaskID)
	}).Return(nil, nil).Once()

	got, taskID, err := svc.DestroyProvisioning(ctx, l, principal, provID)
	require.NoError(t, err)
	require.NotNil(t, taskID)
	assert.Equal(t, createdTaskID, *taskID)
	assert.Equal(t, provID, got.ID)
	assert.Nil(t, got.PlanOutput)
	assert.Nil(t, got.Outputs)
}

func TestDestroyProvisioning_NewStateImmediateDestroy(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("owner@example.com", provtypes.PermissionDestroyAll)
	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:         provID,
		Owner:      "owner@example.com",
		State:      provmodels.ProvisioningStateNew,
		PlanOutput: json.RawMessage(`{"format_version":"1.0"}`),
		Outputs:    map[string]interface{}{"k": "v"},
	}

	repo.On("GetProvisioning", ctx, mock.Anything, provID).Return(prov, nil).Once()
	repo.On("UpdateProvisioning", ctx, mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
		return p.ID == provID && p.State == provmodels.ProvisioningStateDestroyed
	})).Return(nil).Once()

	got, taskID, err := svc.DestroyProvisioning(ctx, l, principal, provID)
	require.NoError(t, err)
	assert.Nil(t, taskID)
	assert.Equal(t, provID, got.ID)
	assert.Nil(t, got.PlanOutput)
	assert.Nil(t, got.Outputs)
	taskSvc.AssertNotCalled(t, "GetTasks", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	taskSvc.AssertNotCalled(t, "CreateTask", mock.Anything, mock.Anything, mock.Anything)
}

func TestGetProvisioning_StripsPlanAndOutputs(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("owner@example.com", provtypes.PermissionViewAll)
	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:         provID,
		Owner:      "owner@example.com",
		State:      provmodels.ProvisioningStateProvisioned,
		PlanOutput: json.RawMessage(`{"format_version":"1.0"}`),
		Outputs:    map[string]interface{}{"ip": "1.2.3.4"},
	}
	repo.On("GetProvisioning", ctx, mock.Anything, provID).Return(prov, nil).Once()

	got, err := svc.GetProvisioning(ctx, l, principal, provID)
	require.NoError(t, err)
	assert.Equal(t, provID, got.ID)
	assert.Nil(t, got.PlanOutput)
	assert.Nil(t, got.Outputs)
}

func TestGetProvisionings_ViewOwnFiltersAndStrips(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("alice@example.com", provtypes.PermissionViewOwn)
	repo.On("GetProvisionings", ctx, mock.Anything, mock.Anything).Return([]provmodels.Provisioning{
		{
			ID:         uuid.MakeV4(),
			Owner:      "alice@example.com",
			PlanOutput: json.RawMessage(`{"format_version":"1.0"}`),
			Outputs:    map[string]interface{}{"k": "v"},
		},
		{
			ID:         uuid.MakeV4(),
			Owner:      "bob@example.com",
			PlanOutput: json.RawMessage(`{"format_version":"1.0"}`),
			Outputs:    map[string]interface{}{"k": "v"},
		},
	}, 2, nil).Once()

	got, total, err := svc.GetProvisionings(ctx, l, principal, provtypes.InputGetAllDTO{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, 1, total)
	assert.Equal(t, "alice@example.com", got[0].Owner)
	assert.Nil(t, got[0].PlanOutput)
	assert.Nil(t, got[0].Outputs)
}
