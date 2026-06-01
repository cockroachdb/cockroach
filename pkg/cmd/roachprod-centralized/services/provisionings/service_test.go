// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/templates"
	provtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/vars"
	tasksmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/mocks"
	stasktypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type fakeArtifactStore struct {
	objects   map[string][]byte
	putErr    error
	deleteErr error
	deletes   []string
}

func newFakeArtifactStore() *fakeArtifactStore {
	return &fakeArtifactStore{objects: make(map[string][]byte)}
}

func (s *fakeArtifactStore) Put(
	ctx context.Context, objectKey string, r io.Reader, contentType string,
) (string, error) {
	if s.putErr != nil {
		return "", s.putErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	ref := "gs://test-bucket/" + objectKey
	s.objects[ref] = data
	return ref, nil
}

func (s *fakeArtifactStore) NewReader(ctx context.Context, ref string) (io.ReadCloser, error) {
	data, ok := s.objects[ref]
	if !ok {
		return nil, fmt.Errorf("artifact %s not found", ref)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *fakeArtifactStore) Delete(ctx context.Context, ref string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.objects, ref)
	s.deletes = append(s.deletes, ref)
	return nil
}

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

func writeTemplateFixtureWithLifetime(
	t *testing.T, root, dirName, templateName, tf, defaultLifetime string,
) {
	t.Helper()
	tmplDir := filepath.Join(root, dirName)
	require.NoError(t, os.MkdirAll(tmplDir, 0o755))
	yaml := fmt.Sprintf("name: %s\ndescription: test template\n", templateName)
	if defaultLifetime != "" {
		yaml += fmt.Sprintf("default_lifetime: %q\n", defaultLifetime)
	}
	require.NoError(t, os.WriteFile(
		filepath.Join(tmplDir, "template.yaml"), []byte(yaml), 0o644,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(tmplDir, "main.tf"), []byte(tf), 0o644,
	))
}

func newTestService(
	templatesDir string,
	repo provrepo.IProvisioningsRepository,
	envSvc envtypes.IService,
	taskSvc stasktypes.IService,
	workingDirBase string,
) *Service {
	return newTestServiceWithArtifactStore(
		templatesDir, repo, envSvc, taskSvc, workingDirBase, "repository", nil,
	)
}

func newTestServiceWithArtifactStore(
	templatesDir string,
	repo provrepo.IProvisioningsRepository,
	envSvc envtypes.IService,
	taskSvc stasktypes.IService,
	workingDirBase string,
	artifactBackend string,
	artifactStore *fakeArtifactStore,
) *Service {
	return NewService(repo, envSvc, taskSvc, provtypes.Options{
		TemplatesDir:      templatesDir,
		WorkingDirBase:    workingDirBase,
		TofuBinary:        "tofu",
		ArtifactBackend:   artifactBackend,
		WorkersEnabled:    true,
		DefaultLifetime:   12 * time.Hour,
		LifetimeExtension: 12 * time.Hour,
		GCWatcherInterval: 5 * time.Minute,
	}, templates.NewLocalBackend(), artifactStore, nil)
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
			strings.HasPrefix(p.Name, "owner-tmpl-meta-") &&
			p.ClusterName == p.Name &&
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

func TestCreateProvisioning_GCSArtifactsStoresTemplateRef(t *testing.T) {
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
	artifactStore := newFakeArtifactStore()
	svc := newTestServiceWithArtifactStore(
		templatesDir, repo, envSvc, taskSvc, t.TempDir(), "gcs", artifactStore,
	)

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
			p.TemplateSnapshot == nil &&
			strings.HasPrefix(p.TemplateSnapshotRef, "gs://test-bucket/") &&
			p.TemplateChecksum != ""
	})).Return(nil).Once()

	createdTaskID := uuid.MakeV4()
	taskSvc.On("CreateTask", ctx, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		task := args.Get(2).(taskmodels.ITask)
		task.SetID(createdTaskID)
	}).Return(nil, nil).Once()

	prov, taskID, err := svc.CreateProvisioning(ctx, l, principal, input)
	require.NoError(t, err)
	require.NotNil(t, taskID)
	require.NotEmpty(t, prov.TemplateSnapshotRef)
	require.Nil(t, prov.TemplateSnapshot)
	_, ok := artifactStore.objects[prov.TemplateSnapshotRef]
	require.True(t, ok, "expected template snapshot to be stored externally")
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
	}

	repo.On("GetProvisioningSummary", ctx, mock.Anything, provID).Return(prov, nil).Once()
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
	repo.AssertNotCalled(t, "UpdateProvisioningProgress", mock.Anything, mock.Anything, mock.Anything)
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
	}

	repo.On("GetProvisioningSummary", ctx, mock.Anything, provID).Return(prov, nil).Once()
	taskSvc.On("GetTasks", ctx, mock.Anything, (*auth.Principal)(nil), mock.Anything).
		Return([]taskmodels.ITask{}, 0, nil).Once()
	repo.On("UpdateProvisioningProgress", ctx, mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
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
		ID:      provID,
		Owner:   "owner@example.com",
		State:   provmodels.ProvisioningStateNew,
		Outputs: map[string]interface{}{"k": "v"},
	}

	repo.On("GetProvisioningSummary", ctx, mock.Anything, provID).Return(prov, nil).Once()
	repo.On("UpdateProvisioningProgress", ctx, mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
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
		ID:      provID,
		Owner:   "owner@example.com",
		State:   provmodels.ProvisioningStateProvisioned,
		Outputs: map[string]interface{}{"ip": "1.2.3.4"},
	}
	repo.On("GetProvisioningSummary", ctx, mock.Anything, provID).Return(prov, nil).Once()

	got, err := svc.GetProvisioning(ctx, l, principal, provID)
	require.NoError(t, err)
	assert.Equal(t, provID, got.ID)
	assert.Nil(t, got.PlanOutput)
	assert.Nil(t, got.Outputs)
}

func TestGetProvisioningPlan_LoadsExternalArtifact(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	artifactStore := newFakeArtifactStore()
	svc := newTestServiceWithArtifactStore(
		t.TempDir(), repo, envSvc, taskSvc, t.TempDir(), "repository", artifactStore,
	)

	principal := makePrincipal("owner@example.com", provtypes.PermissionViewAll)
	provID := uuid.MakeV4()
	ref := "gs://test-bucket/" + provID.String() + "/plan.json"
	artifactStore.objects[ref] = []byte(`{"changes":true}`)

	repo.On("GetProvisioning", ctx, mock.Anything, provID).Return(provmodels.Provisioning{
		ID:            provID,
		Owner:         "owner@example.com",
		PlanOutputRef: ref,
	}, nil).Once()

	plan, err := svc.GetProvisioningPlan(ctx, l, principal, provID)
	require.NoError(t, err)
	require.JSONEq(t, `{"changes":true}`, string(plan))
}

func TestGetProvisionings_ViewOwnFiltersAndStrips(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("alice@example.com", provtypes.PermissionViewOwn)
	repo.On("GetProvisioningsSummary", ctx, mock.Anything, mock.Anything).Return([]provmodels.Provisioning{
		{
			ID:      uuid.MakeV4(),
			Owner:   "alice@example.com",
			Outputs: map[string]interface{}{"k": "v"},
		},
		{
			ID:      uuid.MakeV4(),
			Owner:   "bob@example.com",
			Outputs: map[string]interface{}{"k": "v"},
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

func TestDeleteProvisioning_DeletesArtifactsBeforeRowDelete(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	artifactStore := newFakeArtifactStore()
	svc := newTestServiceWithArtifactStore(
		t.TempDir(), repo, envSvc, taskSvc, t.TempDir(), "repository", artifactStore,
	)

	principal := makePrincipal("owner@example.com", provtypes.PermissionDestroyAll)
	provID := uuid.MakeV4()
	templateRef := "gs://test-bucket/" + provID.String() + "/template.tar.gz"
	planRef := "gs://test-bucket/" + provID.String() + "/plan.json"
	artifactStore.objects[templateRef] = []byte("template")
	artifactStore.objects[planRef] = []byte(`{"changes":true}`)

	repo.On("GetProvisioning", ctx, mock.Anything, provID).Return(provmodels.Provisioning{
		ID:                  provID,
		Owner:               "owner@example.com",
		State:               provmodels.ProvisioningStateDestroyed,
		TemplateSnapshotRef: templateRef,
		PlanOutputRef:       planRef,
	}, nil).Once()
	repo.On("DeleteProvisioning", ctx, mock.Anything, provID).Return(nil).Once()

	err := svc.DeleteProvisioning(ctx, l, principal, provID)
	require.NoError(t, err)
	require.NotContains(t, artifactStore.objects, templateRef)
	require.NotContains(t, artifactStore.objects, planRef)
	require.ElementsMatch(t, []string{templateRef, planRef}, artifactStore.deletes)
}

// --- Step 1 tests: Default lifetime ---

func TestCreateProvisioning_DefaultLifetimeGlobal(t *testing.T) {
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
	repo.On("StoreProvisioning", ctx, mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
		return p.Lifetime == 12*time.Hour && p.ExpiresAt != nil
	})).Return(nil).Once()

	createdTaskID := uuid.MakeV4()
	taskSvc.On("CreateTask", ctx, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		task := args.Get(2).(taskmodels.ITask)
		task.SetID(createdTaskID)
	}).Return(nil, nil).Once()

	prov, _, err := svc.CreateProvisioning(ctx, l, principal, input)
	require.NoError(t, err)
	assert.Equal(t, 12*time.Hour, prov.Lifetime)
	require.NotNil(t, prov.ExpiresAt)
}

func TestCreateProvisioning_DefaultLifetimeTemplate(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger

	templatesDir := t.TempDir()
	writeTemplateFixtureWithLifetime(t, templatesDir, "tmpl-dir", "tmpl-meta", `
variable "identifier" { type = string }
`, "2h")

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
		return p.Lifetime == 2*time.Hour
	})).Return(nil).Once()

	createdTaskID := uuid.MakeV4()
	taskSvc.On("CreateTask", ctx, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		task := args.Get(2).(taskmodels.ITask)
		task.SetID(createdTaskID)
	}).Return(nil, nil).Once()

	prov, _, err := svc.CreateProvisioning(ctx, l, principal, input)
	require.NoError(t, err)
	assert.Equal(t, 2*time.Hour, prov.Lifetime)
}

func TestCreateProvisioning_UserLifetimeOverridesTemplate(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger

	templatesDir := t.TempDir()
	writeTemplateFixtureWithLifetime(t, templatesDir, "tmpl-dir", "tmpl-meta", `
variable "identifier" { type = string }
`, "2h")

	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(templatesDir, repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("owner@example.com", provtypes.PermissionCreate)
	input := provtypes.InputCreateDTO{
		Environment:  "env-a",
		TemplateType: "tmpl-meta",
		Lifetime:     "1h",
	}

	envSvc.On("GetEnvironment", ctx, mock.Anything, principal, "env-a").
		Return(envmodels.Environment{Name: "env-a"}, nil).Once()
	envSvc.On("GetEnvironmentResolved", ctx, mock.Anything, "env-a").
		Return(envtypes.ResolvedEnvironment{Name: "env-a"}, nil).Once()
	repo.On("StoreProvisioning", ctx, mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
		return p.Lifetime == 1*time.Hour
	})).Return(nil).Once()

	createdTaskID := uuid.MakeV4()
	taskSvc.On("CreateTask", ctx, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		task := args.Get(2).(taskmodels.ITask)
		task.SetID(createdTaskID)
	}).Return(nil, nil).Once()

	prov, _, err := svc.CreateProvisioning(ctx, l, principal, input)
	require.NoError(t, err)
	assert.Equal(t, 1*time.Hour, prov.Lifetime)
}

func TestCreateProvisioning_InvalidTemplateLifetimeFallsBack(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger

	templatesDir := t.TempDir()
	writeTemplateFixtureWithLifetime(t, templatesDir, "tmpl-dir", "tmpl-meta", `
variable "identifier" { type = string }
`, "invalid")

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
		return p.Lifetime == 12*time.Hour
	})).Return(nil).Once()

	createdTaskID := uuid.MakeV4()
	taskSvc.On("CreateTask", ctx, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		task := args.Get(2).(taskmodels.ITask)
		task.SetID(createdTaskID)
	}).Return(nil, nil).Once()

	prov, _, err := svc.CreateProvisioning(ctx, l, principal, input)
	require.NoError(t, err)
	assert.Equal(t, 12*time.Hour, prov.Lifetime)
}

// --- Step 2 tests: ExtendLifetime ---

func TestExtendLifetime_ExtendsExpiration(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("owner@example.com", provtypes.PermissionUpdateAll)
	provID := uuid.MakeV4()
	originalExpiry := time.Now().UTC().Add(2 * time.Hour)
	prov := provmodels.Provisioning{
		ID:        provID,
		Owner:     "owner@example.com",
		State:     provmodels.ProvisioningStateProvisioned,
		ExpiresAt: &originalExpiry,
		Outputs:   map[string]interface{}{"k": "v"},
	}

	repo.On("GetProvisioningSummary", ctx, mock.Anything, provID).Return(prov, nil).Once()
	repo.On("UpdateProvisioningExpiration", ctx, mock.Anything, provID, mock.MatchedBy(func(expiresAt *time.Time) bool {
		return expiresAt != nil &&
			expiresAt.After(originalExpiry.Add(11*time.Hour)) // at least 11h extension
	})).Return(nil).Once()

	got, err := svc.ExtendLifetime(ctx, l, principal, provID)
	require.NoError(t, err)
	assert.Equal(t, provID, got.ID)
	assert.Nil(t, got.PlanOutput)
	assert.Nil(t, got.Outputs)
}

func TestExtendLifetime_DestroyedReturnsError(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("owner@example.com", provtypes.PermissionUpdateAll)
	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:    provID,
		Owner: "owner@example.com",
		State: provmodels.ProvisioningStateDestroyed,
	}

	repo.On("GetProvisioningSummary", ctx, mock.Anything, provID).Return(prov, nil).Once()

	_, err := svc.ExtendLifetime(ctx, l, principal, provID)
	require.Error(t, err)
	assert.ErrorIs(t, err, provtypes.ErrInvalidState)
	repo.AssertNotCalled(t, "UpdateProvisioningExpiration", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestExtendLifetime_UpdateOwnCannotExtendOthers(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("alice@example.com", provtypes.PermissionUpdateOwn)
	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:    provID,
		Owner: "bob@example.com",
		State: provmodels.ProvisioningStateProvisioned,
	}

	repo.On("GetProvisioningSummary", ctx, mock.Anything, provID).Return(prov, nil).Once()

	_, err := svc.ExtendLifetime(ctx, l, principal, provID)
	require.Error(t, err)
	assert.ErrorIs(t, err, provtypes.ErrProvisioningNotFound)
}

func TestExtendLifetime_UpdateAllCanExtendOthers(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("alice@example.com", provtypes.PermissionUpdateAll)
	provID := uuid.MakeV4()
	expiry := time.Now().UTC().Add(1 * time.Hour)
	prov := provmodels.Provisioning{
		ID:        provID,
		Owner:     "bob@example.com",
		State:     provmodels.ProvisioningStateProvisioned,
		ExpiresAt: &expiry,
	}

	repo.On("GetProvisioningSummary", ctx, mock.Anything, provID).Return(prov, nil).Once()
	repo.On("UpdateProvisioningExpiration", ctx, mock.Anything, provID, mock.Anything).Return(nil).Once()

	got, err := svc.ExtendLifetime(ctx, l, principal, provID)
	require.NoError(t, err)
	assert.Equal(t, provID, got.ID)
}

func TestExtendLifetime_NilExpiresAt(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	principal := makePrincipal("owner@example.com", provtypes.PermissionUpdateAll)
	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:    provID,
		Owner: "owner@example.com",
		State: provmodels.ProvisioningStateProvisioned,
	}

	repo.On("GetProvisioningSummary", ctx, mock.Anything, provID).Return(prov, nil).Once()
	repo.On("UpdateProvisioningExpiration", ctx, mock.Anything, provID, mock.MatchedBy(func(expiresAt *time.Time) bool {
		return expiresAt != nil
	})).Return(nil).Once()

	got, err := svc.ExtendLifetime(ctx, l, principal, provID)
	require.NoError(t, err)
	require.NotNil(t, got.ExpiresAt)
}

// --- Step 3 tests: GC watcher ---

func TestHandleGC_NewStateMarksDestroyed(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	provID := uuid.MakeV4()
	repo.On("GetExpiredProvisionings", ctx, mock.Anything).Return([]provmodels.Provisioning{{
		ID:    provID,
		State: provmodels.ProvisioningStateNew,
	}}, nil).Once()

	repo.On("UpdateProvisioningProgress", ctx, mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
		return p.ID == provID && p.State == provmodels.ProvisioningStateDestroyed
	})).Return(nil).Once()

	err := svc.HandleGC(ctx, l)
	require.NoError(t, err)
	taskSvc.AssertNotCalled(t, "CreateTask", mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleGC_ProvisionedSchedulesDestroy(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	provID := uuid.MakeV4()
	repo.On("GetExpiredProvisionings", ctx, mock.Anything).Return([]provmodels.Provisioning{{
		ID:    provID,
		State: provmodels.ProvisioningStateProvisioned,
	}}, nil).Once()

	// hasActiveTask returns no active tasks.
	taskSvc.On("GetTasks", ctx, mock.Anything, (*auth.Principal)(nil), mock.Anything).
		Return([]taskmodels.ITask{}, 0, nil).Once()

	repo.On("UpdateProvisioningProgress", ctx, mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
		return p.ID == provID && p.State == provmodels.ProvisioningStateDestroying
	})).Return(nil).Once()

	taskSvc.On("CreateTask", ctx, mock.Anything, mock.MatchedBy(func(task taskmodels.ITask) bool {
		return task.GetType() == string(ptasks.ProvisioningsTaskDestroy)
	})).Return(nil, nil).Once()

	err := svc.HandleGC(ctx, l)
	require.NoError(t, err)
	repo.AssertCalled(t, "UpdateProvisioningProgress", ctx, mock.Anything, mock.Anything)
	taskSvc.AssertCalled(t, "CreateTask", ctx, mock.Anything, mock.Anything)
}

func TestHandleGC_InFlightWithActiveTaskSkips(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	provID := uuid.MakeV4()
	repo.On("GetExpiredProvisionings", ctx, mock.Anything).Return([]provmodels.Provisioning{{
		ID:    provID,
		State: provmodels.ProvisioningStatePlanning,
	}}, nil).Once()

	// hasActiveTask returns an active task.
	taskSvc.On("GetTasks", ctx, mock.Anything, (*auth.Principal)(nil), mock.Anything).
		Return([]taskmodels.ITask{
			&taskmodels.Task{ID: uuid.MakeV4(), State: taskmodels.TaskStateRunning},
		}, 1, nil).Once()

	err := svc.HandleGC(ctx, l)
	require.NoError(t, err)
	repo.AssertNotCalled(t, "UpdateProvisioningProgress", mock.Anything, mock.Anything, mock.Anything)
	taskSvc.AssertNotCalled(t, "CreateTask", mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleGC_InFlightStaleSchedulesDestroy(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	provID := uuid.MakeV4()
	repo.On("GetExpiredProvisionings", ctx, mock.Anything).Return([]provmodels.Provisioning{{
		ID:    provID,
		State: provmodels.ProvisioningStateInitializing,
	}}, nil).Once()

	taskSvc.On("GetTasks", ctx, mock.Anything, (*auth.Principal)(nil), mock.Anything).
		Return([]taskmodels.ITask{}, 0, nil).Once()

	repo.On("UpdateProvisioningProgress", ctx, mock.Anything, mock.MatchedBy(func(p provmodels.Provisioning) bool {
		return p.ID == provID && p.State == provmodels.ProvisioningStateDestroying
	})).Return(nil).Once()

	taskSvc.On("CreateTask", ctx, mock.Anything, mock.MatchedBy(func(task taskmodels.ITask) bool {
		return task.GetType() == string(ptasks.ProvisioningsTaskDestroy)
	})).Return(nil, nil).Once()

	err := svc.HandleGC(ctx, l)
	require.NoError(t, err)
}

func TestGCScheduleDestroy_DuplicatePrevention(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	repo := provisioningsrepmock.NewIProvisioningsRepository(t)
	envSvc := environmensmock.NewIService(t)
	taskSvc := tasksmock.NewIService(t)
	svc := newTestService(t.TempDir(), repo, envSvc, taskSvc, t.TempDir())

	provID := uuid.MakeV4()
	prov := provmodels.Provisioning{
		ID:    provID,
		State: provmodels.ProvisioningStateProvisioned,
	}

	// hasActiveTask returns an existing pending task.
	taskSvc.On("GetTasks", ctx, mock.Anything, (*auth.Principal)(nil), mock.Anything).
		Return([]taskmodels.ITask{
			&taskmodels.Task{ID: uuid.MakeV4(), State: taskmodels.TaskStatePending},
		}, 1, nil).Once()

	svc.gcScheduleDestroy(ctx, l, &prov)
	repo.AssertNotCalled(t, "UpdateProvisioningProgress", mock.Anything, mock.Anything, mock.Anything)
	taskSvc.AssertNotCalled(t, "CreateTask", mock.Anything, mock.Anything, mock.Anything)
}

// --- vars.ProvName tests ---

func TestProvName(t *testing.T) {
	tests := []struct {
		owner        string
		templateType string
		identifier   string
		want         string
	}{{
		owner:        "ludo.leroux@cockroachlabs.com",
		templateType: "gce-standalone",
		identifier:   "abc12def",
		want:         "ludoleroux-gce-standalone-abc12def",
	}, {
		owner:        "admin",
		templateType: "tmpl",
		identifier:   "xyz12345",
		want:         "admin-tmpl-xyz12345",
	}, {
		owner:        "",
		templateType: "tmpl",
		identifier:   "xyz12345",
		want:         "tmpl-xyz12345",
	}, {
		owner:        "user@example.com",
		templateType: "gce-my-template",
		identifier:   "abcd1234",
		want:         "user-gce-my-template-abcd1234",
	}, {
		owner:        "Alice.Bob@example.com",
		templateType: "aws-test",
		identifier:   "12345678",
		want:         "alicebob-aws-test-12345678",
	}}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%s-%s", tt.owner, tt.templateType, tt.identifier), func(t *testing.T) {
			got := vars.ProvName(tt.owner, tt.templateType, tt.identifier)
			assert.Equal(t, tt.want, got)
		})
	}
}
