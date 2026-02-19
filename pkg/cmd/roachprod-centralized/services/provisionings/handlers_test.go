// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"context"
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
	provisioningsmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/templates"
	provtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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
	}, templates.NewLocalBackend())
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
