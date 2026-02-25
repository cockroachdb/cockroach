// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const TaskServiceName = "provisionings"

// MaxIdentifierRetries is the maximum number of times to retry generating a
// unique identifier when a collision is detected.
const MaxIdentifierRetries = 3

// Options contains configuration parameters for the provisionings service.
type Options struct {
	TemplatesDir      string
	WorkingDirBase    string
	TofuBinary        string
	WorkersEnabled    bool
	DefaultLifetime   time.Duration
	LifetimeExtension time.Duration
	GCWatcherInterval time.Duration
}

var (
	ErrProvisioningNotFound      = utils.NewPublicError(fmt.Errorf("provisioning not found"))
	ErrProvisioningAlreadyExists = utils.NewPublicError(fmt.Errorf("provisioning already exists"))
	ErrTemplateNotFound          = utils.NewPublicError(fmt.Errorf("template not found"))
	ErrEnvironmentNotFound       = utils.NewPublicError(fmt.Errorf("environment not found"))
	ErrInvalidState              = utils.NewPublicError(fmt.Errorf("invalid provisioning state for this operation"))
	ErrTaskInProgress            = utils.NewPublicError(fmt.Errorf("a task is in progress for this provisioning"))
	ErrInvalidLifetime           = utils.NewPublicError(fmt.Errorf("invalid lifetime format; must be a duration string ('1h', '30m', etc.)"))
	ErrIdentifierCollision       = fmt.Errorf("identifier collision after max retries")
)

const (
	PermissionViewAll    = TaskServiceName + ":view:all"
	PermissionViewOwn    = TaskServiceName + ":view:own"
	PermissionCreate     = TaskServiceName + ":create"
	PermissionDestroyAll = TaskServiceName + ":destroy:all"
	PermissionDestroyOwn = TaskServiceName + ":destroy:own"
	PermissionUpdateAll  = TaskServiceName + ":update:all"
	PermissionUpdateOwn  = TaskServiceName + ":update:own"
)

// IService is the interface for the provisionings service.
type IService interface {
	GetTemplates(ctx context.Context, l *logger.Logger) ([]provmodels.Template, error)
	GetTemplate(ctx context.Context, l *logger.Logger, name string) (provmodels.Template, error)
	GetProvisioning(ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID) (provmodels.Provisioning, error)
	GetProvisionings(ctx context.Context, l *logger.Logger, principal *auth.Principal, input InputGetAllDTO) ([]provmodels.Provisioning, int, error)
	CreateProvisioning(ctx context.Context, l *logger.Logger, principal *auth.Principal, input InputCreateDTO) (provmodels.Provisioning, *uuid.UUID, error)
	DestroyProvisioning(ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID) (provmodels.Provisioning, *uuid.UUID, error)
	DeleteProvisioning(ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID) error
	GetProvisioningPlan(ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID) (json.RawMessage, error)
	GetProvisioningOutputs(ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID) (map[string]interface{}, error)
	ExtendLifetime(ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID) (provmodels.Provisioning, error)
	SetupSSHKeys(ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID) (provmodels.Provisioning, *uuid.UUID, error)
}

// IProvisioningTaskHandler is the interface used by task handlers to call
// back into the service for orchestration logic. This avoids import cycles
// between the tasks subpackage and the service package.
type IProvisioningTaskHandler interface {
	HandleProvision(ctx context.Context, l *logger.Logger, provisioningID uuid.UUID) error
	HandleDestroy(ctx context.Context, l *logger.Logger, provisioningID uuid.UUID) error
	HandleGC(ctx context.Context, l *logger.Logger) error
	HandleSetupSSHKeys(ctx context.Context, l *logger.Logger, provisioningID uuid.UUID) error
}

// InputCreateDTO is the service-layer DTO for creating a provisioning.
type InputCreateDTO struct {
	Environment  string                 `json:"environment" binding:"required"`
	TemplateType string                 `json:"template_type" binding:"required"`
	Variables    map[string]interface{} `json:"variables"`
	Lifetime     string                 `json:"lifetime"`
}

// InputGetAllDTO is the service-layer DTO for listing provisionings.
type InputGetAllDTO struct {
	Filters filtertypes.FilterSet
}
