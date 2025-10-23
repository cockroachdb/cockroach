// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package health

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	healthtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/health/types"
	"github.com/gin-gonic/gin"
)

const (
	// ControllerPath is the path for the health controller.
	ControllerPath = "/v1/health"
)

// Controller is the health controller.
type Controller struct {
	*controllers.Controller
	service  healthtypes.IHealthService
	handlers []controllers.IControllerHandler
}

// NewController creates a new health controller.
func NewController(service healthtypes.IHealthService) (ctrl *Controller) {
	ctrl = &Controller{
		Controller: controllers.NewDefaultController(),
		service:    service,
	}
	ctrl.handlers = []controllers.IControllerHandler{
		&controllers.ControllerHandler{
			Method:         "GET",
			Path:           ControllerPath,
			Func:           ctrl.Ping,
			Authentication: controllers.AuthenticationTypeNone,
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   ControllerPath + "/diagnostics/tempdirs",
			Func:   ctrl.TempDirs,
		},
	}
	return
}

// GetHandlers returns the controller's handlers, as required
func (ctrl *Controller) GetHandlers() []controllers.IControllerHandler {
	return ctrl.handlers
}

// Ping returns pong
func (ctrl *Controller) Ping(c *gin.Context) {
	ctrl.Render(c, &HealthDTO{})
}

// TempDirs returns diagnostics for temporary directories
func (ctrl *Controller) TempDirs(c *gin.Context) {
	diagnostics := ctrl.service.GetTempDirsDiagnostics(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
	)

	// Convert service types to controller DTOs
	dto := &TempDirsDTO{
		Directories: make([]*DiskUsageDTO, 0, len(diagnostics.Directories)),
	}

	for _, dir := range diagnostics.Directories {
		files := make([]*FileInfo, 0, len(dir.Files))
		for _, f := range dir.Files {
			files = append(files, &FileInfo{
				Path:         f.Path,
				SizeBytes:    f.SizeBytes,
				ModifiedTime: f.ModifiedTime,
				IsDir:        f.IsDir,
			})
		}

		dto.Directories = append(dto.Directories, &DiskUsageDTO{
			Path:        dir.Path,
			TotalBytes:  dir.TotalBytes,
			UsedBytes:   dir.UsedBytes,
			AvailBytes:  dir.AvailBytes,
			UsedPercent: dir.UsedPercent,
			FileCount:   dir.FileCount,
			Files:       files,
			Error:       dir.Error,
		})
	}

	ctrl.Render(c, dto)
}
