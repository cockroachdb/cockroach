// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/clusters/types"
	clustermodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings"
	"github.com/gin-gonic/gin"
)

// Controller is the clusters controller.
type Controller struct {
	*controllers.Controller
	service  clustermodels.IService
	handlers []controllers.IControllerHandler
}

// NewController creates a new clusters controller.
func NewController(service clustermodels.IService) *Controller {
	ctrl := &Controller{
		Controller: controllers.NewDefaultController(),
		service:    service,
	}
	ctrl.handlers = []controllers.IControllerHandler{
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath,
			Func:   ctrl.GetAll,
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/:name",
			Func:   ctrl.GetOne,
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/register",
			Func:   ctrl.Register,
		},
		&controllers.ControllerHandler{
			Method: "PUT",
			Path:   types.ControllerPath + "/register/:name",
			Func:   ctrl.RegisterUpdate,
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/register/:name",
			Func:   ctrl.RegisterDelete,
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/sync",
			Func:   ctrl.Sync,
		},
	}
	return ctrl
}

// GetHandlers returns the controller's handlers, as required
// by the controllers.IController interface.
func (ctrl *Controller) GetHandlers() []controllers.IControllerHandler {
	return ctrl.handlers
}

// GetAll returns all clusters from the clusters service.
func (ctrl *Controller) GetAll(c *gin.Context) {
	var inputDTO types.InputGetAllDTO
	if err := c.ShouldBindWith(&inputDTO, bindings.StripeQuery); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	clusters, err := ctrl.service.GetAllClusters(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		inputDTO.ToServiceInputGetAllDTO(),
	)

	ctrl.Render(c, (&types.ClustersResult{}).FromService(clusters, err))
}

// GetOne returns a cluster from the clusters service.
func (ctrl *Controller) GetOne(c *gin.Context) {
	// Validate cluster name from URL parameter
	clusterName := c.Param("name")
	if err := controllers.ValidateInputSecurity(clusterName, 64); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	cluster, err := ctrl.service.GetCluster(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		clustermodels.InputGetClusterDTO{
			Name: clusterName,
		},
	)

	ctrl.Render(c, (&types.ClusterResult{}).FromService(cluster, err))
}

// Register registers an external cluster creation in the clusters service.
func (ctrl *Controller) Register(c *gin.Context) {
	var inputDTO types.InputRegisterClusterDTO
	if err := c.ShouldBindJSON(&inputDTO); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Additional security and business logic validation
	if err := inputDTO.Validate(); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	cluster, err := ctrl.service.RegisterCluster(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		inputDTO.ToServiceInputRegisterClusterDTO(),
	)

	ctrl.Render(c, (&types.ClusterResult{}).FromService(cluster, err))
}

// RegisterUpdate registers an external update to a cluster in the clusters service.
func (ctrl *Controller) RegisterUpdate(c *gin.Context) {
	var inputDTO types.InputRegisterClusterUpdateDTO
	if err := c.ShouldBindJSON(&inputDTO); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Validate cluster name from URL parameter
	clusterName := c.Param("name")
	if err := controllers.ValidateInputSecurity(clusterName, 64); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Make sure we force the name to be the one in the URL
	if inputDTO.Cluster.Name != clusterName {
		ctrl.Render(c, &controllers.BadRequestResult{Error: types.ErrWrongClusterName})
		return
	}

	// Additional security and business logic validation
	if err := inputDTO.Validate(); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	cluster, err := ctrl.service.RegisterClusterUpdate(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		inputDTO.ToServiceInputRegisterClusterUpdateDTO(),
	)

	ctrl.Render(c, (&types.ClusterResult{}).FromService(cluster, err))
}

// RegisterDelete registers an external deletion of a cluster in the clusters service.
func (ctrl *Controller) RegisterDelete(c *gin.Context) {
	// Validate cluster name from URL parameter
	clusterName := c.Param("name")
	if err := controllers.ValidateInputSecurity(clusterName, 64); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	err := ctrl.service.RegisterClusterDelete(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		clustermodels.InputRegisterClusterDeleteDTO{
			Name: clusterName,
		},
	)

	ctrl.Render(c, (&types.ClusterResult{}).FromService(nil, err))
}

// Sync triggers a clusters sync to the store.
func (ctrl *Controller) Sync(c *gin.Context) {
	task, err := ctrl.service.SyncClouds(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
	)

	ctrl.Render(c, (&types.TaskResult{}).FromService(task, err))
}
