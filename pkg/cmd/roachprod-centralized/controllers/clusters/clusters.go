// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters"
	"github.com/gin-gonic/gin"
)

var (
	ErrWrongClusterName = fmt.Errorf("name in URL does not match name in body")
)

// Controller is the clusters controller.
type Controller struct {
	*controllers.Controller
	service  clusters.IService
	handlers []controllers.IControllerHandler
}

// NewController creates a new clusters controller.
func NewController(service clusters.IService) *Controller {
	ctrl := &Controller{
		Controller: controllers.NewDefaultController(),
		service:    service,
	}
	ctrl.handlers = []controllers.IControllerHandler{
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   "/clusters",
			Func:   ctrl.GetAll,
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   "/clusters/:name",
			Func:   ctrl.GetOne,
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   "/clusters",
			Func:   ctrl.Create,
		},
		&controllers.ControllerHandler{
			Method: "PUT",
			Path:   "/clusters/:name",
			Func:   ctrl.Update,
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   "/clusters/:name",
			Func:   ctrl.Delete,
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   "/clusters/sync",
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
	var inputDTO InputGetAllDTO
	if err := c.ShouldBindQuery(&inputDTO); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	clusters, err := ctrl.service.GetAllClusters(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		inputDTO.ToServiceInputGetAllDTO(),
	)

	ctrl.Render(c, (&ClustersResult{}).FromService(clusters, err))
}

// GetOne returns a cluster from the clusters service.
func (ctrl *Controller) GetOne(c *gin.Context) {
	cluster, err := ctrl.service.GetCluster(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		clusters.InputGetClusterDTO{
			Name: c.Param("name"),
		},
	)

	ctrl.Render(c, (&ClusterResult{}).FromService(cluster, err))
}

// Create creates a cluster in the clusters service.
func (ctrl *Controller) Create(c *gin.Context) {
	var inputDTO InputCreateClusterDTO
	if err := c.ShouldBindJSON(&inputDTO); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	cluster, err := ctrl.service.CreateCluster(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		inputDTO.ToServiceInputCreateClusterDTO(),
	)

	ctrl.Render(c, (&ClusterResult{}).FromService(cluster, err))
}

// Update creates a cluster in the clusters service.
func (ctrl *Controller) Update(c *gin.Context) {
	var inputDTO InputUpdateClusterDTO
	if err := c.ShouldBindJSON(&inputDTO); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Make sure we force the name to be the one in the URL
	if inputDTO.Cluster.Name != c.Param("name") {
		ctrl.Render(c, &controllers.BadRequestResult{Error: ErrWrongClusterName})
		return
	}

	cluster, err := ctrl.service.UpdateCluster(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		inputDTO.ToServiceInputUpdateClusterDTO(),
	)

	ctrl.Render(c, (&ClusterResult{}).FromService(cluster, err))
}

// Delete deletes a cluster in the clusters service.
func (ctrl *Controller) Delete(c *gin.Context) {
	err := ctrl.service.DeleteCluster(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
		clusters.InputDeleteClusterDTO{
			Name: c.Param("name"),
		},
	)

	ctrl.Render(c, (&ClusterResult{}).FromService(nil, err))
}

// Sync triggers a clusters sync to the store.
func (ctrl *Controller) Sync(c *gin.Context) {
	task, err := ctrl.service.SyncClouds(
		c.Request.Context(),
		ctrl.GetRequestLogger(c),
	)

	ctrl.Render(c, (&TaskResult{}).FromService(task, err))
}
