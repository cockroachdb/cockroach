// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"sort"
	"strings"
)

// getMachineClass extracts the machine class from a machine type name.
// For GCE: "n2-standard-4" -> "n2-standard"
// For AWS: "m6i.xlarge" -> "m6i"
// For Azure: "Standard_D4s_v3" -> "Standard_D"
func getMachineClass(name string) string {
	// AWS pattern: family.size (e.g., m6i.xlarge)
	if strings.Contains(name, ".") {
		parts := strings.Split(name, ".")
		return parts[0]
	}

	// Azure pattern: Standard_Family... (e.g., Standard_D4s_v3)
	if strings.HasPrefix(name, "Standard_") {
		// Extract just the family letter (D, E, F, etc.)
		re := regexp.MustCompile(`^Standard_([A-Z]+)`)
		if matches := re.FindStringSubmatch(name); len(matches) > 1 {
			return "Standard_" + matches[1]
		}
	}

	// GCE pattern: family-type-size (e.g., n2-standard-4)
	// Extract everything before the last hyphen and number
	re := regexp.MustCompile(`^(.+)-\d+$`)
	if matches := re.FindStringSubmatch(name); len(matches) > 1 {
		return matches[1]
	}

	// Fallback: return the full name
	return name
}

// handleClusters handles GET requests to list clusters.
func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	// Get optional user filter
	userFilter := r.URL.Query().Get("user")

	// List clusters using cluster manager
	clusters, err := s.clusterManager.ListClusters(context.Background(), userFilter)
	if err != nil {
		// Check if error indicates SSO token expiration
		errStr := err.Error()
		if strings.Contains(errStr, "Token has expired") ||
		   strings.Contains(errStr, "refresh failed") ||
		   strings.Contains(errStr, "Error loading SSO Token") {
			s.ssoTokenExpired.Store(true)
		}
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list clusters: %v", err))
		return
	}

	// Check if log broadcaster captured any SSO errors
	if s.logBroadcaster.CheckForSSOError() {
		s.ssoTokenExpired.Store(true)
	} else {
		// If we successfully listed clusters and no SSO errors in logs, token is valid
		s.ssoTokenExpired.Store(false)
	}

	response := ListClustersResponse{
		Clusters: clusters,
		Total:    len(clusters),
	}

	s.writeJSON(w, http.StatusOK, response)
}

// handleCreateCluster handles POST requests to create a new cluster.
func (s *Server) handleCreateCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	var req CreateClusterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	// Validate required fields
	if req.Name == "" {
		s.writeError(w, http.StatusBadRequest, "Cluster name is required")
		return
	}
	if req.Nodes <= 0 {
		s.writeError(w, http.StatusBadRequest, "Number of nodes must be positive")
		return
	}

	// Create cluster using cluster manager
	err := s.clusterManager.CreateCluster(context.Background(), req)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create cluster: %v", err))
		return
	}

	response := CreateClusterResponse{
		Success: true,
		Message: fmt.Sprintf("Cluster %s created successfully", req.Name),
		Cluster: req.Name,
	}

	s.writeJSON(w, http.StatusCreated, response)
}

// handleDeleteClusters handles DELETE requests to remove clusters.
func (s *Server) handleDeleteClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	var req DeleteClusterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	if len(req.Clusters) == 0 {
		s.writeError(w, http.StatusBadRequest, "No clusters specified")
		return
	}

	// Delete clusters using cluster manager
	err := s.clusterManager.DeleteClusters(context.Background(), req.Clusters)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to delete clusters: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Successfully deleted %d cluster(s)", len(req.Clusters)),
	})
}

// handleClusterActions handles cluster-specific actions (extend, details, etc.).
func (s *Server) handleClusterActions(w http.ResponseWriter, r *http.Request) {
	// Extract cluster name from URL path: /api/clusters/{name}/{action}
	path := strings.TrimPrefix(r.URL.Path, "/api/clusters/")
	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		s.writeError(w, http.StatusBadRequest, "Invalid URL format")
		return
	}

	clusterName := parts[0]
	action := parts[1]

	switch action {
	case "extend":
		s.handleExtendCluster(w, r, clusterName)
	case "details":
		s.handleClusterDetails(w, r, clusterName)
	case "stage":
		s.handleStageCluster(w, r)
	case "stage-local":
		s.handleStageLocalCluster(w, r)
	case "start":
		s.handleStartCluster(w, r)
	case "stop":
		s.handleStopCluster(w, r)
	case "wipe":
		s.handleWipeCluster(w, r)
	case "info":
		s.handleGetClusterInfo(w, r)
	case "adminurl":
		s.handleGetAdminURL(w, r)
	case "status":
		s.handleGetClusterStatus(w, r)
	case "stage-workload":
		s.handleStageWorkload(w, r, clusterName)
	case "start-workload":
		s.handleStartWorkload(w, r, clusterName)
	case "stop-workload":
		s.handleStopWorkload(w, r, clusterName)
	default:
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("Unknown action: %s", action))
	}
}

// handleExtendCluster handles extending a cluster's lifetime.
func (s *Server) handleExtendCluster(w http.ResponseWriter, r *http.Request, clusterName string) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	var req ExtendClusterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	// Extend cluster using cluster manager
	err := s.clusterManager.ExtendCluster(context.Background(), clusterName, req.Lifetime)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to extend cluster: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Cluster %s extended by %s", clusterName, req.Lifetime),
	})
}

// handleClusterDetails returns detailed information about a cluster.
func (s *Server) handleClusterDetails(w http.ResponseWriter, r *http.Request, clusterName string) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// TODO: Implement cluster details retrieval
	// This would involve getting more detailed information about nodes, status, etc.
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"cluster": clusterName,
		"details": "Not yet implemented",
	})
}

// writeJSON writes a JSON response.
func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Printf("Error encoding JSON response: %v", err)
	}
}

// handleAWSSSOLogin handles refreshing AWS SSO credentials.
func (s *Server) handleAWSSSOLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	// Execute AWS SSO login
	output, err := s.clusterManager.RefreshAWSSSO(context.Background())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to refresh AWS SSO: %v", err))
		return
	}

	// Clear SSO token expired flag and recent logs on successful refresh
	s.ssoTokenExpired.Store(false)
	s.logBroadcaster.ClearRecentLogs()

	s.writeJSON(w, http.StatusOK, AWSSSOLoginResponse{
		Success: true,
		Message: "AWS SSO credentials refreshed successfully",
		Output:  output,
	})
}

// handleSSOStatus handles GET requests to check SSO token status.
func (s *Server) handleSSOStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Return the current SSO token status
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"tokenExpired": s.ssoTokenExpired.Load(),
	})
}

// handleStageCluster handles POST requests to stage cockroach binaries.
func (s *Server) handleStageCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	clusterName := extractClusterName(r.URL.Path)
	if clusterName == "" {
		s.writeError(w, http.StatusBadRequest, "Cluster name is required")
		return
	}

	// Broadcast command start
	s.BroadcastLog(fmt.Sprintf("$ roachprod stage %s\n", clusterName))

	err := s.clusterManager.StageCluster(context.Background(), clusterName)
	if err != nil {
		s.BroadcastLog(fmt.Sprintf("Error: %v\n", err))
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to stage cluster: %v", err))
		return
	}

	s.BroadcastLog(fmt.Sprintf("Successfully staged cockroach binaries on cluster %s\n", clusterName))
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Staged cockroach binaries on cluster %s", clusterName),
	})
}

// handleStageLocalCluster handles POST requests to stage local cockroach binary.
func (s *Server) handleStageLocalCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	clusterName := extractClusterName(r.URL.Path)
	if clusterName == "" {
		s.writeError(w, http.StatusBadRequest, "Cluster name is required")
		return
	}

	// Broadcast command start
	s.BroadcastLog(fmt.Sprintf("$ roachprod put %s ~/go/src/github.com/cockroachdb/cockroach/artifacts/cockroach\n", clusterName))

	err := s.clusterManager.StageLocalCluster(context.Background(), clusterName)
	if err != nil {
		s.BroadcastLog(fmt.Sprintf("Error: %v\n", err))
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to stage local binary: %v", err))
		return
	}

	s.BroadcastLog(fmt.Sprintf("Successfully staged local binary on cluster %s\n", clusterName))
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Staged local binary on cluster %s", clusterName),
	})
}

// handleStartCluster handles POST requests to start cockroach.
func (s *Server) handleStartCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	clusterName := extractClusterName(r.URL.Path)
	if clusterName == "" {
		s.writeError(w, http.StatusBadRequest, "Cluster name is required")
		return
	}

	// Broadcast command start
	s.BroadcastLog(fmt.Sprintf("$ roachprod start %s\n", clusterName))

	err := s.clusterManager.StartCluster(context.Background(), clusterName)
	if err != nil {
		s.BroadcastLog(fmt.Sprintf("Error: %v\n", err))
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to start cluster: %v", err))
		return
	}

	s.BroadcastLog(fmt.Sprintf("Successfully started cockroach on cluster %s\n", clusterName))
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Started cockroach on cluster %s", clusterName),
	})
}

// handleStopCluster handles POST requests to stop cockroach.
func (s *Server) handleStopCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	clusterName := extractClusterName(r.URL.Path)
	if clusterName == "" {
		s.writeError(w, http.StatusBadRequest, "Cluster name is required")
		return
	}

	// Parse request body for optional node specification
	var req struct {
		Node string `json:"node"` // Optional: specific node number or range (e.g., "3" or "1-3")
	}
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
			return
		}
	}

	// Broadcast command start
	targetSpec := clusterName
	if req.Node != "" {
		targetSpec = fmt.Sprintf("%s:%s", clusterName, req.Node)
	}
	s.BroadcastLog(fmt.Sprintf("$ roachprod stop %s\n", targetSpec))

	err := s.clusterManager.StopClusterNodes(context.Background(), clusterName, req.Node)
	if err != nil {
		s.BroadcastLog(fmt.Sprintf("Error: %v\n", err))
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to stop cluster: %v", err))
		return
	}

	successMsg := fmt.Sprintf("Successfully stopped cockroach on cluster %s", clusterName)
	if req.Node != "" {
		successMsg = fmt.Sprintf("Successfully stopped cockroach on node(s) %s of cluster %s", req.Node, clusterName)
	}
	s.BroadcastLog(successMsg + "\n")
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Stopped cockroach on cluster %s", clusterName),
	})
}

// handleWipeCluster handles POST requests to wipe cluster nodes.
func (s *Server) handleWipeCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	clusterName := extractClusterName(r.URL.Path)
	if clusterName == "" {
		s.writeError(w, http.StatusBadRequest, "Cluster name is required")
		return
	}

	// Broadcast command start
	s.BroadcastLog(fmt.Sprintf("$ roachprod wipe %s\n", clusterName))

	err := s.clusterManager.WipeCluster(context.Background(), clusterName)
	if err != nil {
		s.BroadcastLog(fmt.Sprintf("Error: %v\n", err))
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to wipe cluster: %v", err))
		return
	}

	s.BroadcastLog(fmt.Sprintf("Successfully wiped cluster %s\n", clusterName))
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Wiped cluster %s", clusterName),
	})
}

// handleGetClusterInfo handles GET requests to retrieve cluster details.
func (s *Server) handleGetClusterInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	clusterName := extractClusterName(r.URL.Path)
	if clusterName == "" {
		s.writeError(w, http.StatusBadRequest, "Cluster name is required")
		return
	}

	details, err := s.clusterManager.GetClusterInfo(context.Background(), clusterName)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get cluster info: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, details)
}

// handleGetAdminURL handles GET requests to retrieve the admin URL.
func (s *Server) handleGetAdminURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	clusterName := extractClusterName(r.URL.Path)
	if clusterName == "" {
		s.writeError(w, http.StatusBadRequest, "Cluster name is required")
		return
	}

	adminURL, err := s.clusterManager.GetAdminURL(context.Background(), clusterName)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get admin URL: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{
		"url": adminURL,
	})
}

// extractClusterName extracts the cluster name from URL paths like /api/clusters/{name}/action
func extractClusterName(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "api" && parts[1] == "clusters" {
		return parts[2]
	}
	return ""
}

// handleGetClusterStatus handles GET requests to retrieve cluster node status.
func (s *Server) handleGetClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	clusterName := extractClusterName(r.URL.Path)
	if clusterName == "" {
		s.writeError(w, http.StatusBadRequest, "Cluster name is required")
		return
	}

	statuses, err := s.clusterManager.GetClusterStatus(context.Background(), clusterName)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get cluster status: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"statuses": statuses,
	})
}

// writeError writes an error response.
func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, ErrorResponse{
		Error: message,
	})
}

// handleCloudProviderOptions handles GET requests to retrieve cloud provider options.
func (s *Server) handleCloudProviderOptions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Extract provider from URL path: /api/cloud-providers/{provider}/options
	path := strings.TrimPrefix(r.URL.Path, "/api/cloud-providers/")
	parts := strings.Split(strings.Trim(path, "/"), "/")

	if len(parts) == 0 || parts[0] == "" {
		s.writeError(w, http.StatusBadRequest, "Cloud provider not specified")
		return
	}

	provider := strings.ToLower(parts[0])

	var options CloudProviderOptions
	var err error

	switch provider {
	case "gce":
		options, err = s.getGCEOptions()
	case "aws":
		options, err = s.getAWSOptions()
	case "azure":
		options, err = s.getAzureOptions()
	default:
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("Unknown cloud provider: %s", provider))
		return
	}

	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get options for %s: %v", provider, err))
		return
	}

	s.writeJSON(w, http.StatusOK, options)
}

// getGCEOptions queries GCE for available zones and machine types using gcloud CLI.
func (s *Server) getGCEOptions() (CloudProviderOptions, error) {
	var options CloudProviderOptions

	// Get zones
	zonesCmd := exec.Command("gcloud", "compute", "zones", "list",
		"--format=json",
		"--project=cockroach-ephemeral")

	zonesOutput, err := zonesCmd.Output()
	if err != nil {
		return options, fmt.Errorf("failed to list GCE zones: %w", err)
	}

	var zones []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(zonesOutput, &zones); err != nil {
		return options, fmt.Errorf("failed to parse GCE zones: %w", err)
	}

	// Filter for UP zones only and extract names
	for _, zone := range zones {
		if zone.Status == "UP" {
			options.Zones = append(options.Zones, zone.Name)
		}
	}

	// Get machine types from a representative zone (us-east1-b)
	// We pick one zone since machine types are generally available across zones
	machineTypesCmd := exec.Command("gcloud", "compute", "machine-types", "list",
		"--zones=us-east1-b",
		"--format=json",
		"--project=cockroach-ephemeral")

	machineTypesOutput, err := machineTypesCmd.Output()
	if err != nil {
		return options, fmt.Errorf("failed to list GCE machine types: %w", err)
	}

	var machineTypes []struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		GuestCpus   int    `json:"guestCpus"`
		MemoryMb    int    `json:"memoryMb"`
	}
	if err := json.Unmarshal(machineTypesOutput, &machineTypes); err != nil {
		return options, fmt.Errorf("failed to parse GCE machine types: %w", err)
	}

	// Extract machine type names with specs
	for _, mt := range machineTypes {
		memoryGb := float64(mt.MemoryMb) / 1024.0
		description := fmt.Sprintf("%d vCPUs, %.1f GB RAM", mt.GuestCpus, memoryGb)
		options.MachineTypes = append(options.MachineTypes, MachineType{
			Name:        mt.Name,
			Description: description,
			VCPUs:       mt.GuestCpus,
			MemoryGB:    memoryGb,
		})
	}

	// Sort machine types by machine class (alphabetically), then by vCPUs within class
	sort.Slice(options.MachineTypes, func(i, j int) bool {
		// Extract machine class (e.g., "n2-standard" from "n2-standard-4")
		classI := getMachineClass(options.MachineTypes[i].Name)
		classJ := getMachineClass(options.MachineTypes[j].Name)

		if classI != classJ {
			return classI < classJ
		}
		// Within same class, sort by vCPUs
		return options.MachineTypes[i].VCPUs < options.MachineTypes[j].VCPUs
	})

	return options, nil
}

// getAWSOptions queries AWS for available zones and instance types using aws CLI.
func (s *Server) getAWSOptions() (CloudProviderOptions, error) {
	var options CloudProviderOptions

	// Get availability zones for common regions
	regions := []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1", "eu-west-2", "ap-southeast-1", "ap-southeast-2"}

	for _, region := range regions {
		zonesCmd := exec.Command("aws", "ec2", "describe-availability-zones",
			"--region", region,
			"--output=json")

		zonesOutput, err := zonesCmd.Output()
		if err != nil {
			// Skip regions where we don't have access
			continue
		}

		var zonesResp struct {
			AvailabilityZones []struct {
				ZoneName string `json:"ZoneName"`
				State    string `json:"State"`
			} `json:"AvailabilityZones"`
		}
		if err := json.Unmarshal(zonesOutput, &zonesResp); err != nil {
			continue
		}

		for _, zone := range zonesResp.AvailabilityZones {
			if zone.State == "available" {
				options.Zones = append(options.Zones, zone.ZoneName)
			}
		}
	}

	// Get instance types from us-east-1 (representative region)
	// Filter for common instance families that roachprod typically uses
	instanceTypesCmd := exec.Command("aws", "ec2", "describe-instance-types",
		"--region", "us-east-1",
		"--filters", "Name=instance-type,Values=m6i.*,m6id.*,c6i.*,r6i.*,m5.*,c5.*,r5.*",
		"--query", "InstanceTypes[*].[InstanceType,VCpuInfo.DefaultVCpus,MemoryInfo.SizeInMiB]",
		"--output=json")

	instanceTypesOutput, err := instanceTypesCmd.Output()
	if err != nil {
		return options, fmt.Errorf("failed to list AWS instance types: %w", err)
	}

	var instanceTypesRaw [][]interface{}
	if err := json.Unmarshal(instanceTypesOutput, &instanceTypesRaw); err != nil {
		return options, fmt.Errorf("failed to parse AWS instance types: %w", err)
	}

	// Extract instance type info
	for _, it := range instanceTypesRaw {
		if len(it) < 3 {
			continue
		}
		name, ok1 := it[0].(string)
		vCpus, ok2 := it[1].(float64)
		memoryMiB, ok3 := it[2].(float64)
		if !ok1 || !ok2 || !ok3 {
			continue
		}
		memoryGiB := memoryMiB / 1024.0
		description := fmt.Sprintf("%d vCPUs, %.1f GB RAM", int(vCpus), memoryGiB)
		options.MachineTypes = append(options.MachineTypes, MachineType{
			Name:        name,
			Description: description,
			VCPUs:       int(vCpus),
			MemoryGB:    memoryGiB,
		})
	}

	// Sort machine types by machine class (alphabetically), then by vCPUs within class
	sort.Slice(options.MachineTypes, func(i, j int) bool {
		// Extract machine class (e.g., "m6i" from "m6i.xlarge")
		classI := getMachineClass(options.MachineTypes[i].Name)
		classJ := getMachineClass(options.MachineTypes[j].Name)

		if classI != classJ {
			return classI < classJ
		}
		// Within same class, sort by vCPUs
		return options.MachineTypes[i].VCPUs < options.MachineTypes[j].VCPUs
	})

	return options, nil
}

// getAzureOptions queries Azure for available locations and VM sizes using az CLI.
func (s *Server) getAzureOptions() (CloudProviderOptions, error) {
	var options CloudProviderOptions

	// Get Azure locations
	locationsCmd := exec.Command("az", "account", "list-locations",
		"--output=json")

	locationsOutput, err := locationsCmd.Output()
	if err != nil {
		return options, fmt.Errorf("failed to list Azure locations: %w", err)
	}

	var locations []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(locationsOutput, &locations); err != nil {
		return options, fmt.Errorf("failed to parse Azure locations: %w", err)
	}

	for _, loc := range locations {
		options.Zones = append(options.Zones, loc.Name)
	}

	// Get VM sizes from eastus (representative location)
	vmSizesCmd := exec.Command("az", "vm", "list-sizes",
		"--location", "eastus",
		"--output=json")

	vmSizesOutput, err := vmSizesCmd.Output()
	if err != nil {
		return options, fmt.Errorf("failed to list Azure VM sizes: %w", err)
	}

	var vmSizes []struct {
		Name                 string `json:"name"`
		NumberOfCores        int    `json:"numberOfCores"`
		MemoryInMb           int    `json:"memoryInMb"`
		MaxDataDiskCount     int    `json:"maxDataDiskCount"`
		OsDiskSizeInMb       int    `json:"osDiskSizeInMb"`
		ResourceDiskSizeInMb int    `json:"resourceDiskSizeInMb"`
	}
	if err := json.Unmarshal(vmSizesOutput, &vmSizes); err != nil {
		return options, fmt.Errorf("failed to parse Azure VM sizes: %w", err)
	}

	// Filter for common VM families (D, E, F series) and add with specs
	for _, vm := range vmSizes {
		if strings.HasPrefix(vm.Name, "Standard_D") ||
			strings.HasPrefix(vm.Name, "Standard_E") ||
			strings.HasPrefix(vm.Name, "Standard_F") {
			memoryGb := float64(vm.MemoryInMb) / 1024.0
			description := fmt.Sprintf("%d vCPUs, %.1f GB RAM", vm.NumberOfCores, memoryGb)
			options.MachineTypes = append(options.MachineTypes, MachineType{
				Name:        vm.Name,
				Description: description,
				VCPUs:       vm.NumberOfCores,
				MemoryGB:    memoryGb,
			})
		}
	}

	// Sort machine types by machine class (alphabetically), then by vCPUs within class
	sort.Slice(options.MachineTypes, func(i, j int) bool {
		// Extract machine class (e.g., "Standard_D" from "Standard_D4s_v3")
		classI := getMachineClass(options.MachineTypes[i].Name)
		classJ := getMachineClass(options.MachineTypes[j].Name)

		if classI != classJ {
			return classI < classJ
		}
		// Within same class, sort by vCPUs
		return options.MachineTypes[i].VCPUs < options.MachineTypes[j].VCPUs
	})

	return options, nil
}

// handleCurrentUser handles GET requests to get the current user.
func (s *Server) handleCurrentUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	username, err := s.clusterManager.GetCurrentUser()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get current user: %v", err))
		return
	}

	response := map[string]string{
		"username": username,
	}

	s.writeJSON(w, http.StatusOK, response)
}

// handleStageWorkload handles staging the workload binary and setting up HAProxy.
func (s *Server) handleStageWorkload(w http.ResponseWriter, r *http.Request, clusterName string) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	err := s.clusterManager.StageWorkload(context.Background(), clusterName)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to stage workload: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Successfully staged workload on cluster %s", clusterName),
	})
}

// handleStartWorkload handles initializing and starting the TPCC workload.
func (s *Server) handleStartWorkload(w http.ResponseWriter, r *http.Request, clusterName string) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	err := s.clusterManager.StartWorkload(context.Background(), clusterName)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to start workload: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Successfully started workload on cluster %s", clusterName),
	})
}

// handleStopWorkload handles stopping the running workload.
func (s *Server) handleStopWorkload(w http.ResponseWriter, r *http.Request, clusterName string) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.clusterManager == nil {
		s.writeError(w, http.StatusInternalServerError, "Cluster manager not initialized")
		return
	}

	err := s.clusterManager.StopWorkload(context.Background(), clusterName)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to stop workload: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Successfully stopped workload on cluster %s", clusterName),
	})
}
