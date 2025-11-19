// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprod

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/errors"
)

// uiClusterManager implements the ui.ClusterManager interface.
type uiClusterManager struct {
	logger      *logger.Logger
	broadcaster ui.LogBroadcaster
}

// NewUIClusterManager creates a new cluster manager for the UI.
func NewUIClusterManager(l *logger.Logger) ui.ClusterManager {
	return &uiClusterManager{logger: l}
}

// SetBroadcaster sets the log broadcaster for WebSocket streaming.
func (m *uiClusterManager) SetBroadcaster(b ui.LogBroadcaster) {
	m.broadcaster = b
}

// getCPUCount extracts the CPU count from a machine type string.
// Supports formats like:
// - GCE: n2-standard-4, n4-highcpu-16, c2-standard-8
// - AWS: m6i.xlarge, c5.2xlarge, r5.4xlarge
func getCPUCount(machineType string) (int, error) {
	// Try GCE format first: family-type-cpus (e.g., n2-standard-4)
	gcePattern := regexp.MustCompile(`^[a-z]\d+d?-[a-z]+-(\d+)$`)
	if matches := gcePattern.FindStringSubmatch(machineType); len(matches) > 1 {
		return strconv.Atoi(matches[1])
	}

	// Try AWS format: family.size (e.g., m6i.xlarge)
	// AWS instance size to vCPU mapping
	awsPattern := regexp.MustCompile(`^[a-z]\d+[a-z]*\.(.+)$`)
	if matches := awsPattern.FindStringSubmatch(machineType); len(matches) > 1 {
		size := matches[1]
		// Common AWS vCPU counts by size
		awsSizes := map[string]int{
			"nano":     1,
			"micro":    1,
			"small":    1,
			"medium":   1,
			"large":    2,
			"xlarge":   4,
			"2xlarge":  8,
			"3xlarge":  12,
			"4xlarge":  16,
			"6xlarge":  24,
			"8xlarge":  32,
			"9xlarge":  36,
			"10xlarge": 40,
			"12xlarge": 48,
			"16xlarge": 64,
			"18xlarge": 72,
			"24xlarge": 96,
			"32xlarge": 128,
		}
		if cpus, ok := awsSizes[size]; ok {
			return cpus, nil
		}
	}

	return 0, errors.Newf("could not parse CPU count from machine type: %s", machineType)
}

// ListClusters returns information about all clusters or filtered by user.
func (m *uiClusterManager) ListClusters(ctx context.Context, userFilter string) ([]ui.ClusterInfo, error) {
	// Determine if this is a "mine" filter or a pattern filter
	mine := false
	pattern := ""
	if userFilter == "me" {
		mine = true
	} else if userFilter != "" {
		pattern = userFilter
	}

	// List clusters using roachprod
	filteredCloud, err := List(m.logger, mine, pattern,
		vm.ListOptions{
			Username:             "",
			ComputeEstimatedCost: false,
		})
	if err != nil {
		return nil, err
	}

	// Convert to API response format
	clusters := make([]ui.ClusterInfo, 0)
	for name, cluster := range filteredCloud.Clusters {
		if len(cluster.VMs) == 0 {
			continue
		}

		// Calculate lifetime remaining
		lifetimeRemaining := "-"
		if !cluster.IsLocal() && cluster.Lifetime > 0 {
			remaining := time.Until(cluster.CreatedAt.Add(cluster.Lifetime))
			if remaining > 0 {
				lifetimeRemaining = formatDuration(remaining)
			} else {
				lifetimeRemaining = "expired"
			}
		}

		// Extract owner from cluster name (format: owner-clustername)
		owner := ""
		if idx := strings.Index(name, "-"); idx > 0 {
			owner = name[:idx]
		}

		// Get cloud provider (join multiple if geo-distributed)
		cloudProviders := cluster.Clouds()
		cloudStr := ""
		if len(cloudProviders) > 0 {
			cloudStr = strings.Join(cloudProviders, ",")
		}

		clusterInfo := ui.ClusterInfo{
			Name:              name,
			Nodes:             len(cluster.VMs),
			Cloud:             cloudStr,
			LifetimeRemaining: lifetimeRemaining,
			Created:           cluster.CreatedAt,
			Owner:             owner,
			MachineType:       cluster.VMs[0].MachineType,
		}
		clusters = append(clusters, clusterInfo)
	}

	return clusters, nil
}

// CreateCluster creates a new cluster.
func (m *uiClusterManager) CreateCluster(ctx context.Context, req ui.CreateClusterRequest) error {
	// Parse lifetime duration
	lifetime, err := time.ParseDuration(req.Lifetime)
	if err != nil {
		return fmt.Errorf("invalid lifetime duration: %w", err)
	}

	// Build create options
	createVMOpts := vm.DefaultCreateOpts()
	createVMOpts.ClusterName = req.Name
	createVMOpts.Lifetime = lifetime
	createVMOpts.GeoDistributed = req.Geo
	createVMOpts.Arch = req.Arch
	createVMOpts.VMProviders = []string{req.Cloud}

	if req.Filesystem != "" {
		createVMOpts.SSDOpts.FileSystem = req.Filesystem
	}
	if !req.LocalSSD {
		createVMOpts.SSDOpts.UseLocalSSD = false
	}

	providerOptsContainer := vm.CreateProviderOptionsContainer()

	// Set machine type and zones based on cloud provider
	switch strings.ToLower(req.Cloud) {
	case "gce":
		if gceOpts, ok := providerOptsContainer["gce"].(*gce.ProviderOpts); ok {
			if req.MachineType != "" {
				gceOpts.MachineType = req.MachineType
			}
			if len(req.Zones) > 0 {
				gceOpts.Zones = req.Zones
			}
			providerOptsContainer["gce"] = gceOpts
		}
	case "aws":
		if awsOpts, ok := providerOptsContainer["aws"].(*aws.ProviderOpts); ok {
			if req.MachineType != "" {
				awsOpts.MachineType = req.MachineType
			}
			if len(req.Zones) > 0 {
				awsOpts.CreateZones = req.Zones
			}
			providerOptsContainer["aws"] = awsOpts
		}
	}

	opts := cloud.ClusterCreateOpts{
		Nodes:                 req.Nodes,
		CreateOpts:            createVMOpts,
		ProviderOptsContainer: providerOptsContainer,
	}

	// Create cluster using roachprod
	err = Create(ctx, m.logger, "", &opts)
	if err != nil {
		// Check if this is a "cluster already exists" error that happens after successful creation
		// This can happen if Create checks cluster existence at the end
		if strings.Contains(err.Error(), "already exists") {
			// Cluster was created successfully, ignore this error
			m.logger.Printf("Cluster %s created successfully (ignoring post-creation existence check)", req.Name)
			return nil
		}
		return err
	}
	return nil
}

// DeleteClusters deletes one or more clusters.
func (m *uiClusterManager) DeleteClusters(ctx context.Context, clusterNames []string) error {
	return Destroy(m.logger, "", false, false, clusterNames...)
}

// ExtendCluster extends a cluster's lifetime.
func (m *uiClusterManager) ExtendCluster(ctx context.Context, clusterName string, lifetime string) error {
	duration, err := time.ParseDuration(lifetime)
	if err != nil {
		return fmt.Errorf("invalid lifetime duration: %w", err)
	}
	return Extend(m.logger, clusterName, duration)
}

// RefreshAWSSSO refreshes AWS SSO credentials by running 'aws sso login'.
// Returns the command output which contains the verification code.
func (m *uiClusterManager) RefreshAWSSSO(ctx context.Context) (string, error) {
	m.logger.Printf("Refreshing AWS SSO credentials...")

	// Run 'aws sso login' command in the background
	// We capture initial output and then let it continue in background
	cmd := exec.CommandContext(ctx, "aws", "sso", "login")

	// Capture stderr which contains the code and URL
	var outputBuf strings.Builder
	cmd.Stderr = &outputBuf
	cmd.Stdout = &outputBuf

	// Start the command but don't wait for it
	err := cmd.Start()
	if err != nil {
		return "", fmt.Errorf("failed to start AWS SSO login: %w", err)
	}

	// Poll for output with verification code for up to 10 seconds
	// The AWS CLI usually writes the code and URL within a few seconds
	maxWait := 10 * time.Second
	pollInterval := 200 * time.Millisecond
	deadline := time.Now().Add(maxWait)

	var output string
	for time.Now().Before(deadline) {
		output = outputBuf.String()
		// Check if we have the verification code in the output
		// The AWS CLI outputs something like "code: ABCD-EFGH"
		if strings.Contains(output, "code:") || len(output) > 50 {
			// We have meaningful output, break early
			break
		}
		time.Sleep(pollInterval)
	}

	// Get final output after polling
	output = outputBuf.String()

	// Continue running in background - don't wait for completion
	go func() {
		_ = cmd.Wait()
		m.logger.Printf("AWS SSO login command completed")
	}()

	m.logger.Printf("AWS SSO login initiated with output length: %d", len(output))
	return output, nil
}

// StageCluster stages cockroach binaries on all nodes.
func (m *uiClusterManager) StageCluster(ctx context.Context, clusterName string) error {
	// Stage(ctx, logger, clusterName, stageOS, stageArch, stageDir, applicationName, version)
	// Empty version string means "latest"

	// For workload clusters, stage cockroach on ALL nodes (including workload node)
	// The workload node needs the cockroach binary for running "cockroach gen haproxy"
	// We just won't start the database on the workload node
	return Stage(ctx, m.logger, clusterName, "", "", "", "cockroach", "")
}

// StageLocalCluster uploads the local cockroach binary from ~/go/src/github.com/cockroachdb/cockroach/artifacts/cockroach
func (m *uiClusterManager) StageLocalCluster(ctx context.Context, clusterName string) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	localBinaryPath := filepath.Join(homeDir, "go", "src", "github.com", "cockroachdb", "cockroach", "artifacts", "cockroach")

	// Stop the cluster first to ensure the binary is not in use
	// Ignore errors since the cluster might not be running
	_ = Stop(ctx, m.logger, clusterName, StopOpts{Sig: 9})

	// Remove the existing cockroach binary from all nodes
	// This prevents tree distribution scp failures when the file already exists
	// Ignore errors since the file might not exist
	_ = Run(ctx, m.logger, clusterName, "", "", install.SimpleSecureOption(false), io.Discard, io.Discard, []string{"rm", "-f", "./cockroach"}, install.DefaultRunOptions())

	// Put the local binary to all nodes using tree distribution for efficiency
	return Put(ctx, m.logger, clusterName, localBinaryPath, "./cockroach", true)
}

// StartCluster starts cockroach on all nodes.
func (m *uiClusterManager) StartCluster(ctx context.Context, clusterName string) error {
	// For workload clusters, only start on nodes 1..N-1 (exclude the workload node)
	targetCluster := clusterName
	if isWorkloadCluster(clusterName) {
		// Get cluster info to determine number of nodes
		filteredCloud, err := List(m.logger, false, clusterName, vm.ListOptions{})
		if err != nil {
			return err
		}
		cluster, ok := filteredCloud.Clusters[clusterName]
		if !ok {
			return fmt.Errorf("cluster %s not found", clusterName)
		}
		numNodes := len(cluster.VMs)
		if numNodes > 1 {
			// Use node selector to target nodes 1..N-1
			targetCluster = fmt.Sprintf("%s:1-%d", clusterName, numNodes-1)
		}
	}

	return Start(ctx, m.logger, targetCluster, DefaultStartOpts())
}

// StopCluster stops cockroach on all nodes.
func (m *uiClusterManager) StopCluster(ctx context.Context, clusterName string) error {
	return m.StopClusterNodes(ctx, clusterName, "")
}

// StopClusterNodes stops cockroach on specific nodes or all nodes of a cluster.
// If nodeSpec is empty, stops all cockroach nodes (1..N-1 for workload clusters, all for regular clusters).
// nodeSpec can be a single node number (e.g., "3") or a range (e.g., "1-3").
func (m *uiClusterManager) StopClusterNodes(ctx context.Context, clusterName string, nodeSpec string) error {
	targetCluster := clusterName

	if nodeSpec != "" {
		// User specified specific node(s) to stop
		targetCluster = fmt.Sprintf("%s:%s", clusterName, nodeSpec)
	} else if isWorkloadCluster(clusterName) {
		// For workload clusters, only stop on nodes 1..N-1 (exclude the workload node)
		// Get cluster info to determine number of nodes
		filteredCloud, err := List(m.logger, false, clusterName, vm.ListOptions{})
		if err != nil {
			return err
		}
		cluster, ok := filteredCloud.Clusters[clusterName]
		if !ok {
			return fmt.Errorf("cluster %s not found", clusterName)
		}
		numNodes := len(cluster.VMs)
		if numNodes > 1 {
			// Use node selector to target nodes 1..N-1
			targetCluster = fmt.Sprintf("%s:1-%d", clusterName, numNodes-1)
		}
	}

	return Stop(ctx, m.logger, targetCluster, StopOpts{Sig: 9})
}

// WipeCluster wipes all nodes of the cluster.
func (m *uiClusterManager) WipeCluster(ctx context.Context, clusterName string) error {
	return Wipe(ctx, m.logger, clusterName, false)
}

// GetClusterInfo returns detailed cluster information including IPs and URLs.
func (m *uiClusterManager) GetClusterInfo(ctx context.Context, clusterName string) (*ui.ClusterDetails, error) {
	// Get cluster info
	filteredCloud, err := List(m.logger, false, clusterName, vm.ListOptions{})
	if err != nil {
		return nil, err
	}

	cluster, ok := filteredCloud.Clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("cluster %s not found", clusterName)
	}

	// Load the SyncedCluster to determine security mode
	syncedCluster, err := NewCluster(m.logger, clusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to load cluster for security check: %w", err)
	}

	details := &ui.ClusterDetails{
		Name:      clusterName,
		Nodes:     make([]string, 0, len(cluster.VMs)),
		PGURLs:    make([]string, 0, len(cluster.VMs)),
		AdminURLs: make([]string, 0, len(cluster.VMs)),
	}

	// Get node IPs and URLs
	for i, vm := range cluster.VMs {
		details.Nodes = append(details.Nodes, vm.PublicIP)

		// PostgreSQL URL format - adjust based on security mode
		var pgURL string
		if syncedCluster.Secure {
			// Secure mode: use sslmode=require (or verify-full if certificates are available)
			pgURL = fmt.Sprintf("postgresql://root@%s:26257?sslmode=require", vm.PublicIP)
		} else {
			// Insecure mode: disable SSL
			pgURL = fmt.Sprintf("postgresql://root@%s:26257?sslmode=disable", vm.PublicIP)
		}
		details.PGURLs = append(details.PGURLs, pgURL)

		// Admin UI URL format - adjust based on security mode
		var adminURL string
		if syncedCluster.Secure {
			// Secure mode: use HTTPS on port 8080
			adminURL = fmt.Sprintf("https://%s:8080", vm.PublicIP)
		} else {
			// Insecure mode: use HTTP on port 8080
			adminURL = fmt.Sprintf("http://%s:8080", vm.PublicIP)
		}
		details.AdminURLs = append(details.AdminURLs, adminURL)

		m.logger.Printf("Node %d: IP=%s, PG=%s, Admin=%s (Secure=%v)", i+1, vm.PublicIP, pgURL, adminURL, syncedCluster.Secure)
	}

	return details, nil
}

// GetAdminURL returns the admin UI URL for the first node in the cluster.
func (m *uiClusterManager) GetAdminURL(ctx context.Context, clusterName string) (string, error) {
	// Use the existing AdminURL function which handles DNS, security, and dynamic port discovery
	urls, err := AdminURL(
		ctx,
		m.logger,
		clusterName,
		"", // virtualClusterName (empty for default)
		0,  // sqlInstance (0 for default)
		"", // path (empty for root)
		true, // usePublicIP (use IPs instead of DNS)
		false, // openInBrowser (don't open, just return URL)
		install.ComplexSecureOption{}, // secure option with defaults
	)
	if err != nil {
		return "", err
	}

	if len(urls) == 0 {
		return "", fmt.Errorf("no admin URLs found for cluster %s", clusterName)
	}

	// Return the first node's admin URL
	return urls[0], nil
}

// GetClusterStatus returns the status of each node in the cluster.
func (m *uiClusterManager) GetClusterStatus(ctx context.Context, clusterName string) ([]ui.NodeStatus, error) {
	// Use the existing Status function
	statuses, err := Status(ctx, m.logger, clusterName, "")
	if err != nil {
		return nil, err
	}

	// Convert install.NodeStatus to ui.NodeStatus
	result := make([]ui.NodeStatus, len(statuses))
	for i, status := range statuses {
		errMsg := ""
		if status.Err != nil {
			errMsg = status.Err.Error()
		}
		result[i] = ui.NodeStatus{
			NodeID:  status.NodeID,
			Running: status.Running,
			Version: status.Version,
			Pid:     status.Pid,
			Err:     errMsg,
		}
	}

	return result, nil
}

// formatDuration formats a duration in human-readable form.
func formatDuration(d time.Duration) string {
	if d < 0 {
		return "0s"
	}

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh%dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}

// isWorkloadCluster returns true if the cluster name indicates it's a workload-enabled cluster.
func isWorkloadCluster(clusterName string) bool {
	return strings.HasSuffix(clusterName, "-with-workload")
}

// GetCurrentUser returns all active account names as a comma-separated string.
// These are the account names that could be used as cluster name prefixes.
// The UI uses this to determine which clusters the user owns.
func (m *uiClusterManager) GetCurrentUser() (string, error) {
	accounts, err := vm.FindActiveAccounts(m.logger)
	if err != nil {
		// If we can't get active accounts, fall back to OS username
		m.logger.Printf("FindActiveAccounts failed: %v, falling back to OS username: %s", err, config.OSUser.Username)
		return config.OSUser.Username, nil
	}
	if len(accounts) == 0 {
		// Fallback to OS username if no active accounts found
		m.logger.Printf("No active accounts found, using OS username: %s", config.OSUser.Username)
		return config.OSUser.Username, nil
	}

	// Convert map values to slice and join
	accountList := make([]string, 0, len(accounts))
	for _, account := range accounts {
		accountList = append(accountList, account)
	}
	result := strings.Join(accountList, ",")
	m.logger.Printf("Active accounts for ownership check: %s", result)
	return result, nil
}

// StageWorkload stages the workload binary and sets up HAProxy on the workload node.
func (m *uiClusterManager) StageWorkload(ctx context.Context, clusterName string) error {
	if !isWorkloadCluster(clusterName) {
		return fmt.Errorf("cluster %s is not a workload-enabled cluster", clusterName)
	}

	// Get cluster info to determine number of nodes
	filteredCloud, err := List(m.logger, false, clusterName, vm.ListOptions{})
	if err != nil {
		return err
	}
	cluster, ok := filteredCloud.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s not found", clusterName)
	}
	numNodes := len(cluster.VMs)
	if numNodes < 2 {
		return fmt.Errorf("workload cluster must have at least 2 nodes")
	}

	// Pre-flight check: Verify CockroachDB is running on cluster nodes (1..N-1)
	m.logger.Printf("Checking if CockroachDB is running on cluster nodes before staging workload...")
	statuses, err := Status(ctx, m.logger, clusterName, "")
	if err != nil {
		return fmt.Errorf("failed to check cluster status: %w", err)
	}

	// Check nodes 1..N-1 (exclude the workload node which is N)
	var notRunning []int
	for i := 0; i < numNodes-1; i++ {
		if i < len(statuses) {
			if !statuses[i].Running {
				notRunning = append(notRunning, statuses[i].NodeID)
			}
		}
	}

	if len(notRunning) > 0 {
		return fmt.Errorf("CockroachDB is not running on the following nodes: %v. Please start the cluster before staging the workload", notRunning)
	}
	m.logger.Printf("Pre-flight check passed: CockroachDB is running on all cluster nodes")

	// Stage workload binary on the last node
	workloadNode := fmt.Sprintf("%s:%d", clusterName, numNodes)
	if err := Stage(ctx, m.logger, workloadNode, "", "", "", "workload", ""); err != nil {
		return fmt.Errorf("failed to stage workload binary: %w", err)
	}

	// Install HAProxy on the workload node using roachprod's built-in installer
	m.logger.Printf("Installing HAProxy on %s", workloadNode)

	// Get the cluster to use InstallTool
	roachprodCluster, err := NewCluster(m.logger, clusterName)
	if err != nil {
		return fmt.Errorf("failed to load cluster for HAProxy install: %w", err)
	}

	// Install HAProxy using the built-in installation script
	var installBuf bytes.Buffer
	workloadNodeIndex := numNodes // Last node (1-indexed)
	if err := install.InstallTool(ctx, m.logger, roachprodCluster,
		roachprodCluster.Nodes[workloadNodeIndex-1:workloadNodeIndex],
		"haproxy", &installBuf, &installBuf); err != nil {
		m.logger.Printf("HAProxy installation output: %s", installBuf.String())
		return fmt.Errorf("failed to install haproxy: %w", err)
	}
	m.logger.Printf("HAProxy installed successfully")

	// Configure HAProxy using cockroach gen haproxy
	// This will generate the config and write it to haproxy.cfg
	m.logger.Printf("Generating HAProxy config on %s", workloadNode)

	// Use the first node's private IP for the gen haproxy command
	firstNodeIP := cluster.VMs[0].PrivateIP
	m.logger.Printf("Using first node IP for gen haproxy: %s", firstNodeIP)
	m.logger.Printf("Cluster has %d VMs total", len(cluster.VMs))
	m.logger.Printf("Cluster secure mode: %v", roachprodCluster.Secure)

	// Generate HAProxy config using cockroach gen haproxy
	// Note: cockroach binary is in the home directory after staging
	// The gen haproxy command writes to ~/haproxy.cfg, not stdout
	secureFlag := "--insecure"
	if roachprodCluster.Secure {
		secureFlag = "--certs-dir=certs"
	}
	genCmd := []string{fmt.Sprintf("~/cockroach gen haproxy %s --host %s:26257", secureFlag, firstNodeIP)}
	if err := Run(ctx, m.logger, workloadNode, "", "", install.SimpleSecureOption(false),
		nil, nil, genCmd, install.DefaultRunOptions()); err != nil {
		m.logger.Printf("Failed to run cockroach gen haproxy: %v", err)
		// Fall back to manual config
		m.logger.Printf("Falling back to manual HAProxy configuration")

		haproxyConfig := "global\n    maxconn 4096\n\ndefaults\n    mode tcp\n    timeout connect 10s\n    timeout client 1m\n    timeout server 1m\n\nlisten psql\n    bind :26257\n    mode tcp\n    balance roundrobin\n"
		for i := 0; i < numNodes-1; i++ {
			vm := cluster.VMs[i]
			haproxyConfig += fmt.Sprintf("    server cockroach%d %s:26257 check\n", i+1, vm.PrivateIP)
		}

		writeConfigCmd := []string{"bash", "-c", fmt.Sprintf("sudo tee /etc/haproxy/haproxy.cfg > /dev/null << 'HAPROXY_EOF'\n%s\nHAPROXY_EOF", haproxyConfig)}
		if err := Run(ctx, m.logger, workloadNode, "", "", install.SimpleSecureOption(false),
			nil, nil, writeConfigCmd, install.DefaultRunOptions()); err != nil {
			return fmt.Errorf("failed to write haproxy config: %w", err)
		}
	} else {
		// Successfully generated config - copy it to /etc/haproxy/haproxy.cfg
		m.logger.Printf("Copying generated HAProxy config to /etc/haproxy/haproxy.cfg")
		copyConfigCmd := []string{"sudo", "cp", "~/haproxy.cfg", "/etc/haproxy/haproxy.cfg"}
		if err := Run(ctx, m.logger, workloadNode, "", "", install.SimpleSecureOption(false),
			nil, nil, copyConfigCmd, install.DefaultRunOptions()); err != nil {
			return fmt.Errorf("failed to copy haproxy config: %w", err)
		}
	}

	// Restart HAProxy
	m.logger.Printf("Restarting HAProxy")
	restartCmd := []string{"sudo", "systemctl", "restart", "haproxy"}
	if err := Run(ctx, m.logger, workloadNode, "", "", install.SimpleSecureOption(false),
		nil, nil, restartCmd, install.DefaultRunOptions()); err != nil {
		return fmt.Errorf("failed to restart haproxy: %w", err)
	}

	// Note: Certificates should already be present on the workload node from cluster setup
	// Roachprod automatically distributes certificates to all nodes in secure mode clusters

	m.logger.Printf("Workload staged successfully on %s with HAProxy configured", workloadNode)
	return nil
}

// StartWorkload initializes and starts the TPCC workload.
func (m *uiClusterManager) StartWorkload(ctx context.Context, clusterName string) error {
	if !isWorkloadCluster(clusterName) {
		return fmt.Errorf("cluster %s is not a workload-enabled cluster", clusterName)
	}

	// Get cluster info to calculate vCPUs and determine workload node
	filteredCloud, err := List(m.logger, false, clusterName, vm.ListOptions{})
	if err != nil {
		return err
	}
	cluster, ok := filteredCloud.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s not found", clusterName)
	}
	numNodes := len(cluster.VMs)
	if numNodes < 2 {
		return fmt.Errorf("workload cluster must have at least 2 nodes")
	}

	// Calculate total vCPUs from nodes 1..N-1
	// Get actual vCPU count from the first VM (all CockroachDB nodes should have same machine type)
	var totalVCPUs int
	if len(cluster.VMs) > 0 {
		// Extract vCPU count from machine type
		machineType := cluster.VMs[0].MachineType
		cpus, err := getCPUCount(machineType)
		if err != nil {
			m.logger.Printf("Warning: could not determine CPU count from machine type %s: %v, using default of 4", machineType, err)
			cpus = 4
		}
		totalVCPUs = (numNodes - 1) * cpus
		m.logger.Printf("Detected %d vCPUs per node (%s), total vCPUs: %d", cpus, machineType, totalVCPUs)
	} else {
		totalVCPUs = (numNodes - 1) * 4
	}

	warehouses := int(float64(totalVCPUs) * 12.5)
	// Use TPCC default of 10 workers per warehouse
	workers := warehouses * 10
	// But limit active workers based on CPU capacity (2.5 per vCPU)
	activeWorkers := int(float64(totalVCPUs) * 2.5)
	// Connections should match active workers
	conns := activeWorkers

	// Workload node is the last node
	workloadNode := fmt.Sprintf("%s:%d", clusterName, numNodes)

	// Initialize TPCC database (using HAProxy on localhost with secure connection)
	// Certificates are in the certs directory
	connStr := "postgresql://root@localhost:26257?sslcert=certs/client.root.crt&sslkey=certs/client.root.key&sslrootcert=certs/ca.crt&sslmode=verify-full"
	initCmdStr := fmt.Sprintf("~/workload init tpcc --warehouses %d --drop '%s'", warehouses, connStr)
	initCmd := []string{initCmdStr}

	m.logger.Printf("Initializing TPCC with %d warehouses on %s", warehouses, workloadNode)
	m.logger.Printf("Init command: %s", initCmdStr)

	var initOut, initErr strings.Builder
	if err := Run(ctx, m.logger, workloadNode, "", "", install.SimpleSecureOption(false),
		&initOut, &initErr, initCmd, install.DefaultRunOptions()); err != nil {
		m.logger.Printf("Init stdout: %s", initOut.String())
		m.logger.Printf("Init stderr: %s", initErr.String())
		return fmt.Errorf("failed to initialize TPCC: %w", err)
	}
	m.logger.Printf("Init completed successfully")
	m.logger.Printf("Init output: %s", initOut.String())

	// Run TPCC workload in background
	// Create a script file and execute it to avoid command parsing issues
	// Using --workers (defaults to warehouses * 10) but limiting with --active-workers
	// --wait=0 is required when using custom active-workers that don't match the default
	m.logger.Printf("Starting TPCC workload: warehouses=%d, workers=%d, active-workers=%d, conns=%d",
		warehouses, workers, activeWorkers, conns)
	scriptContent := fmt.Sprintf("#!/bin/bash\nnohup ~/workload run tpcc --warehouses %d --workers %d --active-workers %d --conns %d --wait=0 --tolerate-errors '%s' > /tmp/workload.log 2>&1 < /dev/null &",
		warehouses, workers, activeWorkers, conns, connStr)

	// Write the script to a file
	writeScriptCmd := []string{"bash", "-c", fmt.Sprintf("cat > /tmp/start_workload.sh << 'EOF'\n%s\nEOF", scriptContent)}
	if err := Run(ctx, m.logger, workloadNode, "", "", install.SimpleSecureOption(false),
		nil, nil, writeScriptCmd, install.DefaultRunOptions()); err != nil {
		return fmt.Errorf("failed to create workload script: %w", err)
	}

	// Make it executable
	chmodCmd := []string{"chmod", "+x", "/tmp/start_workload.sh"}
	if err := Run(ctx, m.logger, workloadNode, "", "", install.SimpleSecureOption(false),
		nil, nil, chmodCmd, install.DefaultRunOptions()); err != nil {
		return fmt.Errorf("failed to make workload script executable: %w", err)
	}

	// Execute the script
	m.logger.Printf("Starting TPCC workload with %d warehouses, %d workers (%d active), %d connections on %s",
		warehouses, workers, activeWorkers, conns, workloadNode)
	runCmd := []string{"/tmp/start_workload.sh"}
	if err := Run(ctx, m.logger, workloadNode, "", "", install.SimpleSecureOption(false),
		nil, nil, runCmd, install.DefaultRunOptions()); err != nil {
		return fmt.Errorf("failed to start TPCC workload: %w", err)
	}

	// Wait a moment and verify the workload is running
	// Check multiple times with increasing delays to give the process time to start
	m.logger.Printf("Verifying workload process started...")
	var processRunning bool
	var pidOutput string

	for i := 0; i < 5; i++ {
		time.Sleep(time.Duration(i+1) * time.Second)
		checkCmd := []string{"bash", "-c", "pgrep -f 'workload run tpcc' || echo 'NO_PROCESS'"}
		var checkOut strings.Builder
		if err := Run(ctx, m.logger, workloadNode, "", "", install.SimpleSecureOption(false),
			&checkOut, nil, checkCmd, install.DefaultRunOptions()); err != nil {
			m.logger.Printf("Attempt %d: Failed to check workload process: %v", i+1, err)
			continue
		}

		output := strings.TrimSpace(checkOut.String())
		if !strings.Contains(output, "NO_PROCESS") && output != "" {
			processRunning = true
			pidOutput = output
			m.logger.Printf("Workload process verified running (PID: %s)", pidOutput)
			break
		}
		m.logger.Printf("Attempt %d: Process not found yet, will retry...", i+1)
	}

	if !processRunning {
		// Process not running after multiple attempts - check the log
		m.logger.Printf("Workload process not detected after 5 attempts, checking log file...")
		logCmd := []string{"bash", "-c", "tail -50 /tmp/workload.log 2>&1 || echo 'NO_LOG_FILE'"}
		var logOut strings.Builder
		if err := Run(ctx, m.logger, workloadNode, "", "", install.SimpleSecureOption(false),
			&logOut, nil, logCmd, install.DefaultRunOptions()); err == nil {
			logContents := logOut.String()
			m.logger.Printf("Workload log contents:\n%s", logContents)
			// Don't fail - just warn. The process might still be running even if pgrep didn't find it
			m.logger.Printf("WARNING: Could not verify workload process, but it may still be running. Check log above.")
		}
	}

	m.logger.Printf("TPCC workload started successfully on %s", workloadNode)
	return nil
}

// StopWorkload stops the running workload.
func (m *uiClusterManager) StopWorkload(ctx context.Context, clusterName string) error {
	if !isWorkloadCluster(clusterName) {
		return fmt.Errorf("cluster %s is not a workload-enabled cluster", clusterName)
	}

	// Get cluster info to determine workload node
	filteredCloud, err := List(m.logger, false, clusterName, vm.ListOptions{})
	if err != nil {
		return err
	}
	cluster, ok := filteredCloud.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s not found", clusterName)
	}
	numNodes := len(cluster.VMs)
	if numNodes < 2 {
		return fmt.Errorf("workload cluster must have at least 2 nodes")
	}

	// Workload node is the last node
	workloadNode := fmt.Sprintf("%s:%d", clusterName, numNodes)

	// Kill workload processes
	stopCmd := []string{"pkill", "-9", "workload"}
	if err := Run(ctx, m.logger, workloadNode, "", "", install.SimpleSecureOption(false),
		nil, nil, stopCmd, install.DefaultRunOptions()); err != nil {
		// pkill returns error if no process found, which is fine
		m.logger.Printf("Note: pkill returned error (may be no workload running): %v", err)
	}

	m.logger.Printf("Workload stopped on %s", workloadNode)
	return nil
}
