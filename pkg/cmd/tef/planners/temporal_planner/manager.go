// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package temporal_planner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/tef/planners"
	"github.com/cockroachdb/logtags"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/uber-go/tally/v4"
	promreporter "github.com/uber-go/tally/v4/prometheus"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	sdktally "go.temporal.io/sdk/contrib/tally"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
)

// manager implements PlannerManager for Temporal-based plan execution.
// it manages Temporal client connections, worker lifecycle, and workflow execution.
type manager struct {
	basePlanner        *planners.BasePlanner
	temporalAddress    string
	temporalNamespace  string
	metricsListenAddr  string
	metricsBasePort    int // Base port for metrics (later workers use basePort+1, basePort+2, etc.)
	metricsEnabled     bool
	prometheusRegistry *prometheus.Registry
	metricsScope       tally.Scope
}

func (m *manager) newPrometheusScope() (tally.Scope, error) {
	if !m.metricsEnabled {
		return tally.NoopScope, nil
	}

	// Return a cached scope if already initialized
	if m.metricsScope != nil {
		return sdktally.NewPrometheusNamingScope(m.metricsScope), nil
	}

	// Create a Prometheus reporter with the configured registry
	if m.prometheusRegistry == nil {
		m.prometheusRegistry = prometheus.NewRegistry()
	}

	reporter := promreporter.NewReporter(promreporter.Options{
		Registerer: m.prometheusRegistry,
	})

	// Create Tally scope with Prometheus-compatible naming
	scopeOpts := tally.ScopeOptions{
		CachedReporter:  reporter,
		Separator:       promreporter.DefaultSeparator,
		SanitizeOptions: &sdktally.PrometheusSanitizeOptions,
		Prefix:          "tef_temporal",
	}

	scope, closer := tally.NewRootScope(scopeOpts, time.Second)
	m.metricsScope = scope

	// Store closer for cleanup (optional, but good practice)
	_ = closer

	// Wrap with Prometheus naming conventions for SDK compatibility
	return sdktally.NewPrometheusNamingScope(scope), nil
}

// dialTemporal creates a Temporal client with proper DNS resolution configuration.
// This is necessary for Kubernetes environments where service DNS names must be resolved
// using the Go DNS resolver instead of the default cgo resolver.
func (m *manager) dialTemporal(_ context.Context) (client.Client, error) {
	// Create a custom dialer that uses the Go DNS resolver.
	// This ensures proper DNS resolution in Kubernetes environments.
	// Force the use of Go's DNS resolver by setting a custom Resolver.
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Resolver: &net.Resolver{
			PreferGo: true, // Force pure Go resolver
		},
	}

	// Configure gRPC dial options with the custom dialer.
	// The Temporal SDK uses gRPC for communication, so we need to configure
	// the gRPC dialer to use our custom net.Dialer.
	dialOptions := grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, "tcp", addr)
	})

	// Configure a metrics handler if metrics are enabled
	var metricsHandler client.MetricsHandler
	if m.metricsEnabled {
		scope, err := m.newPrometheusScope()
		if err != nil {
			return nil, fmt.Errorf("failed to create metrics scope: %w", err)
		}
		metricsHandler = sdktally.NewMetricsHandler(scope)
	}

	clientOpts := client.Options{
		HostPort:          m.temporalAddress,
		Namespace:         m.temporalNamespace,
		ConnectionOptions: client.ConnectionOptions{DialOptions: []grpc.DialOption{dialOptions}},
	}

	// Add a metrics handler if configured
	if metricsHandler != nil {
		clientOpts.MetricsHandler = metricsHandler
	}

	return client.Dial(clientOpts)
}

// NewPlannerManager creates a new Temporal-based planner manager from a registry.
// The plan is validated during creation, and any validation errors are returned.
// The manager is automatically registered in the global managers registry for child task execution.
func NewPlannerManager(ctx context.Context, r planners.Registry) (planners.PlannerManager, error) {
	p, err := planners.NewBasePlanner(ctx, r)
	m := &manager{
		basePlanner: p,
	}
	if err == nil {
		// Automatically register the manager for child task execution
		planners.RegisterManager(r.GetPlanName(), m)
	}
	return m, err
}

// The manager type implements the PlannerManager interface.
var _ planners.PlannerManager = &manager{}

// StartWorker initializes and runs a Temporal worker for this plan.
// The worker listens on a plan-specific task queue and blocks until the context is canceled.
// If the connection to Temporal server is lost, the worker will automatically retry every 5 seconds.
func (m *manager) StartWorker(_ context.Context, _ string) error {
	// TODO: Implement worker start
	return errors.New("StartWorker not implemented yet")
}

// AddPlannerFlags registers planner-specific flags for Temporal configuration.
// These flags configure the Temporal server connection parameters.
func (m *manager) AddPlannerFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&m.temporalAddress, "temporal-address", "a", "localhost:7233", "Temporal server address")
	cmd.Flags().StringVarP(&m.temporalNamespace, "temporal-namespace", "n", "default", "Temporal namespace")
	cmd.Flags().BoolVar(&m.metricsEnabled, "metrics-enabled", true, "Enable Prometheus metrics collection")
	cmd.Flags().StringVar(&m.metricsListenAddr, "metrics-listen-addr", "0.0.0.0:9090", "Base address for Prometheus metrics HTTP endpoint (workers use incremental ports)")
}

// parseMetricsPort extracts the port number from a listen address string.
// Returns 9090 as default if parsing fails.
func parseMetricsPort(addr string) int {
	// Split on the last colon to handle IPv6 addresses
	idx := strings.LastIndex(addr, ":")
	if idx == -1 {
		return 9090
	}
	portStr := addr[idx+1:]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 9090
	}
	return port
}

// ClonePropertiesFrom copies planner-specific configuration properties from another manager.
// This ensures child plan managers inherit the same connection configuration.
// Note: Each worker gets its own Prometheus registry to avoid metric overwrites, but they
// share the base port number (each worker increments from the base).
func (m *manager) ClonePropertiesFrom(source planners.PlannerManager) {
	// Type asserts to get access to temporal-specific fields
	if srcManager, ok := source.(*manager); ok {
		m.temporalAddress = srcManager.temporalAddress
		m.temporalNamespace = srcManager.temporalNamespace
		m.metricsEnabled = srcManager.metricsEnabled
		m.metricsListenAddr = srcManager.metricsListenAddr
		m.metricsBasePort = parseMetricsPort(srcManager.metricsListenAddr)
		// Note: Do NOT copy prometheusRegistry or metricsScope - each worker gets its own
	}
}

// ExecutePlan starts a Temporal workflow execution for this plan with the given input.
// A unique workflow ID is generated and returned for tracking the execution.
// Returns an error if the task queue does not exist.
func (m *manager) ExecutePlan(_ context.Context, _ interface{}, _ string) (string, error) {
	// TODO: implement the execute workflow
	return "", errors.New("ExecutePlan not implemented yet")
}

func (m *manager) getActiveWorkersCount(
	ctx context.Context, c client.Client, planID string,
) (int, error) {
	resp, err := c.DescribeTaskQueue(ctx, planID, enums.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return 0, fmt.Errorf("failed to describe task queue %q: %w", planID, err)
	}
	return len(resp.Pollers), nil
}

// GetExecutionStatus queries the execution status of a workflow.
func (m *manager) GetExecutionStatus(
	ctx context.Context, planID, workflowID string,
) (*planners.ExecutionStatus, error) {
	return GetExecutionStatus(ctx, m.temporalAddress, m.temporalNamespace, planID, workflowID)
}

// ListExecutions lists workflow executions for this plan's task queue.
func (m *manager) ListExecutions(
	ctx context.Context, planID string,
) ([]*planners.WorkflowExecutionInfo, error) {
	// Add plan ID to context for automatic logging
	ctx = logtags.AddTag(ctx, "plan", planID)

	// A Temporal client is created to communicate with the Temporal server.
	c, err := m.dialTemporal(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	// Build a query to filter by task queue (plan ID)
	query := fmt.Sprintf("TaskQueue = '%s'", planID)

	// List workflow executions
	resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: query,
	})
	if err != nil {
		return nil, err
	}

	// Convert to our format
	executions := make([]*planners.WorkflowExecutionInfo, 0, len(resp.Executions))
	for _, exec := range resp.Executions {
		startTime := exec.GetStartTime()
		info := &planners.WorkflowExecutionInfo{
			WorkflowID: exec.Execution.WorkflowId,
			RunID:      exec.Execution.RunId,
			Status:     exec.Status.String(),
			StartTime:  startTime,
		}
		// Add EndTime if the workflow has closed
		if exec.GetCloseTime() != nil {
			endTime := exec.GetCloseTime()
			info.EndTime = endTime
		}
		executions = append(executions, info)
	}

	return executions, nil
}

// ListAllPlanIDs queries Temporal to find all active plan instances by examining task queues.
func (m *manager) ListAllPlanIDs(ctx context.Context) ([]planners.PlanMetadata, error) {
	// Connect to Temporal
	c, err := m.dialTemporal(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Temporal: %w", err)
	}
	defer c.Close()

	// List all workflows to extract unique task queues and their descriptions
	listResp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}

	// Extract unique plan_ids (task queues) and their descriptions from workflows
	planMetadataMap := make(map[string]string) // map[planID]description
	for _, exec := range listResp.Executions {
		if exec.TaskQueue != "" && strings.HasPrefix(exec.TaskQueue, planners.PlanIDPrefix) {
			// Only update if we haven't seen this plan ID before, or if we found a description
			if _, exists := planMetadataMap[exec.TaskQueue]; !exists {
				planMetadataMap[exec.TaskQueue] = ""
			}
			// Try to extract a description from the memo
			if exec.Memo != nil && exec.Memo.Fields != nil {
				// Try a new plan_structure format first
				if structField, ok := exec.Memo.Fields["plan_structure"]; ok {
					var planStructure planners.WorkflowInfo
					dc := converter.GetDefaultDataConverter()
					if err := dc.FromPayload(structField, &planStructure); err == nil && planStructure.Description != "" {
						planMetadataMap[exec.TaskQueue] = planStructure.Description
					}
				} else if descField, ok := exec.Memo.Fields["plan_description"]; ok {
					// Fallback to the old plan_description format for backwards compatibility
					var description string
					dc := converter.GetDefaultDataConverter()
					if err := dc.FromPayload(descField, &description); err == nil && description != "" {
						planMetadataMap[exec.TaskQueue] = description
					}
				}
			}
		}
	}

	// Convert to PlanMetadata slice
	plans := make([]planners.PlanMetadata, 0, len(planMetadataMap))
	for planID, description := range planMetadataMap {
		plans = append(plans, planners.PlanMetadata{
			PlanID:      planID,
			Description: description,
		})
	}

	return plans, nil
}

// ResumeTask signals a running workflow to resume an async task with the provided result.
// This is used to complete async tasks that are waiting for external events.
func (m *manager) ResumeTask(
	ctx context.Context, planID, workflowID, stepID string, input string,
) error {
	// Add plan ID to context for automatic logging
	ctx = logtags.AddTag(ctx, "plan", planID)

	// Connect to Temporal
	c, err := m.dialTemporal(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Temporal: %w", err)
	}
	defer c.Close()

	// Construct signal name based on step ID
	signalName := fmt.Sprintf("async-task-complete-%s", stepID)

	// Send signal to the workflow
	err = c.SignalWorkflow(ctx, workflowID, "", signalName, input)
	if err != nil {
		return fmt.Errorf("failed to signal workflow %s with step %s: %w", workflowID, stepID, err)
	}

	logger := planners.LoggerFromContext(ctx)
	logger.Info("Successfully signaled workflow to resume async task", "workflow_id", workflowID, "step_id", stepID)
	return nil
}
