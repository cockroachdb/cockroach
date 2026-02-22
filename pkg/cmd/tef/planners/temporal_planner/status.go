// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package temporal_planner

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/tef/planners"
	"github.com/cockroachdb/logtags"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
)

// dialTemporalClient creates a Temporal client with proper DNS resolution configuration.
// This is necessary for Kubernetes environments where service DNS names must be resolved
// using the Go DNS resolver instead of the default cgo resolver.
func dialTemporalClient(_ context.Context, temporalAddr, namespace string) (client.Client, error) {
	// Create a custom dialer that uses the Go DNS resolver.
	// This ensures proper DNS resolution in Kubernetes environments.
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	// Configure gRPC dial options with the custom dialer.
	// The Temporal SDK uses gRPC for communication, so we need to configure
	// the gRPC dialer to use our custom net.Dialer.
	dialOptions := grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, "tcp", addr)
	})

	return client.Dial(client.Options{
		HostPort:          temporalAddr,
		Namespace:         namespace,
		ConnectionOptions: client.ConnectionOptions{DialOptions: []grpc.DialOption{dialOptions}},
	})
}

// GetExecutionStatus queries Temporal for the workflow execution status.
func GetExecutionStatus(
	ctx context.Context, temporalAddr, namespace, planID, workflowID string,
) (*planners.ExecutionStatus, error) {
	// Add plan ID to context for automatic logging
	ctx = logtags.AddTag(ctx, "plan", planID)

	// Create Temporal client with proper DNS configuration
	c, err := dialTemporalClient(ctx, temporalAddr, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to create Temporal client: %w", err)
	}
	defer c.Close()

	// Check workflow status using non-blocking describe
	describe, err := c.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to describe workflow: %w", err)
	}

	// Extract plan structure from the workflow memo
	planStructure, err := extractPlanStructureFromMemo(describe.WorkflowExecutionInfo.Memo)
	if err != nil {
		return nil, fmt.Errorf("failed to extract plan structure: %w", err)
	}

	// Determine status from workflow info
	var status string
	workflowStatus := describe.WorkflowExecutionInfo.GetStatus()
	switch workflowStatus {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		status = "Running"
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		status = "Completed"
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		status = "Failed"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		status = "Canceled"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		status = "Terminated"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		status = "TimedOut"
	default:
		status = workflowStatus.String()
	}
	// Extract task results from the workflow history
	taskResults, err := extractTaskResultsFromHistory(ctx, c, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to extract task results: %w", err)
	}

	// Extract parent workflow info from workflow input
	parentWorkflowURL, parentWorkflowUIURL, err := extractParentWorkflowURLs(ctx, c, workflowID)
	if err != nil {
		// Log but don't fail if we can't extract parent info
		logger := planners.LoggerFromContext(ctx)
		logger.Error("Could not extract parent workflow info", "error", err)
	}

	// Find the currently running task from task results
	var currentTasks []string
	if status == "Running" {
		for taskName, result := range taskResults {
			if result.Status == "InProgress" {
				currentTasks = append(currentTasks, taskName)
			}
		}
	}

	return &planners.ExecutionStatus{
		WorkflowID:          workflowID,
		Status:              status,
		CurrentTasks:        currentTasks,
		Workflow:            buildWorkflowInfo(planStructure, taskResults, workflowID),
		ParentWorkflowURL:   parentWorkflowURL,
		ParentWorkflowUIURL: parentWorkflowUIURL,
	}, nil
}

// buildWorkflowInfo constructs a WorkflowInfo by populating the plan structure with runtime data.
// The planStructure contains the static task definitions, and taskResults contain runtime execution data.
func buildWorkflowInfo(
	planStructure *planners.WorkflowInfo,
	taskResults map[string]planners.ActivityResult,
	workflowID string,
) *planners.WorkflowInfo {
	if planStructure == nil {
		return nil
	}

	// Create a copy of the plan structure to populate with runtime data
	workflowInfo := &planners.WorkflowInfo{
		Name:        planStructure.Name,
		Description: planStructure.Description,
		FirstTask:   planStructure.FirstTask,
		OutputTask:  planStructure.OutputTask,
		Tasks:       make(map[string]planners.TaskInfo),
	}

	// Populate workflow input from the first task's input
	if planStructure.FirstTask != "" {
		firstTaskName := planStructure.FirstTask
		// Check if the first task exists in the plan structure to determine its type
		if firstTask, ok := planStructure.Tasks[firstTaskName]; ok {
			// For async tasks, the input comes from the _execution activity
			if firstTask.Type == string(planners.TaskTypeCallbackTask) {
				if result, ok := taskResults[firstTaskName+"_execution"]; ok {
					workflowInfo.Input = result.Input
				}
			} else {
				if result, ok := taskResults[firstTaskName]; ok {
					workflowInfo.Input = result.Input
				}
			}
		}
	}

	// Populate runtime data for all tasks from the plan structure
	for taskName, taskInfo := range planStructure.Tasks {
		// Start with the static task definition from the plan structure
		populatedTaskInfo := taskInfo

		// Add execution results if available
		// For async tasks, results are split between _execution and _processor activities
		if populatedTaskInfo.Type == string(planners.TaskTypeCallbackTask) {
			// For async tasks, get start time from _execution and result from _processor
			if execResult, ok := taskResults[taskName+"_execution"]; ok {
				populatedTaskInfo.Input = execResult.Input
				populatedTaskInfo.StartTime = execResult.StartTime
				// If the processor hasn't run yet, set the status from execution
				if _, processorExists := taskResults[taskName+"_processor"]; !processorExists {
					populatedTaskInfo.Status = "InProgress"
				}
			}
			if procResult, ok := taskResults[taskName+"_processor"]; ok {
				populatedTaskInfo.Status = procResult.Status
				populatedTaskInfo.Output = procResult.Output
				populatedTaskInfo.EndTime = procResult.EndTime
				if procResult.Error != nil {
					populatedTaskInfo.Error = procResult.Error.Error()
				}
			}
		} else if populatedTaskInfo.Type == string(planners.TaskTypeChildPlanTask) {
			// For child workflow tasks, get start time from _preparation and result from _execution
			if execResult, ok := taskResults[taskName+"_preparation"]; ok {
				populatedTaskInfo.Input = execResult.Input
				populatedTaskInfo.StartTime = execResult.StartTime
				// If execution hasn't run yet, set status from preparation
				if _, executionExists := taskResults[taskName+"_execution"]; !executionExists {
					populatedTaskInfo.Status = "InProgress"
				}
				if execResult.Output != nil {
					// Convert the output (which is unmarshalled as map[string]interface{}) to ChildTaskInfo
					var childTaskInfo planners.ChildTaskInfo
					// Re-marshal and unmarshal to convert a map to struct
					if data, err := json.Marshal(execResult.Output); err == nil {
						if err := json.Unmarshal(data, &childTaskInfo); err == nil {
							if populatedTaskInfo.ChildPlanInfo == nil {
								populatedTaskInfo.ChildPlanInfo = &planners.ChildPlanInfo{}
							}
							populatedTaskInfo.ChildPlanInfo.PlanVariant = childTaskInfo.PlanVariant
						}
					}
				}
			}
			if procResult, ok := taskResults[taskName+"_execution"]; ok {
				populatedTaskInfo.Status = procResult.Status
				populatedTaskInfo.Output = procResult.Output
				populatedTaskInfo.EndTime = procResult.EndTime
				if procResult.Error != nil {
					populatedTaskInfo.Error = procResult.Error.Error()
				}
			}

			// Build the child workflow URLs if we have both plan name and variant
			// This makes child tasks clickable once the preparation activity has completed
			if populatedTaskInfo.ChildPlanInfo != nil && populatedTaskInfo.ChildPlanInfo.PlanName != "" && populatedTaskInfo.ChildPlanInfo.PlanVariant != "" {
				childPlanID := planners.GetPlanID(populatedTaskInfo.ChildPlanInfo.PlanName, populatedTaskInfo.ChildPlanInfo.PlanVariant)
				childWorkflowID := planners.GetChildWFID(workflowID, taskName, populatedTaskInfo.ChildPlanInfo.PlanName, populatedTaskInfo.ChildPlanInfo.PlanVariant)
				populatedTaskInfo.ChildPlanInfo.ChildWorkflowURL = fmt.Sprintf("/v1/plans/%s/executions/%s", url.PathEscape(childPlanID), url.PathEscape(childWorkflowID))
				populatedTaskInfo.ChildPlanInfo.ChildWorkflowUIURL = fmt.Sprintf("/?plan_id=%s&execution_id=%s", url.QueryEscape(childPlanID), url.QueryEscape(childWorkflowID))
			}
		} else {
			// For non-async tasks, use the standard lookup
			if result, ok := taskResults[taskName]; ok {
				populatedTaskInfo.Status = result.Status
				populatedTaskInfo.Input = result.Input
				populatedTaskInfo.Output = result.Output
				populatedTaskInfo.StartTime = result.StartTime
				populatedTaskInfo.EndTime = result.EndTime
				if result.Error != nil {
					populatedTaskInfo.Error = result.Error.Error()
				}
			}
		}

		workflowInfo.Tasks[taskName] = populatedTaskInfo
	}

	// Populate workflow output from the output task's result
	if planStructure.OutputTask != "" {
		outputTaskName := planStructure.OutputTask
		// Check if an output task exists in the plan structure to determine its type
		if outputTask, ok := planStructure.Tasks[outputTaskName]; ok {
			// For async tasks, the output comes from the _processor activity
			if outputTask.Type == string(planners.TaskTypeCallbackTask) {
				if result, ok := taskResults[outputTaskName+"_processor"]; ok {
					workflowInfo.Output = result.Output
				}
			} else {
				if result, ok := taskResults[outputTaskName]; ok {
					workflowInfo.Output = result.Output
				}
			}
		}
	}

	return workflowInfo
}

// extractParentWorkflowURLs extracts both API and UI URLs for the parent workflow from the workflow execution input.
// The first input parameter to the workflow is PlanExecutionInfo, which contains ParentInfo.
// Returns (apiURL, uiURL, error).
func extractParentWorkflowURLs(
	ctx context.Context, c client.Client, workflowID string,
) (string, string, error) {
	iter := c.GetWorkflowHistory(ctx, workflowID, "", false, 0)

	// Look for the workflow-started event which contains the input
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return "", "", fmt.Errorf("failed to get workflow history: %w", err)
		}

		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			if attrs := event.GetWorkflowExecutionStartedEventAttributes(); attrs != nil {
				payloads := attrs.GetInput().GetPayloads()
				if len(payloads) > 0 {
					// The first payload is PlanExecutionInfo
					var planExecutionInfo planners.PlanExecutionInfo
					if err := json.Unmarshal(payloads[0].GetData(), &planExecutionInfo); err == nil {
						// Check if there's parent info
						if planExecutionInfo.ParentInfo != nil {
							// Build parent workflow URLs (both API endpoint and UI page)
							parentPlanID := planExecutionInfo.ParentInfo.PlanID
							parentWorkflowID := planExecutionInfo.ParentInfo.WorkflowID
							apiURL := fmt.Sprintf("/v1/plans/%s/executions/%s", url.PathEscape(parentPlanID), url.PathEscape(parentWorkflowID))
							uiURL := fmt.Sprintf("/?plan_id=%s&execution_id=%s", url.QueryEscape(parentPlanID), url.QueryEscape(parentWorkflowID))
							return apiURL, uiURL, nil
						}
					}
				}
			}
			// Workflow started event found no need to continue
			break
		}
	}

	return "", "", nil
}

// extractPlanStructureFromMemo extracts the plan structure from the workflow memo.
// The plan structure is stored during workflow execution and contains a frozen
// snapshot of the plan definition, independent of any shared state.
func extractPlanStructureFromMemo(memo *commonpb.Memo) (*planners.WorkflowInfo, error) {
	if memo == nil || memo.Fields == nil {
		return nil, fmt.Errorf("workflow memo is empty")
	}

	structField, ok := memo.Fields["plan_structure"]
	if !ok {
		return nil, fmt.Errorf("plan_structure not found in workflow memo")
	}

	var planStructure planners.WorkflowInfo
	dc := converter.GetDefaultDataConverter()
	if err := dc.FromPayload(structField, &planStructure); err != nil {
		return nil, fmt.Errorf("failed to decode plan structure: %w", err)
	}

	return &planStructure, nil
}

// extractTaskNameFromActivityName extracts the result key from an activity name.
// Activity names follow these formats:
//   - Regular tasks: "task_{taskName}" → returns "{taskName}"
//   - Async tasks: "task_{taskName}_execution" → returns "{taskName}_execution"
//   - Async tasks: "task_{taskName}_processor" → returns "{taskName}_processor"
//
// The returned value is used as the key in the taskResults map.
func extractTaskNameFromActivityName(activityName string) string {
	const prefix = "task_"
	if len(activityName) > len(prefix) && activityName[:len(prefix)] == prefix {
		return activityName[len(prefix):]
	}
	// Fallback: return the activity name as-is if it doesn't match the expected format
	return activityName
}

// extractTaskResultsFromHistory extracts task execution results from Temporal history.
func extractTaskResultsFromHistory(
	ctx context.Context, c client.Client, workflowID string,
) (map[string]planners.ActivityResult, error) {
	results := make(map[string]planners.ActivityResult)
	iter := c.GetWorkflowHistory(ctx, workflowID, "", false, 0)

	// With the new task-specific activity naming scheme (task_{taskName}),
	// we can directly extract the task name from the activity name.
	// This solves the problem where multiple tasks could use the same executor function.

	// Map of scheduled event ID to task name
	scheduledTasks := make(map[int64]string)

	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to get workflow history: %w", err)
		}

		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
				activityName := attrs.GetActivityType().GetName()
				// Extract the task name from the activity name (format: "task_{taskName}")
				taskName := extractTaskNameFromActivityName(activityName)
				scheduledTasks[event.GetEventId()] = taskName

				// Parse all input payloads if available
				var inputs []interface{}
				payloads := attrs.GetInput().GetPayloads()
				if len(payloads) > 0 {
					for _, payload := range payloads {
						var input interface{}
						if err := json.Unmarshal(payload.GetData(), &input); err == nil {
							inputs = append(inputs, input)
						}
					}
					if len(inputs) > 0 {
						result := results[taskName]
						result.Input = inputs
						result.Status = "InProgress"
						if event.GetEventTime() != nil {
							startTime := event.GetEventTime()
							result.StartTime = startTime
						}
						results[taskName] = result
					}
				}
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			if attrs := event.GetActivityTaskStartedEventAttributes(); attrs != nil {
				scheduledEventID := attrs.GetScheduledEventId()
				if taskName, ok := scheduledTasks[scheduledEventID]; ok {
					result := results[taskName]
					result.Status = "InProgress"
					// Set the start time from the event timestamp
					if event.GetEventTime() != nil {
						startTime := event.GetEventTime()
						result.StartTime = startTime
					}
					results[taskName] = result
				}
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil {
				scheduledEventID := attrs.GetScheduledEventId()
				if taskName, ok := scheduledTasks[scheduledEventID]; ok {
					result := results[taskName]
					result.Status = "Completed"

					// Set end time from event timestamp
					if event.GetEventTime() != nil {
						endTime := event.GetEventTime()
						result.EndTime = endTime
					}

					// Parse output if available
					var output interface{}
					if attrs.GetResult() != nil && len(attrs.GetResult().GetPayloads()) > 0 {
						payload := attrs.GetResult().GetPayloads()[0]
						if err := json.Unmarshal(payload.GetData(), &output); err == nil {
							result.Output = output
						}
					}
					results[taskName] = result
				}
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			if attrs := event.GetActivityTaskFailedEventAttributes(); attrs != nil {
				scheduledEventID := attrs.GetScheduledEventId()
				if taskName, ok := scheduledTasks[scheduledEventID]; ok {
					result := results[taskName]
					result.Status = "Failed"
					result.Error = fmt.Errorf("%s", attrs.GetFailure().GetMessage())

					// Set end time from event timestamp
					if event.GetEventTime() != nil {
						endTime := event.GetEventTime()
						result.EndTime = endTime
					}

					results[taskName] = result
				}
			}
		}
	}

	return results, nil
}
