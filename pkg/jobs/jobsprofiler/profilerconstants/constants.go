// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profilerconstants

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const DSPDiagramInfoKeyPrefix = "~dsp-diag-url-"

// DSPDiagramInfoKeyMax sorts after any diagram info key, because `:“ > [0-9].
const DSPDiagramInfoKeyMax = DSPDiagramInfoKeyPrefix + ":"

// MakeDSPDiagramInfoKey constructs an ephemeral DSP diagram info key.
func MakeDSPDiagramInfoKey(timestampInNanos int64) string {
	return fmt.Sprintf("%s%d", DSPDiagramInfoKeyPrefix, timestampInNanos)
}

// NodeProcessorProgressInfoKeyPrefix is the prefix of the info key used for
// rows that store the per node, per processor progress for a job.
const NodeProcessorProgressInfoKeyPrefix = "~node-processor-progress-"

// MakeNodeProcessorProgressInfoKey returns the info_key used for rows that
// store the per node, per processor progress for a job.
func MakeNodeProcessorProgressInfoKey(flowID string, instanceID string, processorID int32) string {
	// The info key is of the form: <prefix>-<flowID>,<instanceID>,<processorID>.
	return fmt.Sprintf("%s%s,%s,%d", NodeProcessorProgressInfoKeyPrefix, flowID, instanceID, processorID)
}

// ExecutionDetailsChunkKeyPrefix is the prefix of the info key used for rows that
// store chunks of a job's execution details.
const ExecutionDetailsChunkKeyPrefix = "~profiler/"

// MakeProfilerExecutionDetailsChunkKeyPrefix is the prefix of the info key used
// to store all chunks of a job's execution details.
func MakeProfilerExecutionDetailsChunkKeyPrefix(filename string) string {
	return fmt.Sprintf("%s%s", ExecutionDetailsChunkKeyPrefix, filename)
}

// GetNodeProcessorProgressInfoKeyParts deconstructs the passed in info key and
// returns the referenced flowID, instanceID and processorID.
func GetNodeProcessorProgressInfoKeyParts(infoKey string) (uuid.UUID, int, int, error) {
	parts := strings.Split(strings.TrimPrefix(infoKey, NodeProcessorProgressInfoKeyPrefix), ",")
	if len(parts) != 3 {
		return uuid.Nil, 0, 0, errors.AssertionFailedf("expected 3 parts in info key but found %d: %v", len(parts), parts)
	}
	flowID, err := uuid.FromString(parts[0])
	if err != nil {
		return uuid.Nil, 0, 0, err
	}
	instanceID, err := strconv.Atoi(parts[1])
	if err != nil {
		return uuid.Nil, 0, 0, err
	}
	processorID, err := strconv.Atoi(parts[2])
	if err != nil {
		return uuid.Nil, 0, 0, err
	}

	return flowID, instanceID, processorID, nil
}
