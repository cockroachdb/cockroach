// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package profilerconstants

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/errors"
)

const dspDiagramInfoKeyPrefix = "dsp-diag-url-"

type dspDiagramInfoKey struct{}

// MakeDSPDiagramInfoKey returns the info_key used for rows that store the
// DistSQL plan diagram for a job.
func MakeDSPDiagramInfoKey() jobs.InfoKey {
	return &dspDiagramInfoKey{}
}

// MakeKey implements the InfoKey interface.
func (d *dspDiagramInfoKey) MakeKey(parts ...any) (string, error) {
	if len(parts) != 1 {
		return "", errors.AssertionFailedf("expected 1 part got %d", len(parts))
	}
	return fmt.Sprintf("%s%d", dspDiagramInfoKeyPrefix, parts[0]), nil
}

// KeyPrefix implements the InfoKey interface.
func (d *dspDiagramInfoKey) KeyPrefix() string {
	return dspDiagramInfoKeyPrefix
}

// GetParts implements the InfoKey interface.
func (d *dspDiagramInfoKey) GetParts(infoKey string) []string {
	return []string{strings.TrimPrefix(infoKey, dspDiagramInfoKeyPrefix)}
}

var _ jobs.InfoKey = &dspDiagramInfoKey{}

// nodeProcessorProgressInfoKeyPrefix is the prefix of the info key used for
// rows that store the per node, per processor progress for a job.
const nodeProcessorProgressInfoKeyPrefix = "node-processor-progress-"

type nodeProcessorProgressInfoKey struct{}

// MakeNodeProcessorProgressInfoKey returns the info_key used for rows that
// store the per node, per processor progress for a job.
func MakeNodeProcessorProgressInfoKey() jobs.InfoKey {
	return &nodeProcessorProgressInfoKey{}
}

// MakeKey implements the InfoKey interface.
func (d *nodeProcessorProgressInfoKey) MakeKey(parts ...any) (string, error) {
	if len(parts) != 3 {
		return "", errors.AssertionFailedf("expected 3 parts got %d", len(parts))
	}
	// The info key is of the form: <prefix>-<flowID>,<instanceID>,<processorID>.
	return fmt.Sprintf("%s%s,%s,%d",
		nodeProcessorProgressInfoKeyPrefix, parts[0], parts[1], parts[2]), nil
}

// KeyPrefix implements the InfoKey interface.
func (d *nodeProcessorProgressInfoKey) KeyPrefix() string {
	return nodeProcessorProgressInfoKeyPrefix
}

// GetParts implements the InfoKey interface.
func (d *nodeProcessorProgressInfoKey) GetParts(infoKey string) []string {
	return strings.Split(strings.TrimPrefix(infoKey, nodeProcessorProgressInfoKeyPrefix), ",")
}

var _ jobs.InfoKey = &nodeProcessorProgressInfoKey{}
