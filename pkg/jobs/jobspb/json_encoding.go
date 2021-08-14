// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobspb

import (
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// ExecutionLogToJSON converts an executionLog in a job payload in a JSON object.
// It internally uses protoreflect.MessageToJSON to individually convert an
// ExecutionEvent in executionLog. Default values are omitted from the resulting
// JSON object.
func ExecutionLogToJSON(executionLog []*ExecutionEvent) (json.JSON, error) {
	b := json.NewArrayBuilder(len(executionLog))
	for _, event := range executionLog {
		eventJSON, err := protoreflect.MessageToJSON(event, false /* emitDefaults */)
		if err != nil {
			return nil, err
		}
		b.Add(eventJSON)
	}
	return b.Build(), nil
}
