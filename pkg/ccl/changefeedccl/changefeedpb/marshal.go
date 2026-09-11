// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedpb

import (
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
)

// MarshalJSONPB provides a custom Marshaller for jsonpb that redacts secrets
// in the sink URI and WITH options of the changefeed statement.
func (m ScheduledChangefeedExecutionArgs) MarshalJSONPB(x *jsonpb.Marshaler) ([]byte, error) {
	stmt, err := parser.ParseOne(m.ChangefeedStatement)
	if err != nil {
		return nil, err
	}
	export, ok := stmt.AST.(*tree.CreateChangefeed)
	if !ok {
		return nil, errors.Errorf("unexpected %T statement in backup schedule: %v", export, export)
	}

	rawURI, ok := export.SinkURI.(*tree.StrVal)
	if !ok {
		return nil, errors.Errorf("unexpected %T arg in export schedule: %v", rawURI, rawURI)
	}
	sinkURI, err := cloud.SanitizeExternalStorageURI(rawURI.RawString(), nil /* extraParams */)
	if err != nil {
		return nil, err
	}
	export.SinkURI = tree.NewDString(sinkURI)

	for i, opt := range export.Options {
		redactionFunc, ok := changefeedbase.RedactedOptions[string(opt.Key)]
		if !ok {
			continue
		}
		rawValue, ok := opt.Value.(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T value for option %s", opt.Value, opt.Key)
		}
		redacted, err := redactionFunc(rawValue.RawString())
		if err != nil {
			return nil, err
		}
		export.Options[i].Value = tree.NewDString(redacted)
	}

	m.ChangefeedStatement = tree.AsStringWithFlags(export, tree.FmtShowFullURIs)
	return json.Marshal(m)
}
