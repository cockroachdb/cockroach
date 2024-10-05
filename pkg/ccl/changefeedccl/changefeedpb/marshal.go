// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedpb

import (
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
)

// MarshalJSONPB provides a custom Marshaller for jsonpb that redacts secrets in
// URI fields.
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

	m.ChangefeedStatement = tree.AsStringWithFlags(export, tree.FmtShowFullURIs)
	return json.Marshal(m)
}
