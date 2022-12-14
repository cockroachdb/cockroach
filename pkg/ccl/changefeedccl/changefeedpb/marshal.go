// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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

	m.ChangefeedStatement = export.String()
	return json.Marshal(m)
}
