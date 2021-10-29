// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctestutils

import (
	"bytes"
	"context"
	gojson "encoding/json"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	jsonb "github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"gopkg.in/yaml.v2"
)

// WithBuilderDependenciesFromTestServer sets up and tears down an
// scbuild.Dependencies object built using the test server interface and which
// it passes to the callback.
func WithBuilderDependenciesFromTestServer(
	s serverutils.TestServerInterface, fn func(scbuild.Dependencies),
) {
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	ip, cleanup := sql.NewInternalPlanner(
		"test",
		kv.NewTxn(context.Background(), s.DB(), s.NodeID()),
		security.RootUserName(),
		&sql.MemoryMetrics{},
		&execCfg,
		// Setting the database on the session data to "defaultdb" in the obvious
		// way doesn't seem to do what we want.
		sessiondatapb.SessionData{},
	)
	defer cleanup()
	planner := ip.(interface {
		Txn() *kv.Txn
		Descriptors() *descs.Collection
		SessionData() *sessiondata.SessionData
		resolver.SchemaResolver
		scbuild.AuthorizationAccessor
	})
	fn(scdeps.NewBuilderDependencies(
		execCfg.Codec,
		planner.Txn(),
		planner.Descriptors(),
		planner,
		planner,
		planner.SessionData(),
		execCfg.Settings,
		nil, /* statements */
	))
}

// ProtoToYAML marshals a protobuf to YAML in a roundabout way.
func ProtoToYAML(m protoutil.Message) (string, error) {
	js, err := protoreflect.MessageToJSON(m, protoreflect.FmtFlags{})
	if err != nil {
		return "", err
	}
	str, err := jsonb.Pretty(js)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	buf.WriteString(str)
	target := make(map[string]interface{})
	err = gojson.Unmarshal(buf.Bytes(), &target)
	if err != nil {
		return "", err
	}
	out, err := yaml.Marshal(target)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
