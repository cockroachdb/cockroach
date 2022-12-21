// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	gojson "encoding/json"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// GenerateTestObjects synthetises objects quickly.
func (p *planner) GenerateTestObjects(ctx context.Context, parameters string) (string, error) {
	// Start with a default configuration.
	// Then, if the user requested a customization, apply it.
	cfg := randgen.DefaultTestSchemaGeneratorConfig()
	if parameters != "" {
		d := gojson.NewDecoder(strings.NewReader(parameters))
		d.DisallowUnknownFields()
		if err := d.Decode(&cfg); err != nil {
			return "", pgerror.WithCandidateCode(err, pgcode.Syntax)
		}
	}
	cfg.NameGen.Finalize()

	expandGlob := func(ctx context.Context, pattern tree.TablePattern) ([]descpb.ID, error) {
		_, objectIDs, err := expandTableGlob(ctx, p, pattern)
		return objectIDs, err
	}

	// The generator we'll use.
	g := randgen.NewTestSchemaGenerator(cfg,
		p.Txn(),
		p.HasAdminRole,
		p.canCreateDatabase,
		p.CheckPrivilegeForUser,
		p.Descriptors(),
		p.SessionData().User(),
		p.CurrentDatabase(),
		p.CurrentSearchPath(),
		p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		p.EvalContext().DescIDGenerator,
		expandGlob,
		rand.New(rand.NewSource(cfg.Seed)),
	)

	// Do it!
	finalCfg, err := g.Generate(ctx)
	if err != nil {
		return "", err
	}
	j, err := gojson.Marshal(finalCfg)
	if err != nil {
		return "", err
	}
	return string(j), nil
}
