// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	gojson "encoding/json"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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

	var seed int64
	if cfg.Seed != nil {
		seed = *cfg.Seed
	} else {
		seed = randutil.NewPseudoSeed()
	}
	rng := rand.New(rand.NewSource(seed))

	// Report the seed back to the user.
	cfg.Seed = &seed

	// The generator we'll use.
	g := randgen.NewTestSchemaGenerator(cfg,
		p.EvalContext().Settings,
		p.Txn(),
		p, /* catalog */
		p.Descriptors(),
		p.SessionData().User(),
		p.CurrentDatabase(),
		p.CurrentSearchPath(),
		p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		p.EvalContext().DescIDGenerator,
		p.Mon(),
		rng,
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
