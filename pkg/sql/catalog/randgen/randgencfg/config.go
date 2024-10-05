// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package randgencfg is defined separately from randgen so that
// client packages can avoid a direct dependency on randgen, and
// randgen can be rebuilt without rebuilding the entire sql package.
package randgencfg

import "github.com/cockroachdb/cockroach/pkg/util/randident/randidentcfg"

// ConfigDoc explains how to use the test schema generation
// configuration.
const ConfigDoc = `
- "names": pattern to use to name the generated objects (default:
  "test").
- "counts": counts of generated objects (default: [10]).
- "dry_run": prepare the schema but do not actually write it
  (default: false).
- "seed": random seed to use (default: auto).
- "randomize_columns": whether to randomize the column names on tables
  (default: true).
- "table_templates": table templates to use.
  If the last part of "names" is "_", the name of the template
  will be used as base pattern during name generation for tables.
  Otherwise, the last part of "names" will be used as pattern.
  If no table templates are specified, a simple template is used.
- "name_gen": configuration for the name generation, see below.

Name generation options:` + randidentcfg.ConfigDoc

// Config configures the test schema generator.
//
// The caller is responsible for calling the Finalize() method
// before using a config object.
type Config struct {
	// Names is the name pattern to use for the generated objects.
	Names string `json:"names"`

	// Counts defines how many objects to create.
	Counts []int `json:"counts"`

	// TableTemplates is the list of table templates to use.
	// If none are specified, a simple table template is used instead.
	TableTemplates []string `json:"table_templates,omitempty"`

	// BatchSize is the number of new object creations to accumulate in
	// a txn.Batch before the Batch gets executed.
	BatchSize int `json:"batch_size"`

	// Seed is the random seed to use.
	// nil (JSON null) means auto-select.
	Seed *int64 `json:"seed,omitempty"`

	// RandomizeColumns uses the name generation algorithm on column
	// names for generated tables.
	RandomizeColumns bool `json:"randomize_columns"`

	// NameGen is the name generation configuration.
	NameGen randidentcfg.Config `json:"name_gen"`

	// DryRun, if set, avoids actually writing the object descriptors.
	DryRun bool `json:"dry_run,omitempty"`

	// GeneratedCounts reports the number of objects generated.
	GeneratedCounts struct {
		Databases int `json:"databases"`
		Schemas   int `json:"schemas"`
		Tables    int `json:"tables"`
	} `json:"generated_counts"`
}

// Finalize populates all configuration fields after some of them have
// been customized.
func (cfg *Config) Finalize() {
	cfg.NameGen.Finalize()
}
