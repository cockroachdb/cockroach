// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package yamlutil

import "go.yaml.in/yaml/v4"

var marshalOpts = []yaml.Option{
	yaml.WithIndent(2),
	yaml.WithCompactSeqIndent(true),
	yaml.WithLineWidth(-1),
}

// Marshal encodes a value to YAML using 2 space indents, compact sequence
// indentation, and no line wrapping.
func Marshal(in interface{}) ([]byte, error) {
	return yaml.Dump(in, marshalOpts...)
}

var unmarshalStrictOpts = []yaml.Option{
	yaml.WithUniqueKeys(false),
	yaml.WithKnownFields(),
}

// UnmarshalStrict decodes a YAML document, returning an error if the input
// contains unknown fields.
func UnmarshalStrict(in []byte, out interface{}) error {
	return yaml.Load(in, out, unmarshalStrictOpts...)
}
