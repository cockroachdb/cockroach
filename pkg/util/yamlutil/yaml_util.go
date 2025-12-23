// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package yamlutil

import "go.yaml.in/yaml/v4"

// Marshal is like yaml.v4.Marshal but indents to 2 spaces.
func Marshal(in interface{}) ([]byte, error) {
	var buf bytes.Buffer
	e := yaml.NewEncoder(&buf)
	e.SetIndent(2)
	if err := e.Encode(in); err != nil {
		return nil, err
	}
	if err := e.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalStrict is like yaml.v4.Unmarshal but fails if unknown fields are
// encountered.
func UnmarshalStrict(in []byte, out interface{}) error {
	return yaml.Load(in, out, unmarshalStrictOpts...)
}
