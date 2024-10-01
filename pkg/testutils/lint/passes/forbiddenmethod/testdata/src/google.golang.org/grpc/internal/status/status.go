// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package status

type Status struct{}

func (s *Status) WithDetails() (*Status, error) {
	return s, nil
}
