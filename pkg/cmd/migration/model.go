// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import "github.com/cockroachdb/cockroach/pkg/sql/parser"

type ImportMetadata struct {
	Statements []SingleStatement `json:"statements"`
	Database   string            `json:"database"`
	Status     string            `json:"status"`
	Message    string            `json:"message"`
}

type Issue struct {
	Level      string `json:"level"`
	Type       string `json:"type"`
	Identifier string `json:"id"`
	Text       string `json:"text"`
}

type SingleStatement struct {
	Original  string  `json:"original"`
	Cockroach string  `json:"cockroach"`
	Issues    []Issue `json:"issues"`
	parsed    *parser.Statement
}
