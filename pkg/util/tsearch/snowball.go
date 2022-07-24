// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tsearch

import (
	"github.com/blevesearch/snowball/english"
	"github.com/blevesearch/snowball/french"
	"github.com/blevesearch/snowball/norwegian"
	"github.com/blevesearch/snowball/russian"
	"github.com/blevesearch/snowball/spanish"
	"github.com/blevesearch/snowball/swedish"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

type stemmer func(word string, stemStopWords bool) string

func getStemmer(config string) (stemmer, error) {
	switch config {
	case "simple":
		return func(s string, _ bool) string { return s }, nil
	case "english":
		return english.Stem, nil
	case "french":
		return french.Stem, nil
	case "spanish":
		return spanish.Stem, nil
	case "norwegian":
		return norwegian.Stem, nil
	case "russian":
		return russian.Stem, nil
	case "swedish":
		return swedish.Stem, nil
	}
	return nil, pgerror.Newf(pgcode.UndefinedObject, "text search configuration %q does not exist", config)
}
