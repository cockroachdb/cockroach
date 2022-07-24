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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// ValidConfig returns an error if the input string is not a supported and valid
// text search config.
func ValidConfig(input string) error {
	input = GetConfigKey(input)
	switch input {
	case "english", "french", "spanish", "norwegian", "simple", "russian", "swedish":
		return nil
	}
	return pgerror.Newf(pgcode.UndefinedObject, "text search configuration %q does not exist", input)
}

// GetConfigKey returns a config that can be used as a key to look up stemmers
// and stopwords from an input config value. This is simulating the more
// advanced customizable dictionaries and configs that Postgres has, which
// allows user-defined text search configurations: because of this, configs can
// have schema prefixes. Because we don't (yet?) allow this, we just have to
// trim off any `pg_catalog.` prefix if it exists.
func GetConfigKey(config string) string {
	return strings.TrimPrefix(config, "pg_catalog.")
}
