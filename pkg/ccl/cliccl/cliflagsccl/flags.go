// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliflagsccl

import "github.com/cockroachdb/cockroach/pkg/cli/cliflags"

// Attrs and others store the static information for CLI flags.
var (
	EnterpriseEncryption = cliflags.FlagInfo{
		Name: "enterprise-encryption",
		Description: `
*** Valid enterprise licenses only ***

TODO(mberhault): fill this.

Valid fields:
* path: must match the path of one of the stores
* key: path to the current key file
* old-key: path to the previous key file
* rotation-period: amount of time after which data keys should be rotated
`,
	}

	CSVTable = cliflags.FlagInfo{
		Name:        "table",
		Description: `location of a file containing a single CREATE TABLE statement`,
	}

	CSVDataNames = cliflags.FlagInfo{
		Name:        "data",
		Description: `filenames of CSV data; uses <table>.dat if empty`,
	}

	CSVDest = cliflags.FlagInfo{
		Name:        "dest",
		Description: `destination directory for backup files`,
	}

	CSVNullIf = cliflags.FlagInfo{
		Name:        "nullif",
		Description: `if specified, the value of NULL; can specify the empty string`,
	}

	CSVComma = cliflags.FlagInfo{
		Name:        "delimited",
		Description: `if specified, the CSV delimiter instead of a comma`,
	}

	CSVComment = cliflags.FlagInfo{
		Name:        "comment",
		Description: `if specified, allows comment lines starting with this character`,
	}

	CSVTempDir = cliflags.FlagInfo{
		Name:        "tempdir",
		Description: `directory to store intermediate temp files`,
	}
)
