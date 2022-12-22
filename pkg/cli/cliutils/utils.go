// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cliutils

import (
	"encoding/csv"
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// ElideInsecureDeprecationNotice elides the deprecation notice for --insecure.
func ElideInsecureDeprecationNotice(csvStr string) string {
	// v20.1 introduces a deprecation notice for --insecure. Skip over it.
	// TODO(knz): Remove this when --insecure is dropped.
	// See: https://github.com/cockroachdb/cockroach/issues/53404
	lines := strings.SplitN(csvStr, "\n", 3)
	if len(lines) > 0 && strings.HasPrefix(lines[0], "Flag --insecure has been deprecated") {
		csvStr = lines[2]
	}
	return csvStr
}

// MatchCSV matches a multi-line csv string with the provided regex
// (matchColRow[i][j] will be matched against the i-th line, j-th column).
func MatchCSV(csvStr string, matchColRow [][]string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "csv input:\n%v\nexpected:\n%s\n",
				csvStr, pretty.Sprint(matchColRow))
		}
	}()

	csvStr = ElideInsecureDeprecationNotice(csvStr)
	reader := csv.NewReader(strings.NewReader(csvStr))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	lr, lm := len(records), len(matchColRow)
	if lr < lm {
		return errors.Errorf("csv has %d rows, but expected at least %d", lr, lm)
	}

	// Compare only the last len(matchColRow) records. That is, if we want to
	// match 4 rows and we have 100 records, we only really compare
	// records[96:], that is, the last four rows.
	records = records[lr-lm:]

	for i := range records {
		if lr, lm := len(records[i]), len(matchColRow[i]); lr != lm {
			return errors.Errorf("row #%d: csv has %d columns, but expected %d", i+1, lr, lm)
		}
		for j := range records[i] {
			pat, str := matchColRow[i][j], records[i][j]
			re := regexp.MustCompile(pat)
			if !re.MatchString(str) {
				err = errors.Wrapf(err, "row #%d, col #%d: found %q which does not match %q",
					i+1, j+1, str, pat)
			}
		}
	}
	return err
}

// RemoveMatchingLines removes lines from the input string that match any of
// the provided regexps. Mind that regexp could match a substrings, so you need
// to put ^ and $ around to ensure full matches.
func RemoveMatchingLines(output string, regexps []string) string {
	if len(regexps) == 0 {
		return output
	}

	var patterns []*regexp.Regexp
	for _, weed := range regexps {
		p := regexp.MustCompile(weed)
		patterns = append(patterns, p)
	}
	filter := func(line string) bool {
		for _, pattern := range patterns {
			if pattern.MatchString(line) {
				return true
			}
		}
		return false
	}

	result := strings.Builder{}
	for _, line := range strings.Split(output, "\n") {
		if filter(line) || len(line) == 0 {
			continue
		}
		result.WriteString(line)
		result.WriteRune('\n')
	}
	return result.String()
}

// GetCsvNumCols returns the number of columns in the given csv string.
func GetCsvNumCols(csvStr string) (cols int, err error) {
	csvStr = ElideInsecureDeprecationNotice(csvStr)
	reader := csv.NewReader(strings.NewReader(csvStr))
	records, err := reader.Read()
	if err != nil {
		return 0, errors.Wrapf(err, "error reading csv input:\n %v\n", csvStr)
	}
	return len(records), nil
}
