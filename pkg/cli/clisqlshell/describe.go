// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlshell

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/errors"
)

type dkey struct {
	prefix string
	nargs  int
}

var dcmds = map[dkey]func(bool, bool) string{
	{`l`, 0}:  func(p, s bool) string { return `SHOW DATABASES` },
	{`d`, 0}:  func(p, s bool) string { return `SHOW TABLES` },
	{`d`, 1}:  func(p, s bool) string { return `SHOW COLUMNS FROM %[1]s` },
	{`dT`, 0}: func(p, s bool) string { return `SHOW TYPES` },
	{`dt`, 0}: func(p, s bool) string { return `SHOW TABLES` },
	{`du`, 0}: func(p, s bool) string { return `SHOW USERS` },
	{`du`, 1}: func(p, s bool) string { return `SELECT * FROM [SHOW USERS] WHERE username = %[2]s` },
	{`dd`, 1}: func(p, s bool) string { return `SHOW CONSTRAINTS FROM %[1]s WITH COMMENT` },
}

func pgInspect(args []string) (sql string, qargs []interface{}, err error) {
	origCmd := args[0]
	// Strip the leading `\`.
	cmd := origCmd[1:]
	args = args[1:]

	plus := strings.Contains(cmd, "+")
	inclSystem := strings.Contains(cmd, "S")
	// Remove the characters "S" and "+" from the describe command.
	cmd = strings.TrimRight(cmd, "S+")

	key := dkey{cmd, len(args)}
	fn := dcmds[key]
	if fn == nil {
		return "", nil, errors.WithHint(
			errors.Newf("unsupported command: %s with %d arguments", origCmd, len(args)),
			"Use the SQL SHOW statement to inspect your schema.")
	}

	for _, a := range args {
		qargs = append(qargs, a, lexbase.EscapeSQLString(a))
	}
	return fn(plus, inclSystem), qargs, nil
}
