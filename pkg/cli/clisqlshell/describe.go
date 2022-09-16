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

import "github.com/cockroachdb/errors"

type dkey struct {
	prefix string
	nargs  int
}

var dcmds = map[dkey]func(bool, bool) string{
	{``, 0}:  func(p, s bool) string { return `SHOW TABLES` },
	{``, 1}:  func(p, s bool) string { return `SHOW COLUMNS FROM $1` },
	{`T`, 0}: func(p, s bool) string { return `SHOW TYPES` },
	{`t`, 0}: func(p, s bool) string { return `SHOW TABLES` },
	{`u`, 0}: func(p, s bool) string { return `SHOW USERS` },
	{`u`, 1}: func(p, s bool) string { return `SELECT * FROM [SHOW USERS] WHERE username = $1` },
	{`d`, 1}: func(p, s bool) string { return `SHOW CONSTRAINTS FROM $1 WITH COMMENT` },
}

func (c *cliState) pgInspect(args []string) (sql string, qargs []interface{}, err error) {
	cmd := args[0]
	origCmd := cmd
	args = args[1:]

	// NB: cmd at this point is guaranteed to be prefixed by `\d`, so
	// contains 2+ bytes.

	plus := false
	if cmd[len(cmd)-1] == '+' {
		plus = true
		cmd = cmd[:len(cmd)-1]
	}
	inclSystem := false
	if cmd[len(cmd)-1] == 'S' {
		inclSystem = true
		cmd = cmd[:len(cmd)-1]
	}
	// Finally strip the basic `\d` pattern.
	cmd = cmd[2:]

	key := dkey{cmd, len(args)}
	fn := dcmds[key]
	if fn == nil {
		return "", nil, errors.WithHint(
			errors.Newf("unsupported command: %s with %d arguments", origCmd, len(args)),
			"Use the SQL SHOW statement to inspect your schema.")
	}

	for _, a := range args {
		qargs = append(qargs, a)
	}
	return fn(plus, inclSystem), qargs, nil
}
