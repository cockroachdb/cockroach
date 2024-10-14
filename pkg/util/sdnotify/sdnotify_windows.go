// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sdnotify

import (
	"os/exec"

	"github.com/cockroachdb/errors"
)

func ready(preNotify func()) error {
	return nil
}

func bgExec(*exec.Cmd, string) error {
	return errors.New("not implemented")
}
