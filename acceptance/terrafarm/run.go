// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf

package terrafarm

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

func (f *Farmer) run(cmd string, args ...string) (_ string, _ string, err error) {
	c := exec.Command(cmd, args...)
	c.Dir = f.Cwd
	var outBuf, errBuf bytes.Buffer
	{
		p, err := c.StdinPipe()
		if err != nil {
			return "", "", err
		}
		_ = p.Close() // no input
	}
	f.logf("%s %s\n", cmd, strings.Join(args, " "))
	o, err := c.StdoutPipe()
	if err != nil {
		return "", "", err
	}
	e, err := c.StderrPipe()
	if err != nil {
		return "", "", err
	}
	scanO := bufio.NewScanner(o)
	scanE := bufio.NewScanner(e)
	go func() {
		for scanO.Scan() {
			line := scanO.Text() + "\n"
			_, _ = outBuf.WriteString(line)
			f.logf("%s", line)
		}
	}()
	go func() {
		for scanE.Scan() {
			line := scanE.Text() + "\n"
			_, _ = errBuf.WriteString(line)
			f.logf("%s", line)
		}
	}()

	if err := c.Start(); err != nil {
		return "", "", err
	}
	err = c.Wait()
	return outBuf.String(), errBuf.String(), err
}

func (f *Farmer) runErr(cmd string, args ...string) error {
	o, e, err := f.run(cmd, args...)
	if err != nil {
		return fmt.Errorf("failed: %s\nstdout: %s\nstderr: %s", o, e, err)
	}
	return nil
}

func (f *Farmer) appendDefaults(args []string) []string {
	return append(args, "-no-color", "--var=key_name="+f.KeyName)
}

func (f *Farmer) apply(args ...string) error {
	args = f.appendDefaults(append([]string{"apply"}, args...))
	if err := f.runErr("terraform", args...); err != nil {
		return err
	}
	f.refresh()
	return nil
}

func (f *Farmer) output(key string) []string {
	o, _, err := f.run("terraform", "output", key, "-no-color")
	if _, ok := err.(*exec.ExitError); err != nil && !ok {
		f.logf("%s", err)
		return nil
	}
	o = strings.TrimSpace(o)
	if len(o) == 0 {
		return nil
	}
	return strings.Split(o, ",")
}

func (f *Farmer) execSupervisor(host string, action string) (string, string, error) {
	cmd := "supervisorctl -c supervisor.conf " + action
	return f.ssh(host, f.defaultKeyFile(), cmd)
}
