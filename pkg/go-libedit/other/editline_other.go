// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright 2017 Raphael 'kena' Poss
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

package libedit_other

import (
	"bufio"
	"os"

	common "github.com/knz/go-libedit/common"
)

type EditLine int
type CompletionGenerator = common.CompletionGenerator

type state struct {
	reader                *bufio.Reader
	promptLeft            string
	line                  string
	stdin, stdout, stderr *os.File
}

var editors []state

func Init(x string, w bool) (EditLine, error) {
	return InitFiles(x, w, os.Stdin, os.Stdout, os.Stderr)
}

func InitFiles(_ string, _ bool, stdin, stdout, stderr *os.File) (EditLine, error) {
	st := state{
		reader: bufio.NewReader(stdin),
		stdin:  stdin, stdout: stdout, stderr: stderr,
	}
	editors = append(editors, st)
	return EditLine(len(editors) - 1), nil
}

func (el EditLine) RebindControlKeys()                  {}
func (el EditLine) Close()                              {}
func (el EditLine) SaveHistory() error                  { return nil }
func (el EditLine) AddHistory(_ string) error           { return nil }
func (el EditLine) LoadHistory(_ string) error          { return nil }
func (el EditLine) SetAutoSaveHistory(_ string, _ bool) {}
func (el EditLine) UseHistory(_ int, _ bool) error      { return nil }
func (el EditLine) SetCompleter(_ CompletionGenerator)  {}
func (el EditLine) SetRightPrompt(_ string)             {}

func (el EditLine) Stdin() *os.File {
	return editors[el].stdin
}

func (el EditLine) Stdout() *os.File {
	return editors[el].stdout
}

func (el EditLine) Stderr() *os.File {
	return editors[el].stderr
}

func (el EditLine) SetLeftPrompt(l string) {
	st := &editors[el]
	st.promptLeft = l
}

func (el EditLine) GetLineInfo() (string, int) {
	st := &editors[el]
	return st.line, len(st.line)
}

func (el EditLine) GetLine() (string, error) {
	st := &editors[el]
	st.stdout.WriteString(st.promptLeft)
	st.stdout.Sync()
	line, err := st.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	st.line = line
	return st.line, nil
}
