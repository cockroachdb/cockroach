// Copyright 2018 The Cockroach Authors.
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

package pprofui

import "github.com/spf13/pflag"

// pprofFlags is a wrapper to satisfy pprof's client flag interface.
// That interface is satisfied by what the standard flag package
// offers, with some tweaks. In this package, we just want to specify
// the command line args directly; pprofFlags lets us do that
// essentially by mocking out `os.Args`. `pprof` will register all of
// its flags via this struct, and then they get populated from `args`
// below.
type pprofFlags struct {
	args []string // passed to Parse()
	*pflag.FlagSet
}

func (pprofFlags) ExtraUsage() string {
	return ""
}

func (f pprofFlags) StringList(o, d, c string) *[]*string {
	return &[]*string{f.String(o, d, c)}
}

func (f pprofFlags) Parse(usage func()) []string {
	f.FlagSet.Usage = usage
	if err := f.FlagSet.Parse(f.args); err != nil {
		panic(err)
	}
	return f.FlagSet.Args()
}
