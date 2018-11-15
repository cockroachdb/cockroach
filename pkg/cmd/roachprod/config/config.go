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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package config

import (
	"log"
	"os/user"
)

var (
	// Binary TODO(peter): document
	Binary = "cockroach"
	// SlackToken TODO(peter): document
	SlackToken string
	// OSUser TODO(peter): document
	OSUser *user.User
)

func init() {
	var err error
	OSUser, err = user.Current()
	if err != nil {
		log.Panic("Unable to determine OS user", err)
	}
}

// A sentinel value used to indicate that an installation should
// take place on the local machine.  Later in the refactoring,
// this ought to be replaced by a LocalCloudProvider or somesuch.
const (
	DefaultHostDir = "${HOME}/.roachprod/hosts"
	EmailDomain    = "@cockroachlabs.com"
	Local          = "local"
)
