// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
	DefaultDebugDir = "${HOME}/.roachprod/debug"
	DefaultHostDir  = "${HOME}/.roachprod/hosts"
	EmailDomain     = "@cockroachlabs.com"
	Local           = "local"
)
