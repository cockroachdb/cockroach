package config

import (
	"log"
	"os/user"
)

var (
	Binary     = "cockroach"
	SlackToken string
	OSUser     *user.User
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
