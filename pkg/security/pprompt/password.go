// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package pprompt provides a facility to prompt a user for a password
// securely (i.e. without echoing the password) from an interactive
// terminal.
//
// This is a separate package to ensure that the CLI code that uses
// this does not indirectly depend on other features from the
// 'security' package.
package pprompt

import (
	"fmt"
	"os"

	"golang.org/x/term"
)

// PromptForPassword prompts for a password.
// This is meant to be used when using a password.
func PromptForPassword() (string, error) {
	fmt.Print("Enter password: ")
	password, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", err
	}
	// Make sure stdout moves on to the next line.
	fmt.Print("\n")

	return string(password), nil
}
