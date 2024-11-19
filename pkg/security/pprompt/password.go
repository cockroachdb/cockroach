// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
func PromptForPassword(prompt string) (string, error) {
	if prompt == "" {
		prompt = "Enter password: "
	}
	fmt.Print(prompt)
	password, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", err
	}
	// Make sure stdout moves on to the next line.
	fmt.Print("\n")

	return string(password), nil
}
