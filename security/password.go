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
// Author: Marc Berhault (marc@cockroachlabs.com)

package security

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/pkg/errors"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/ssh/terminal"
)

// BCrypt cost should increase along with computation power.
// For estimates, see: http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt
// For now, we use the library's default cost.
const bcryptCost = bcrypt.DefaultCost

// HashPassword hashes the provided string and returns the string representation
// of this hash.
func HashPassword(s string) (string, error) {
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(s), bcryptCost)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(hashedPassword), nil
}

// PromptForPassword prompts the user for a password twice, returning
// the read bytes if they match, or an error.
// It turns out getting non-echo stdin is tricky and not portable at all.
// terminal seems a decent solution, although it does not work on windows.
func PromptForPassword() (string, error) {
	fmt.Print("Enter password: ")
	one, err := terminal.ReadPassword(0)
	if err != nil {
		return "", err
	}
	fmt.Print("\nConfirm password: ")
	two, err := terminal.ReadPassword(0)
	if err != nil {
		return "", err
	}
	// Make sure stdout moves on to the next line.
	fmt.Print("\n")
	if !bytes.Equal(one, two) {
		return "", errors.Errorf("password mismatch")
	}
	return string(one), nil
}

// PromptForPasswordAndHash prompts the user for a password twice and returns
// the string representation of the bcrypt hash.
func PromptForPasswordAndHash() (string, error) {
	password, err := PromptForPassword()
	if err != nil {
		return "", err
	}
	return HashPassword(password)
}
