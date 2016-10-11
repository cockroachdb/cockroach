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
	"fmt"

	"github.com/pkg/errors"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/ssh/terminal"
)

// BCrypt cost should increase along with computation power.
// For estimates, see: http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt
// For now, we use the library's default cost.
// TODO(marc): re-evaluate when we do actual authentication.
const bcryptCost = bcrypt.DefaultCost

// promptForPassword prompts the user for a password twice, returning
// the read bytes if they match, or an error.
// It turns out getting non-echo stdin is tricky and not portable at all.
// terminal seems a decent solution, although it does not work on windows.
func promptForPassword() ([]byte, error) {
	// Use a raw terminal.
	oldState, err := terminal.MakeRaw(0)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = terminal.Restore(0, oldState)
	}()

	fmt.Print("Enter password: ")
	one, err := terminal.ReadPassword(0)
	if err != nil {
		return nil, err
	}
	fmt.Print("\nConfirm password: ")
	two, err := terminal.ReadPassword(0)
	if err != nil {
		return nil, err
	}
	// Make sure stdout moves on to the next line.
	fmt.Print("\n")
	if !bytes.Equal(one, two) {
		return nil, errors.Errorf("password mismatch")
	}
	return one, nil
}

// HashPassword takes a raw password and returns a bcrypt hashed password.
func HashPassword(raw []byte) ([]byte, error) {
	return bcrypt.GenerateFromPassword(raw, bcryptCost)
}

// PromptForPasswordAndHash prompts for a password on the stdin twice,
// and if both match, returns a bcrypt hashed password.
func PromptForPasswordAndHash() ([]byte, error) {
	password, err := promptForPassword()
	if err != nil {
		return nil, err
	}
	// TODO(marc): we may want to have a minimum length.
	if len(password) == 0 {
		return nil, errors.Errorf("password cannot be empty")
	}
	return HashPassword(password)
}
