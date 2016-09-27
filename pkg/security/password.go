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
	"crypto/sha256"
	"fmt"

	"github.com/pkg/errors"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/ssh/terminal"
)

// BCrypt cost should increase along with computation power.
// For estimates, see: http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt
// For now, we use the library's default cost.
const bcryptCost = bcrypt.DefaultCost

func compareHashAndPassword(hashedPassword []byte, password string) error {
	h := sha256.New()
	return bcrypt.CompareHashAndPassword(hashedPassword, h.Sum([]byte(password)))
}

// HashPassword takes a raw password and returns a bcrypt hashed password.
func HashPassword(password string) ([]byte, error) {
	h := sha256.New()
	return bcrypt.GenerateFromPassword(h.Sum([]byte(password)), bcryptCost)
}

// PromptForPassword prompts for a password twice, returning the read string if
// they match, or an error.
func PromptForPassword() (string, error) {
	// Getting non-echo stdin is tricky and not portable at all. terminal seems
	// a decent solution, although it does not work on windows.
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
		return "", errors.New("password mismatch")
	}

	return string(one), nil
}

// PromptForPasswordAndHash prompts for a password twice and returns the bcrypt
// hash.
func PromptForPasswordAndHash() ([]byte, error) {
	password, err := PromptForPassword()
	if err != nil {
		return nil, err
	}
	return HashPassword(password)
}
