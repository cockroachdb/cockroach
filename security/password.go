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
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/pkg/errors"

	"golang.org/x/crypto/ssh/terminal"
)

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

// PromptForPasswordAndHash prompts for a password on the stdin twice, and if
// both match, returns the hash of the password + username. This hash is
// compatible with md5 pgwire authentication.
func PromptForPasswordAndHash(username string) (string, error) {
	password, err := PromptForPassword()
	if err != nil {
		return "", err
	}
	return MD5Hash(password + username), nil
}

// GetSalt returns a random salt of length n.
func GetSalt(n uint) ([]byte, error) {
	salt := make([]byte, n)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	return salt, nil
}

func MD5Hash(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}
