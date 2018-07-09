// Copyright 2016 The Cockroach Authors.
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

package main

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestListFailures(t *testing.T) {
	for key, value := range map[string]string{
		pkgEnv: "github.com/cockroachdb/cockroach/pkg/storage",
	} {
		if val, ok := os.LookupEnv(key); ok {
			defer func() {
				if err := os.Setenv(key, val); err != nil {
					t.Error(err)
				}
			}()
		} else {
			defer func() {
				if err := os.Unsetenv(key); err != nil {
					t.Error(err)
				}
			}()
		}
		if err := os.Setenv(key, value); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		fileName    string
		packageName string
		testName    string
		message     string
		author      string
	}{
		{
			fileName:    "stress-unknown",
			packageName: "github.com/cockroachdb/cockroach/pkg/storage",
			testName:    "(unknown)",
			message:     "make: *** [bin/.submodules-initialized] Error 1",
			author:      "",
		},
	}
	for _, c := range testCases {
		t.Run(c.fileName, func(t *testing.T) {
			file, err := os.Open(filepath.Join("testdata", c.fileName))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()

			f := func(_ context.Context, packageName, testName, testMessage, author string) error {
				if c.packageName != packageName {
					t.Fatalf("expected %s, but got %s", c.packageName, packageName)
				}
				if c.testName != testName {
					t.Fatalf("expected %s, but got %s", c.testName, testName)
				}
				if c.author != author {
					t.Fatalf("expected %s, but got %s", c.author, author)
				}
				messageRE := regexp.MustCompile(regexp.QuoteMeta(c.message))
				if !messageRE.MatchString(testMessage) {
					t.Fatalf("expected %s, but got %s", messageRE, testMessage)
				}
				return nil
			}

			if err := listFailures(context.Background(), file, f); err != nil {
				t.Fatal(err)
			}
		})
	}
}
