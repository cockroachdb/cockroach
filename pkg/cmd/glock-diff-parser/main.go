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

// This utility is useful when auditing dependency updates. Sample invocation:
//
// ```
// git diff master -- GLOCKFILE | glock-diff-parser | xargs open
// ```
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/pkg/errors"
)

func main() {
	pkgs := make(map[string]struct {
		minusRef, plusRef string
	})

	if err := func() error {
		scanner := bufio.NewScanner(os.Stdin)

		for scanner.Scan() && !strings.HasPrefix(scanner.Text(), "@@") {

		}

		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "@@") {
				continue
			}
			if strings.HasPrefix(line, " ") {
				continue
			}
			fields := strings.Fields(line[1:])
			pkgName := fields[0]
			for orig, replacement := range map[string]string{
				"golang.org/x":           "github.com/golang",
				"honnef.co/go/":          "github.com/dominikh/go-",
				"google.golang.org/grpc": "github.com/grpc/grpc-go",
				"gopkg.in/inf.v0":        "github.com/go-inf/inf",
			} {
				pkgName = strings.Replace(pkgName, orig, replacement, 1)
			}
			if !strings.HasPrefix(pkgName, "github.com/") {
				return errors.Errorf("unhandled import path %s", pkgName)
			}
			pkg := pkgs[pkgName]

			switch line[0] {
			case '-':
				pkg.minusRef = fields[1]
			case '+':
				pkg.plusRef = fields[1]
			default:
				return errors.Errorf("unexpected line %s", line)
			}
			pkgs[pkgName] = pkg
		}
		return scanner.Err()
	}(); err != nil {
		log.Fatal(err)
	}

	for pkg, info := range pkgs {
		fmt.Printf("https://%s/compare/%s...%s\n", pkg, info.minusRef, info.plusRef)
	}
}
