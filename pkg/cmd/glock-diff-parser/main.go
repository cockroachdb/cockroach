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
// $ git diff master -- GLOCKFILE | glock-diff-parser
// https://github.com/coreos/etcd/compare/33e4f2ea283c187cac459462994d084f44d7c9de...7022d2d00cb4dc6adad9a2627566e9675e14b28b
// https://github.com/lightstep/lightstep-tracer-go/compare/7ec5005048fddb1fc15627e1bf58796ce01d919e...7e238cc7deca88342f9fb7875ffc97cc450e6607
// https://github.com/golang/net/compare/f4b625ec9b21d620bb5ce57f2dfc3e08ca97fce6...8b4af36cd21a1f85a7484b49feb7c79363106d8e
// https://github.com/dominikh/go-simple/compare/e4b33f62951a263109be351a6e7f516df082e667...12cefce78c236d3784995fb6e655547c4ffe55d0
// https://github.com/golang/tools/compare/4c4edc0becc8f904a613878ed3e4f970f40a83ce...69f6f5b782e1f090edb33f68be67d96673a8059e
// https://github.com/docker/distribution/compare/99cb7c0946d2f5a38015443e515dc916295064d7...717ac0337f312fc7ca0fc35279f00001caf6dd0b
// https://github.com/lib/pq/compare/fcb9ef54da7cae1ea08f0b5a92f236d83e59294a...ae8357db35d721c58dcdc911318b55bef6b1b001
// https://github.com/opencontainers/runc/compare/02f8fa7863dd3f82909a73e2061897828460d52f...509ddd6f118c243c6fe343a2e95c847a3b013a0a
// https://github.com/golang/crypto/compare/484eb34681af59703e639b971bc307019182c41f...4428aee3e5957ee2252b9c7a17460e5147363b4b
// https://github.com/grpc-ecosystem/grpc-gateway/compare/acebe0f9ff5993e130b141ee60e83e592839ca22...7ba755f85a3dc73224047317dab675b32d3a91e0
// https://github.com/pborman/uuid/compare/b984ec7fa9ff9e428bd0cf0abf429384dfbe3e37...3d4f2ba23642d3cfd06bd4b54cf03d99d95c0f1b
// https://github.com/golang/text/compare/098f51fb687dbaba1f6efabeafbb6461203f9e21...fa5033c827cad7080e8e7047a0091945b0e1f031
// https://github.com/elastic/gosigar/compare/2716c1fe855ee5c88eae707195e0688374458c92...15322f7ed81bc3cfedb66b6716fd272ce042e9db
// https://github.com/golang/lint/compare/64229b89515c2a585c623c79a7ccdea71e8589ff...3390df4df2787994aea98de825b964ac7944b817
// https://github.com/golang/protobuf/compare/df1d3ca07d2d07bba352d5b73c4313b4e2a6203e...98fa357170587e470c5f27d3c3ea0947b71eb455
// https://github.com/google/btree/compare/7364763242911ab6d418d2722e237194938ebad0...925471ac9e2131377a91e1595defec898166fe49
// https://github.com/spf13/cobra/compare/9c28e4bbd74e5c3ed7aacbc552b2cab7cfdfe744...856b96dcb49d6427babe192998a35190a12c2230
// https://github.com/spf13/pflag/compare/c7e63cf4530bcd3ba943729cee0efeff2ebea63f...bf8481a6aebc13a8aab52e699ffe2e79771f5a3f
// https://github.com/tebeka/go2xunit/compare/cb8eb3dedfac9cf4cb1f2d8988f3390f2d0cf2e7...
// https://github.com/golang/sys/compare/8f0908ab3b2457e2e15403d3697c9ef5cb4b57a9...002cbb5f952456d0c50e0d2aff17ea5eca716979
// https://github.com/docker/docker/compare/bd57e6d25c2c9d41a6cf2c9931606f58bb66a07d...fb65df5bfe7c03a449b4d7c2c57d73de590ea2e1
// https://github.com/gogo/protobuf/compare/fdc14ac22689d09f8639e603614593811bc1d81c...50d1bd39ce4e7a96b75e3e040be9caf79dbb4c61
// https://github.com/mattn/go-runewidth/compare/d6bea18f789704b5f83375793155289da36a3c7f...737072b4e32b7a5018b4a7125da8d12de90e8045
// https://github.com/opentracing/opentracing-go/compare/30dda9350627161ff15581c0bdc504e32ec9a536...902ca977fd85455c364050f985eba376b44315f0
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

	scanner:
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line[1:], "cmd") {
				continue
			}
			for _, ignore := range []string{
				" ",
				"@@",
			} {
				if strings.HasPrefix(line, ignore) {
					continue scanner
				}
			}
			fields := strings.Fields(line[1:])
			pkgName := fields[0]
			for orig, replacement := range map[string]string{
				"golang.org/x":                "github.com/golang",
				"google.golang.org/appengine": "github.com/golang/appengine",
				"google.golang.org/grpc":      "github.com/grpc/grpc-go",
				"gopkg.in/inf.v0":             "github.com/go-inf/inf",
				"honnef.co/go/":               "github.com/dominikh/go-",
			} {
				pkgName = strings.Replace(pkgName, orig, replacement, 1)
			}
			if !strings.HasPrefix(pkgName, "github.com/") {
				return errors.Errorf("unhandled import path %s in %s", pkgName, line)
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
