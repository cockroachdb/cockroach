// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

// extractGroups parses a yaml file located at the given path,
// expecting to find a top-level "groups" array.
func extractGroups(path string) ([]interface{}, error) {
	var ruleFile struct {
		Groups []interface{}
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	if err := yaml.UnmarshalStrict(data, &ruleFile); err != nil {
		return nil, err
	}

	if len(ruleFile.Groups) == 0 {
		return nil, errors.New("did not find a top-level groups entry")
	}
	return ruleFile.Groups, nil
}

func main() {
	var outFile string
	rootCmd := &cobra.Command{
		Use:     "wraprules",
		Short:   "wraprules wraps one or more promethus monitoring files into a PrometheusRule object",
		Example: "wraprules -o alert-rules.yaml monitoring/rules/*.rules.yml",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(outFile) == 0 {
				return errors.New("no output file given")
			}
			if len(args) == 0 {
				return errors.New("no input file(s) given")
			}

			var outGroups []interface{}
			for _, path := range args {
				extracted, err := extractGroups(path)
				if err != nil {
					return errors.Wrapf(err, "unable to extract from %s", path)
				}
				outGroups = append(outGroups, extracted...)
			}

			type bag map[string]interface{}
			output := bag{
				"apiVersion": "monitoring.coreos.com/v1",
				"kind":       "PrometheusRule",
				"metadata": bag{
					"name": "prometheus-cockroachdb-rules",
					"labels": bag{
						"app":        "cockroachdb",
						"prometheus": "cockroachdb",
						"role":       "alert-rules",
					},
				},
				"spec": bag{
					"groups": outGroups,
				},
			}

			outBytes, err := yaml.Marshal(output)
			if err != nil {
				return err
			}

			prelude := "# GENERATED FILE - DO NOT EDIT\n"
			outBytes = append([]byte(prelude), outBytes...)

			return ioutil.WriteFile(outFile, outBytes, 0666)
		},
	}
	rootCmd.Flags().StringVarP(&outFile, "out", "o", "", "The output file")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
