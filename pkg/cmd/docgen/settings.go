// Copyright 2017 The Cockroach Authors.
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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	// This should match main.go to link everything for init-registered settings.
	_ "github.com/cockroachdb/cockroach/pkg/ccl" // ccl init hooks
	_ "github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	_ "github.com/cockroachdb/cockroach/pkg/ui/distccl" // ccl web UI init hook
	"github.com/olekukonko/tablewriter"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func init() {
	cmds = append(cmds, &cobra.Command{
		Use:   "settings <output-dir>",
		Short: "generate markdown documentation of cluster settings",
		RunE: func(cmd *cobra.Command, args []string) error {
			outDir := filepath.Join("docs", "generated", "settings")
			if len(args) > 0 {
				outDir = args[0]
			}

			if stat, err := os.Stat(outDir); err != nil {
				return err
			} else if !stat.IsDir() {
				return errors.Errorf("%q is not a directory", outDir)
			}

			return ioutil.WriteFile(filepath.Join(outDir, "settings.md"), generateSettings(), 0644)
		},
	})
}

func tick(s string) string {
	return fmt.Sprintf("`%s`", s)
}

func generateSettings() []byte {
	// Fill a Values struct with the defaults.
	s := cluster.MakeTestingClusterSettings()
	settings.NewUpdater(&s.SV).ResetRemaining()

	b := new(bytes.Buffer)
	table := tablewriter.NewWriter(b)
	table.SetHeader([]string{"Setting", "Type", "Default", "Description"})
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")
	table.SetAutoWrapText(false)

	for _, name := range settings.Keys() {
		setting, ok := settings.Lookup(name)
		if !ok {
			panic(fmt.Sprintf("could not find setting %q", name))
		}
		typ, ok := settings.ReadableTypes[setting.Typ()]
		if !ok {
			panic(fmt.Sprintf("unknown setting type %q", setting.Typ()))
		}
		defaultVal := setting.String(&s.SV)
		if override, ok := sqlmigrations.SettingsDefaultOverrides[name]; ok {
			defaultVal = override
		}
		table.Append([]string{tick(name), typ, tick(defaultVal), setting.Description()})
	}
	table.Render()
	return b.Bytes()
}
