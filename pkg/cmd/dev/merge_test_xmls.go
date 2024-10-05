// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"encoding/xml"
	"os"

	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/spf13/cobra"
)

func makeMergeTestXMLsCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	mergeTestXMLsCommand := &cobra.Command{
		Use:   "merge-test-xmls XML1 [XML2...]",
		Short: "Merge the given test XML's (utility command)",
		Long:  "Merge the given test XML's (utility command)",
		Args:  cobra.MinimumNArgs(1),
		RunE:  runE,
	}
	mergeTestXMLsCommand.Hidden = true
	return mergeTestXMLsCommand
}

func (d *dev) mergeTestXMLs(cmd *cobra.Command, xmls []string) error {
	var suites []bazelutil.TestSuites
	for _, file := range xmls {
		suitesToAdd := bazelutil.TestSuites{}
		input, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		err = xml.Unmarshal(input, &suitesToAdd)
		if err != nil {
			return err
		}
		suites = append(suites, suitesToAdd)
	}
	return bazelutil.MergeTestXMLs(suites, os.Stdout)
}
