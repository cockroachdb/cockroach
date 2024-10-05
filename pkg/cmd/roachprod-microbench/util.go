// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/spf13/cobra"
)

const timeFormat = "2006-01-02T15_04_05"

func splitArgsAtDash(cmd *cobra.Command, args []string) (before, after []string) {
	argsLenAtDash := cmd.ArgsLenAtDash()
	if argsLenAtDash < 0 {
		// If there's no dash, the value of this is -1.
		before = args[:len(args):len(args)]
	} else {
		// NB: Have to do this verbose slicing to force Go to copy the
		// memory. Otherwise later `append`s will break stuff.
		before = args[0:argsLenAtDash:argsLenAtDash]
		after = args[argsLenAtDash:len(args):len(args)]
	}
	return
}

// verifyPathFlag verifies that the given path flag points to a file or
// directory, depending on the expectDir flag.
func verifyPathFlag(flagName, path string, expectDir bool) error {
	if fi, err := os.Stat(path); err != nil {
		return fmt.Errorf("the %s flag points to a path %s that does not exist", flagName, path)
	} else {
		switch isDir := fi.Mode().IsDir(); {
		case expectDir && !isDir:
			return fmt.Errorf("the %s flag must point to a directory not a file", flagName)
		case !expectDir && isDir:
			return fmt.Errorf("the %s flag must point to a file not a directory", flagName)
		}
	}
	return nil
}

// getRegexExclusionPairs returns a list of regex exclusion pairs, separated by
// comma, derived from the command flags. The first element of the pair is the
// package regex and the second is the microbenchmark regex.
func getRegexExclusionPairs(excludeList []string) [][]*regexp.Regexp {
	excludeRegexes := make([][]*regexp.Regexp, 0)
	for _, pair := range excludeList {
		pairSplit := strings.Split(pair, ":")
		var pkgRegex, benchRegex *regexp.Regexp
		if len(pairSplit) != 2 {
			pkgRegex = regexp.MustCompile(".*")
			benchRegex = regexp.MustCompile(pairSplit[0])
		} else {
			pkgRegex = regexp.MustCompile(pairSplit[0])
			benchRegex = regexp.MustCompile(pairSplit[1])
		}
		excludeRegexes = append(excludeRegexes, []*regexp.Regexp{pkgRegex, benchRegex})
	}
	return excludeRegexes
}

func initRoachprod(l *logger.Logger) error {
	_ = roachprod.InitProviders()
	_, err := roachprod.Sync(l, vm.ListOptions{})
	return err
}

func roachprodRun(clusterName string, l *logger.Logger, cmdArray []string) error {
	return roachprod.Run(
		context.Background(), l, clusterName, "", "", false,
		os.Stdout, os.Stderr, cmdArray, install.DefaultRunOptions(),
	)
}

func initLogger(path string) *logger.Logger {
	loggerCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr}
	var loggerError error
	l, loggerError := loggerCfg.NewLogger(path)
	if loggerError != nil {
		_, _ = fmt.Fprintf(os.Stderr, "unable to configure logger: %s\n", loggerError)
		os.Exit(1)
	}
	return l
}
