// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"
)

// TimeFormat defines a constant format for time-related strings, using underscores instead of colons in the time portion.
const TimeFormat = "2006-01-02T15_04_05"

var (
	// invalidCharRegex matches
	// the first character if it is not a letter (a-z, A-Z) or an underscore (_)
	// or
	// any character that is not a letter (a-z, A-Z), digit (0-9), or underscore (_).
	invalidCharKeyRegex = regexp.MustCompile(`(^[^a-zA-Z_])|([^a-zA-Z0-9_])`)
	// invalidCharValueRegex
	invalidCharValueRegex = regexp.MustCompile(`[\\\n"]`)
)

// LabelMapToString converts a map of labels (key-value pairs) into a formatted string.
// It sorts the labels by key and ensures the keys and values are sanitized.
// The result is a string of the form: key1="value1",key2="value2",...
func LabelMapToString(labels map[string]string) string {
	keys := maps.Keys(labels)
	sort.Strings(keys)
	kvLabels := make([]string, 0)
	for _, key := range keys {
		kvLabels = append(kvLabels,
			fmt.Sprintf(`%s="%s"`, SanitizeKey(key), SanitizeValue(labels[key])))
	}
	return strings.Join(kvLabels, ",")
}

// SanitizeKey replaces all invalid characters in the input string with underscores (_).
// Additionally, it ensures the first character is a letter or an underscore; otherwise, it's replaced with an underscore.
func SanitizeKey(input string) string {
	// Replace all characters that match as per the regex with an underscore.
	return invalidCharKeyRegex.ReplaceAllString(input, "_")
}

// SanitizeValue replaces all \,\n and " with underscores (_).
func SanitizeValue(input string) string {
	// Replace all characters that match as per the regex with an underscore.
	return invalidCharValueRegex.ReplaceAllString(input, "_")
}

// SplitArgsAtDash splits the args slice at the position of the dash ("--") in a cobra command.
// It returns two slices: before and after the dash. If no dash is found, all args are returned in the "before" slice.
func SplitArgsAtDash(cmd *cobra.Command, args []string) (before, after []string) {
	argsLenAtDash := cmd.ArgsLenAtDash() // Get the index of the dash in the args slice.
	if argsLenAtDash < 0 {
		// If there's no dash (ArgsLenAtDash returns -1), return all args in the "before" slice.
		before = args[:len(args):len(args)]
	} else {
		// Split the args into two slices: before and after the dash.
		// The third index in slicing ensures Go creates a new slice to avoid side effects of future `append` operations.
		before = args[0:argsLenAtDash:argsLenAtDash]
		after = args[argsLenAtDash:len(args):len(args)]
	}
	return
}

// VerifyPathFlag checks whether the given path (from a command-line flag) exists and whether it matches the expected type
// (directory or file) based on the expectDir flag. It returns an error if the path does not exist or is of the wrong type.
func VerifyPathFlag(flagName, path string, expectDir bool) error {
	if fi, err := os.Stat(path); err != nil {
		// If the path doesn't exist, return an error with a descriptive message.
		return fmt.Errorf("the %s flag points to a path %s that does not exist", flagName, path)
	} else {
		// Check whether the path is a directory.
		isDir := fi.Mode().IsDir()
		switch {
		case expectDir && !isDir:
			// Return an error if the path should be a directory but is a file.
			return fmt.Errorf("the %s flag must point to a directory not a file", flagName)
		case !expectDir && isDir:
			// Return an error if the path should be a file but is a directory.
			return fmt.Errorf("the %s flag must point to a file not a directory", flagName)
		}
	}
	return nil
}

// GetRegexExclusionPairs takes a list of exclusion patterns and returns a slice of pairs of compiled regular expressions.
// Each pair consists of a package regex and a microbenchmark regex. If only one pattern is provided, it is used for the
// microbenchmark regex, and the package regex defaults to `.*`.
func GetRegexExclusionPairs(excludeList []string) [][]*regexp.Regexp {
	excludeRegexes := make([][]*regexp.Regexp, 0) // Initialize a slice to hold pairs of regexes.
	for _, pair := range excludeList {            // Iterate over the exclusion list.
		pairSplit := strings.Split(pair, ":") // Split each exclusion string by the colon (":").
		var pkgRegex, benchRegex *regexp.Regexp
		if len(pairSplit) != 2 {
			// If only one part is provided, use it for the microbenchmark regex, and default the package regex to ".*".
			pkgRegex = regexp.MustCompile(".*")
			benchRegex = regexp.MustCompile(pairSplit[0])
		} else {
			// Otherwise, use the two parts for the package and microbenchmark regexes.
			pkgRegex = regexp.MustCompile(pairSplit[0])
			benchRegex = regexp.MustCompile(pairSplit[1])
		}
		// Add the pair of regexes to the list.
		excludeRegexes = append(excludeRegexes, []*regexp.Regexp{pkgRegex, benchRegex})
	}
	return excludeRegexes
}

// InitRoachprod initializes the roachprod providers by calling InitProviders.
// This function sets up the environment for running roachprod commands.
func InitRoachprod() {
	_ = roachprod.InitProviders()
}

// RoachprodRun runs a command on a roachprod cluster with the given cluster name and logger.
// It takes a list of command arguments and passes them to the roachprod command execution.
func RoachprodRun(clusterName string, l *logger.Logger, cmdArray []string) error {
	// Execute the roachprod command with the provided context, logger, cluster name, and options.
	return roachprod.Run(
		context.Background(), l, clusterName, "", "", false,
		os.Stdout, os.Stderr, cmdArray, install.DefaultRunOptions(),
	)
}

// InitLogger initializes and returns a logger based on the provided log file path.
// If the logger configuration fails, the program prints an error and exits.
func InitLogger(path string) *logger.Logger {
	loggerCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr} // Create a logger config with standard output and error.
	var loggerError error
	l, loggerError := loggerCfg.NewLogger(path) // Create a new logger based on the configuration.
	if loggerError != nil {
		// If there is an error initializing the logger, print the error message and exit the program.
		_, _ = fmt.Fprintf(os.Stderr, "unable to configure logger: %s\n", loggerError)
		os.Exit(1)
	}
	return l // Return the initialized logger.
}
