// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachprod

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/cockroachdb/errors"
	"golang.org/x/term"
)

// userClusterNameRegexp returns a regexp that matches all clusters owned by the
// current user.
func userClusterNameRegexp() (*regexp.Regexp, error) {
	// In general, we expect that users will have the same
	// account name across the services they're using,
	// but we still want to function even if this is not
	// the case.
	seenAccounts := map[string]bool{}
	accounts, err := vm.FindActiveAccounts()
	if err != nil {
		return nil, err
	}
	pattern := ""
	for _, account := range accounts {
		if !seenAccounts[account] {
			seenAccounts[account] = true
			if len(pattern) > 0 {
				pattern += "|"
			}
			pattern += fmt.Sprintf("(^%s-)", regexp.QuoteMeta(account))
		}
	}
	return regexp.Compile(pattern)
}

func sortedClusters() []string {
	var r []string
	for n := range install.Clusters {
		r = append(r, n)
	}
	sort.Strings(r)
	return r
}

// NewClustersOpts has the options used to create a new cluster.
type NewClusterOpts struct {
	Name, Tag, CertsDir        string
	Secure, Quiet, UseTreeDist bool
	NodeArgs, NodeEnv          []string
	NumRacks, MaxConcurrency   int
}

func newCluster(opts NewClusterOpts) (*install.SyncedCluster, error) {
	nodeNames := "all"
	{
		parts := strings.Split(opts.Name, ":")
		switch len(parts) {
		case 2:
			nodeNames = parts[1]
			fallthrough
		case 1:
			opts.Name = parts[0]
		case 0:
			return nil, fmt.Errorf("no cluster specified")
		default:
			return nil, fmt.Errorf("invalid cluster name: %s", opts.Name)
		}
	}

	c, ok := install.Clusters[opts.Name]
	if !ok {
		err := errors.Newf(`unknown cluster: %s`, opts.Name)
		err = errors.WithHintf(err, `
Available clusters:
  %s
`, strings.Join(sortedClusters(), "\n  "))
		err = errors.WithHint(err, `Use "roachprod sync" to update the list of available clusters.`)
		return nil, err
	}

	c.Impl = install.Cockroach{}
	if opts.NumRacks > 0 {
		for i := range c.Localities {
			rack := fmt.Sprintf("rack=%d", i%opts.NumRacks)
			if c.Localities[i] != "" {
				rack = "," + rack
			}
			c.Localities[i] += rack
		}
	}

	nodes, err := install.ListNodes(nodeNames, len(c.VMs))
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		if n > len(c.VMs) {
			return nil, fmt.Errorf("invalid node spec %s, cluster contains %d nodes",
				nodeNames, len(c.VMs))
		}
	}
	c.Nodes = nodes
	c.Secure = opts.Secure
	c.CertsDir = opts.CertsDir
	c.Env = opts.NodeEnv
	c.Args = opts.NodeArgs
	if opts.Tag != "" {
		c.Tag = "/" + opts.Tag
	}
	c.UseTreeDist = opts.UseTreeDist
	c.Quiet = opts.Quiet || !term.IsTerminal(int(os.Stdout.Fd()))
	c.MaxConcurrency = opts.MaxConcurrency
	return c, nil
}

// type LogsOpts struct {
// 	LogsDir, Dest, LogsFilter, LogsProgramFilter string
// 	LogsInterval                                 time.Duration
// 	LogsFrom, LogsTo                             time.Time
// 	Out                                          io.Writer
// }

// verifyClusterName ensures that the given name conforms to
// our naming pattern of "<username>-<clustername>". The
// username must match one of the vm.Provider account names
// or the --username override.
func verifyClusterName(clusterName, username string) (string, error) {
	if len(clusterName) == 0 {
		return "", fmt.Errorf("cluster name cannot be blank")
	}
	if clusterName == config.Local {
		return clusterName, nil
	}

	alphaNum, err := regexp.Compile(`^[a-zA-Z0-9\-]+$`)
	if err != nil {
		return "", err
	}
	if !alphaNum.MatchString(clusterName) {
		return "", errors.Errorf("cluster name must match %s", alphaNum.String())
	}

	// Use the vm.Provider account names, or --username.
	var accounts []string
	if len(username) > 0 {
		accounts = []string{username}
	} else {
		seenAccounts := map[string]bool{}
		active, err := vm.FindActiveAccounts()
		if err != nil {
			return "", err
		}
		for _, account := range active {
			if !seenAccounts[account] {
				seenAccounts[account] = true
				cleanAccount := vm.DNSSafeAccount(account)
				if cleanAccount != account {
					log.Printf("WARN: using `%s' as username instead of `%s'", cleanAccount, account)
				}
				accounts = append(accounts, cleanAccount)
			}
		}
	}

	// If we see <account>-<something>, accept it.
	for _, account := range accounts {
		if strings.HasPrefix(clusterName, account+"-") && len(clusterName) > len(account)+1 {
			return clusterName, nil
		}
	}

	// Try to pick out a reasonable cluster name from the input.
	i := strings.Index(clusterName, "-")
	suffix := clusterName
	if i != -1 {
		// The user specified a username prefix, but it didn't match an active
		// account name. For example, assuming the account is "peter", `roachprod
		// create joe-perf` should be specified as `roachprod create joe-perf -u
		// joe`.
		suffix = clusterName[i+1:]
	} else {
		// The user didn't specify a username prefix. For example, assuming the
		// account is "peter", `roachprod create perf` should be specified as
		// `roachprod create peter-perf`.
		_ = 0
	}

	// Suggest acceptable cluster names.
	var suggestions []string
	for _, account := range accounts {
		suggestions = append(suggestions, fmt.Sprintf("%s-%s", account, suffix))
	}
	return "", fmt.Errorf("malformed cluster name %s, did you mean one of %s",
		clusterName, suggestions)
}
