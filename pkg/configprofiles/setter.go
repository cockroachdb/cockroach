// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package configprofiles

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// profileSetter is an implementation of pflag.Value
// which is able to define a configuration provider
// from a command-line flag (or env var).
type profileSetter struct {
	profileName  string
	taskProvider *acprovider.Provider
}

// NewProfileSetter initializes a profile setter able to assign the
// given Provider reference.
func NewProfileSetter(provider *acprovider.Provider) pflag.Value {
	return &profileSetter{
		profileName:  defaultProfileName,
		taskProvider: provider,
	}
}

// String implements the pflag.Value interface.
func (ps *profileSetter) String() string {
	return ps.profileName
}

// Type implements the pflag.Value interface.
func (ps *profileSetter) Type() string { return "<config profile>" }

// Set implements the pflag.Value interface.
func (ps *profileSetter) Set(v string) error {
	if a, ok := aliases[v]; ok {
		v = a.aliasTarget
	}
	p, ok := staticProfiles[v]
	if !ok {
		return errors.Newf("unknown profile: %q\n%s", v, profileHelp)
	}
	ps.profileName = v

	// Add a pseudo-task with the maximum possible task ID, to ensure no
	// more task will ever be run by this profile. This makes it
	// possible to add new tasks to an existing config profile to apply
	// to new clusters, without impacting clusters created in previous
	// versions with the same profile name.
	tasks := append(p.tasks, endProfileTask)

	*ps.taskProvider = &profileTaskProvider{
		// The envID defines the idempotency key for the application of
		// configuration profiles on new and existing clusters.
		//
		// This string (and corresponding UUID) key must never change for
		// the static config profiles, so that the profiles never get
		// applied more than once after a cluster is initialized.
		envID: "staticprofile:c623b0e0-633d-4014-9eaf-d13d0a15d2a1",
		tasks: tasks,
	}
	return nil
}

// endProfileTask is an empty task which terminates the sequence of
// tasks in a static profile. It uses MaxUint64 as task ID to ensure
// that no more tasks will ever execute on a given cluster after the
// profile has been applied once.
var endProfileTask = autoconfigpb.Task{
	TaskID:      autoconfigpb.TaskID(math.MaxUint64),
	Description: "end of configuration profile",
	MinVersion:  clusterversion.ByKey(clusterversion.BinaryVersionKey),
	Payload: &autoconfigpb.Task_SimpleSQL{
		SimpleSQL: &autoconfigpb.SimpleSQL{},
	},
}

// profileHelp is a help text that documents available profiles.
// It also checks that aliases point to valid profiles.
var profileHelp = func() string {
	allNames := make([]string, 0, len(staticProfiles)+len(aliases))
	for name := range staticProfiles {
		allNames = append(allNames, name)
	}
	for name, a := range aliases {
		if _, ok := staticProfiles[name]; ok {
			panic(errors.AssertionFailedf("alias name duplicates a profile name: %q", name))
		}
		if _, ok := staticProfiles[a.aliasTarget]; !ok {
			panic(errors.AssertionFailedf("alias %q refers to non-existent profile %q", name, a.aliasTarget))
		}
		if a.hidden {
			continue
		}
		allNames = append(allNames, name)
	}
	sort.Strings(allNames)

	var buf strings.Builder
	w := tabwriter.NewWriter(&buf, 2 /* minwidth */, 1 /* tabwidth */, 2 /* padding */, ' ', 0)
	fmt.Fprintln(w, "Available profiles:")
	for _, name := range allNames {
		if a, ok := aliases[name]; ok {
			fmt.Fprintf(w, "%s\t%s (alias for %q)\n", name, a.description, a.aliasTarget)
			continue
		}
		p := staticProfiles[name]
		fmt.Fprintf(w, "%s\t%s\n", name, p.description)
	}
	if err := w.Flush(); err != nil {
		panic(err)
	}
	return buf.String()
}()
