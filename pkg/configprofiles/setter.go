// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package configprofiles

import (
	"math"
	"strings"

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
	tasks, ok := staticProfiles[v]
	if !ok {
		return errors.Newf("unknown profile: %q\nAvailable profiles: %s",
			v, strings.Join(staticProfileNames, ", "))
	}

	// Add a pseudo-task with the maximum possible task ID, to ensure no
	// more task will ever be run by this profile. This makes it
	// possible to add new tasks to an existing config profile to apply
	// to new clusters, without impacting clusters created in previous
	// versions with the same profile name.
	tasks = append(tasks, endProfileTask)

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

var endProfileTask = autoconfigpb.Task{
	TaskID:      autoconfigpb.TaskID(math.MaxUint64),
	Description: "end of configuration profile",
	MinVersion:  clusterversion.ByKey(clusterversion.BinaryVersionKey),
	Payload: &autoconfigpb.Task_SimpleSQL{
		SimpleSQL: &autoconfigpb.SimpleSQL{},
	},
}
