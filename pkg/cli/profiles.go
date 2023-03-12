package cli

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/errors"
)

type profileSetter struct {
	profileName  string
	taskProvider *acprovider.Provider
}

// String implements the pflag.Value interface.
func (p *profileSetter) String() string {
	return p.profileName
}

// Type implements the pflag.Value interface.
func (p *profileSetter) Type() string { return "<config profile>" }

// Set implements the pflag.Value interface.
func (p *profileSetter) Set(v string) error {
	tasks, ok := initProfiles[v]
	if !ok {
		return errors.Newf("unknown profile: %q", v)
	}
	*p.taskProvider = &profileTaskProvider{tasks: tasks}
	return nil
}

type profileTaskProvider struct {
	tasks []autoconfigpb.Task
}

var _ acprovider.Provider = (*profileTaskProvider)(nil)

// RegisterTasksChannel is part of the acprovider.Provider interface.
func (p *profileTaskProvider) RegisterTasksChannel() <-chan struct{} {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return ch
}

func (p *profileTaskProvider) ReportLastKnownCompletedTaskID(uint64) {}
func (p *profileTaskProvider) GetTasks() []autoconfigpb.Task         { return p.tasks }

var initProfiles = map[string][]autoconfigpb.Task{
	"default": nil,
	"example": {
		{
			TaskID:      1,
			Description: "example database",
			MinVersion:  clusterversion.ByKey(clusterversion.BinaryVersionKey),
			Payload: &autoconfigpb.Task_SimpleSQL{
				SimpleSQL: &autoconfigpb.SimpleSQL{
					Statements: []string{
						"CREATE DATABASE IF NOT EXISTS example",
						"CREATE TABLE example.data AS SELECT 'hello' AS value",
					},
				},
			},
		},
	},
}
