package cli

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/errors"
)

type profileSetter struct {
	profileName string
	injected    *[]*jobspb.InjectedSQLDetails
}

// String implements the pflag.Value interface.
func (p *profileSetter) String() string {
	return p.profileName
}

// Type implements the pflag.Value interface.
func (p *profileSetter) Type() string { return "<config profile>" }

// Set implements the pflag.Value interface.
func (p *profileSetter) Set(v string) error {
	ij, ok := initProfiles[v]
	if !ok {
		return errors.Newf("unknown profile: %q", v)
	}
	res := make([]*jobspb.InjectedSQLDetails, len(ij))
	for i := range ij {
		res[i] = &ij[i]
	}
	*p.injected = res
	return nil
}

var initProfiles = map[string][]jobspb.InjectedSQLDetails{
	"default": nil,
	"foo": {
		{
			InjectionID: 123,
			Statements: []string{
				"CREATE TABLE IF NOT EXISTS system.foo(x INT)",
			},
		},
	},
}
