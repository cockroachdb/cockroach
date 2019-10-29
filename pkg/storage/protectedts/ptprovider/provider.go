package ptprovider

import (
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/pttracker"
)

// Config configures the Provider.
type Config struct {
	Settings *cluster.Settings
	DB       *client.DB
}

type provider struct {
	*ptstorage.Provider
	*pttracker.Tracker
}

// New creates a new protectedts.Provider.
func New(c Config) protectedts.Provider {
	s := ptstorage.NewProvider(c.Settings)
	t := pttracker.New(c.Settings, c.DB, s)
	return &provider{
		Provider: s,
		Tracker:  t,
	}
}
