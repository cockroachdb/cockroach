// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageconfig

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestStoreYAMLOutput(t *testing.T) {
	configs := map[string]Store{
		"basic": {
			Path: "/mnt/data1",
			Size: BytesSize(1024 * 1024 * 1024),
		},
		"mem": {
			Path:        "",
			Size:        BytesSize(1024 * 1024 * 1024),
			InMemory:    true,
			StickyVFSID: "foo",
		},
		"misc": {
			Path:            "/mnt/data1",
			BallastSize:     PercentSize(10),
			Attributes:      []string{"foo", "bar"},
			ProvisionedRate: ProvisionedRate{ProvisionedBandwidth: 1000 * 1024},
		},
		"pebble-opts": {
			Path: "/mnt/data1",
			PebbleOptions: `[Options]
l0_compaction_threshold=2
l0_stop_writes_threshold=10`,
		},
	}
	for _, c := range configs {
		testRoundtrip(t, c)
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "store_yaml"), func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "to-yaml":
			cfg, ok := configs[td.CmdArgs[0].Key]
			if !ok {
				t.Fatalf("unknown config %q", td.CmdArgs[0].Key)
			}
			out, err := yaml.Marshal(cfg)
			require.NoError(t, err)
			return string(out)

		case "from-yaml":
			s, err := unmarshal([]byte(td.Input))
			if err != nil {
				return err.Error()
			}
			out, err := yaml.Marshal(s)
			require.NoError(t, err)
			return string(out)

		case "validate":
			s, err := unmarshal([]byte(td.Input))
			require.NoError(t, err)
			if err := s.Validate(); err != nil {
				return err.Error()
			}
			return "ok"

		default:
			t.Fatalf("unknown command %q", td.Cmd)
		}
		return ""
	})
}

func testRoundtrip(t *testing.T, input Store) {
	marshaled, err := yaml.Marshal(input)
	require.NoError(t, err)

	parsed, err := unmarshal(marshaled)
	require.NoError(t, err, "marshaled:\n%s", marshaled)
	require.Equal(t, input, parsed, "roundtrip did not produce equal object; marshaled: \n%s", marshaled)
}

func TestStoreYAMLRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	for range 1000 {
		s := randStore(rng)
		testRoundtrip(t, s)
	}
}

func randStore(rng *rand.Rand) Store {
	var s Store
	if rng.IntN(2) == 0 {
		s.InMemory = true
		s.Size = randSize(rng, MinimumStoreSize, 100<<30, 1, 100)
		if rng.IntN(2) == 0 {
			s.StickyVFSID = "foo"
		}
	} else {
		if rng.IntN(2) == 0 {
			s.Size = randSize(rng, MinimumStoreSize, math.MaxInt64, 1, 100)
		}
		if rng.IntN(2) == 0 {
			s.BallastSize = randSize(rng, 0, 1<<20, 0, 10)
		}
	}

	if rng.IntN(2) == 0 {
		s.Attributes = make([]string, 1+rand.IntN(4))
		for i := range s.Attributes {
			s.Attributes[i] = fmt.Sprintf("%x", rng.Int64())
		}
	}

	if rng.IntN(2) == 0 {
		s.PebbleOptions = `[Options]
something=1
or_other=2

[Level "1"]
target_file_size=4096

[Level 2]
target_file_size=8192`
	}

	if rng.IntN(2) == 0 {
		s.ProvisionedRate.ProvisionedBandwidth = rng.Int64N(1 << 30)
	}

	// TODO(radu): encryption options.
	return s
}

func randSize(rng *rand.Rand, minBytes, maxBytes int64, minPercent, maxPercent float64) Size {
	switch rng.IntN(6) {
	case 0:
		return BytesSize(minBytes)
	case 1:
		return BytesSize(maxBytes)
	case 2:
		return BytesSize(minBytes + rand.Int64N(maxBytes+1-minBytes))
	case 3:
		return PercentSize(minPercent)
	case 4:
		return PercentSize(maxPercent)
	default:
		p := minPercent + rng.Float64()*(maxPercent-minPercent)
		// Round to 3 decimal places to ensure it will roundtrip.
		p = math.Round(p*1000) / 1000
		return PercentSize(p)
	}
}

func unmarshal(data []byte) (Store, error) {
	var s Store
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&s); err != nil {
		return Store{}, err
	}
	return s, nil
}
