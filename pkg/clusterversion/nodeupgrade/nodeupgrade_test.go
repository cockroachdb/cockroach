package nodeupgrade

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createEnegine creates an engine for testing.
// It should be closed when it is no longer needed via the Close() method.
func createEngine(t *testing.T) engine.Engine {
	ctx := context.Background()
	eng := engine.NewInMem(
		ctx,
		engine.DefaultStorageEngine,
		roachpb.Attributes{},
		1<<20,
	)
	val := roachpb.Value{}
	val.SetInt(0)

	for _, key := range []roachpb.Key{
		keys.StoreNodeUpgradePendingVersionKey(),
		keys.StoreNodeUpgradeCurrentVersionKey(),
	} {
		err := engine.MVCCPut(
			ctx,
			eng,
			nil, /* stats */
			key,
			hlc.Timestamp{},
			val,
			nil, /* txn */
		)
		require.NoError(t, err)
	}
	return eng
}

// assertStatus ensures the status from serv matches the expected status.
func assertStatus(t *testing.T, serv *nodeUpgrader, expected []*StatusResponse_Engine) {
	statusResp, err := serv.Status(context.Background(), &StatusRequest{})
	require.NoError(t, err)
	assert.Nil(t, statusResp.Err)
	assert.Equal(t, statusResp, &StatusResponse{Engines: expected})
}

// fabricateBadEngineState makes the given engine into a bad state
// that trips out the defensive pendingVersion > currentVersion assert.
func fabricateBadEngineState(t *testing.T, eng engine.Engine) {
	val := roachpb.Value{}
	val.SetInt(5)
	err := engine.MVCCPut(
		context.Background(),
		eng,
		nil, /* stats */
		keys.StoreNodeUpgradeCurrentVersionKey(),
		hlc.Timestamp{},
		val,
		nil, /* txn */
	)
	require.NoError(t, err)
}

func TestNodeUpgraderSetTargetVersion(t *testing.T) {
	engs := []engine.Engine{
		createEngine(t),
		createEngine(t),
		createEngine(t),
	}
	for _, eng := range engs {
		defer eng.Close()
	}
	serv := &nodeUpgrader{engs: engs}
	ctx := context.Background()

	t.Run("test raising target version works", func(t *testing.T) {
		resp, err := serv.SetTargetVersion(ctx, &SetTargetVersionRequest{TargetVersion: 2})
		require.NoError(t, err)
		assert.Nil(t, resp.Err)
		assertStatus(t, serv, []*StatusResponse_Engine{
			&StatusResponse_Engine{CurrentVersion: 0, PendingVersion: 2},
			&StatusResponse_Engine{CurrentVersion: 0, PendingVersion: 2},
			&StatusResponse_Engine{CurrentVersion: 0, PendingVersion: 2},
		})
	})

	t.Run("test raising target to the the same version or earlier doesn't do anything", func(t *testing.T) {
		for _, version := range []Version{0, 1, 2} {
			resp, err := serv.SetTargetVersion(ctx, &SetTargetVersionRequest{TargetVersion: version})
			require.NoError(t, err)
			assert.Nil(t, resp.Err)
			assertStatus(t, serv, []*StatusResponse_Engine{
				&StatusResponse_Engine{CurrentVersion: 0, PendingVersion: 2},
				&StatusResponse_Engine{CurrentVersion: 0, PendingVersion: 2},
				&StatusResponse_Engine{CurrentVersion: 0, PendingVersion: 2},
			})
		}
	})

	t.Run("test defensive assert of CurrentVersion > PendingVersion", func(t *testing.T) {
		fabricateBadEngineState(t, serv.engs[0])
		resp, err := serv.SetTargetVersion(ctx, &SetTargetVersionRequest{TargetVersion: 3})
		require.NoError(t, err)
		assert.Regexp(t, "current version .* > pending version", errors.DecodeError(ctx, *resp.Err).Error())
	})
}

func TestRunPendingUpgrades(t *testing.T) {
	var hookCounterMutex syncutil.Mutex
	hookCounter := make(map[Version]int)
	makeUpgradeFn := func(i Version) func(ctx context.Context, eng engine.Engine) error {
		return func(ctx context.Context, eng engine.Engine) error {
			hookCounterMutex.Lock()
			hookCounter[i] += 1
			hookCounterMutex.Unlock()
			return nil
		}
	}
	engs := []engine.Engine{
		createEngine(t),
		createEngine(t),
		createEngine(t),
	}
	for _, eng := range engs {
		defer eng.Close()
	}
	serv := &nodeUpgrader{
		engs:     engs,
		registry: &upgradeRegistry{},
	}
	serv.registry.mu.versionToUpgradeFn = map[Version]UpgradeFn{
		Version(0): makeUpgradeFn(0),
		Version(1): makeUpgradeFn(1),
		Version(2): makeUpgradeFn(2),
	}
	ctx := context.Background()

	setTargetResp, err := serv.SetTargetVersion(ctx, &SetTargetVersionRequest{TargetVersion: 2})
	require.NoError(t, err)
	require.Nil(t, setTargetResp.Err)

	t.Run("test upgrade after target version set", func(t *testing.T) {
		resp, err := serv.RunPendingUpgrades(ctx, &RunPendingUpgradesRequest{})
		require.NoError(t, err)
		require.Nil(t, resp.Err)

		assertStatus(t, serv, []*StatusResponse_Engine{
			&StatusResponse_Engine{CurrentVersion: 2, PendingVersion: 2},
			&StatusResponse_Engine{CurrentVersion: 2, PendingVersion: 2},
			&StatusResponse_Engine{CurrentVersion: 2, PendingVersion: 2},
		})

		assert.Equal(
			t,
			map[Version]int{
				Version(1): 3,
				Version(2): 3,
			},
			hookCounter,
		)
	})

	t.Run("test RunPendingUpgrades idempotency", func(t *testing.T) {
		resp, err := serv.RunPendingUpgrades(ctx, &RunPendingUpgradesRequest{})
		require.NoError(t, err)
		require.Nil(t, resp.Err)

		assertStatus(t, serv, []*StatusResponse_Engine{
			&StatusResponse_Engine{CurrentVersion: 2, PendingVersion: 2},
			&StatusResponse_Engine{CurrentVersion: 2, PendingVersion: 2},
			&StatusResponse_Engine{CurrentVersion: 2, PendingVersion: 2},
		})

		assert.Equal(
			t,
			map[Version]int{
				Version(1): 3,
				Version(2): 3,
			},
			hookCounter,
		)
	})

	t.Run("test RunPendingUpgrades defensive assert", func(t *testing.T) {
		fabricateBadEngineState(t, serv.engs[0])
		resp, err := serv.RunPendingUpgrades(ctx, &RunPendingUpgradesRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp.Err)
		assert.Regexp(t, "current version .* > pending version", errors.DecodeError(ctx, *resp.Err).Error())
	})
}
