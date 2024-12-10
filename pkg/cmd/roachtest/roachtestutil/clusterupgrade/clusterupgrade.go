// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterupgrade

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
)

var (
	TestBuildVersion *version.Version

	currentBranch = os.Getenv("TC_BUILD_BRANCH")

	// CurrentVersionString is how we represent the binary or cluster
	// versions associated with the current binary (the one being
	// tested). Note that, in TeamCity, we use the branch name to make
	// it even clearer.
	CurrentVersionString = "<current>"
)

// Version is a thin wrapper around the `version.Version` struct that
// provides convenient utility function to pretty print versions and
// check whether this is the current version being tested.
type Version struct {
	version.Version
}

// String returns the string representation of this version. For
// convenience, if this version represents the current version being
// tested, we print the branch name being tested if the test is
// running on TeamCity, to make it clearer (instead of "<current>").
func (v *Version) String() string {
	if v.IsCurrent() {
		if currentBranch != "" {
			return currentBranch
		}

		return CurrentVersionString
	}

	return v.Version.String()
}

// IsCurrent returns whether this version corresponds to the current
// version being tested.
func (v *Version) IsCurrent() bool {
	return v.Equal(CurrentVersion())
}

// Equal compares the two versions, returning whether they represent
// the same version.
func (v *Version) Equal(other *Version) bool {
	return v.Version.Compare(&other.Version) == 0
}

// AtLeast is a thin wrapper around `(*version.Version).AtLeast`,
// allowing two `Version` objects to be compared directly.
func (v *Version) AtLeast(other *Version) bool {
	return v.Version.AtLeast(&other.Version)
}

// Series returns the release series this version is a part of.
func (v *Version) Series() string {
	return release.VersionSeries(&v.Version)
}

// CurrentVersion returns the version associated with the current
// build.
func CurrentVersion() *Version {
	if TestBuildVersion != nil {
		return &Version{*TestBuildVersion} // test-only
	}

	return &Version{*version.MustParse(build.BinaryVersion())}
}

// MustParseVersion parses the version string given (with or without
// leading 'v') and returns the corresponding `Version` object.
func MustParseVersion(v string) *Version {
	parsedVersion, err := ParseVersion(v)
	if err != nil {
		panic(err)
	}
	return parsedVersion
}

// ParseVersion parses the version string given (with or without
// leading 'v') and returns the corresponding `Version` object. Returns
// an error if the version string is not valid.
func ParseVersion(v string) (*Version, error) {
	// The current version is rendered differently (see String()
	// implementation). If the user passed that string representation,
	// return the current version object.
	if currentVersion := CurrentVersion(); v == currentVersion.String() {
		return currentVersion, nil
	}

	versionStr := v
	if !strings.HasPrefix(v, "v") {
		versionStr = "v" + v
	}

	parsedVersion, err := version.Parse(versionStr)
	if err != nil {
		return nil, err
	}

	return &Version{*parsedVersion}, nil
}

// LatestPatchRelease returns the latest patch release version for a given
// release series.
func LatestPatchRelease(series string) (*Version, error) {
	seriesStr := strings.TrimPrefix(series, "v")
	versionStr, err := release.LatestPatch(seriesStr)
	if err != nil {
		return nil, err
	}

	// release.LatestPatch uses mustParseVersion internally, so the returned
	// version is guaranteed to be valid.
	return MustParseVersion(versionStr), nil
}

// BinaryVersion returns the binary version running on the node
// associated with the given database connection.
// NB: version means major.minor[-internal]; the patch level isn't
// returned. For example, a binary of version 19.2.4 will return 19.2.
func BinaryVersion(ctx context.Context, db *gosql.DB) (roachpb.Version, error) {
	zero := roachpb.Version{}
	var sv string
	if err := db.QueryRowContext(ctx, `SELECT crdb_internal.node_executable_version();`).Scan(&sv); err != nil {
		return zero, err
	}

	if len(sv) == 0 {
		return zero, fmt.Errorf("empty version")
	}

	return roachpb.ParseVersion(sv)
}

// ClusterVersion returns the cluster version active on the node
// associated with the given database connection. Note that the
// returned value might become stale due to the cluster auto-upgrading
// in the background plus gossip asynchronicity.
// NB: cluster versions are always major.minor[-internal]; there isn't
// a patch level.
func ClusterVersion(ctx context.Context, db *gosql.DB) (roachpb.Version, error) {
	zero := roachpb.Version{}
	var sv string
	if err := db.QueryRowContext(ctx, `SHOW CLUSTER SETTING version`).Scan(&sv); err != nil {
		return zero, err
	}

	return roachpb.ParseVersion(sv)
}

// UploadCockroach stages the cockroach binary in the nodes.
// Convenience function, see `uploadBinaryVersion` for more details.
func UploadCockroach(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	c cluster.Cluster,
	nodes option.NodeListOption,
	v *Version,
) (string, error) {
	// Short-circuit this special case to avoid the extra SSH
	// connections: the current version is always uploaded to every node
	// in the cluster in a fixed location.
	if v.IsCurrent() {
		return test.DefaultCockroachPath, nil
	}

	return uploadBinaryVersion(ctx, t, l, "cockroach", c, nodes, v)
}

// UploadWorkload stages the workload binary in the nodes.
// Convenience function, see `uploadBinaryVersion` for more details.
// The boolean return value indicates whether a workload binary was
// uploaded to the nodes; a `false` value indicates that the version
// passed is too old and no binary is available.
func UploadWorkload(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	c cluster.Cluster,
	nodes option.NodeListOption,
	v *Version,
) (string, bool, error) {
	// minWorkloadBinaryVersion is the minimum version for which we have
	// `workload` binaries available.
	var minWorkloadBinaryVersion *Version
	switch c.Architecture() {
	case vm.ArchARM64:
		minWorkloadBinaryVersion = MustParseVersion("v23.2.0")
	default:
		minWorkloadBinaryVersion = MustParseVersion("v22.2.0")
	}

	// If we are uploading the `current` version, skip version checking,
	// as the binary used is the one passed via command line flags.
	if !v.IsCurrent() && !v.AtLeast(minWorkloadBinaryVersion) {
		return "", false, nil
	}

	path, err := uploadBinaryVersion(ctx, t, l, "workload", c, nodes, v)
	return path, err == nil, err
}

// uploadBinaryVersion uploads the specified binary associated with
// the given version to the given nodes. It returns the path of the
// uploaded binaries on the nodes.
func uploadBinaryVersion(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	binary string,
	c cluster.Cluster,
	nodes option.NodeListOption,
	v *Version,
) (string, error) {
	dstBinary := BinaryPathForVersion(t, v, binary)
	var defaultBinary string
	var isOverridden bool
	switch binary {
	case "cockroach":
		defaultBinary, isOverridden = t.VersionsBinaryOverride()[v.String()]
		if isOverridden {
			l.Printf("using cockroach binary override for version %s: %s", v, defaultBinary)
		} else {
			// Run with standard binary as older versions retrieved through roachprod stage
			// are not currently available with crdb_test enabled.
			// TODO(DarrylWong): Compile older versions with crdb_test flag.
			defaultBinary = t.StandardCockroach()
		}
	case "workload":
		defaultBinary = t.DeprecatedWorkload()
	default:
		return "", fmt.Errorf("unknown binary name: %s", binary)
	}

	if isOverridden {
		if err := c.PutE(ctx, l, defaultBinary, dstBinary, nodes); err != nil {
			return "", err
		}
	} else {
		dir := filepath.Dir(dstBinary)
		// Avoid staging the binary if it already exists.
		if err := c.RunE(ctx, option.WithNodes(nodes), "test -e", dstBinary); err == nil {
			return dstBinary, nil
		}

		// Ensure binary directory exists.
		if err := c.RunE(ctx, option.WithNodes(nodes), "mkdir -p", dir); err != nil {
			return "", err
		}

		var application, stageVersion string
		switch binary {
		case "cockroach":
			application = "release"
			stageVersion = v.String()
		case "workload":
			application = "workload"
			// For workload binaries, we do not have a convenient way to get
			// a build for a specific release. Instead, we stage the binary
			// for the corresponding release branch, which is good enough in
			// most cases.
			stageVersion = fmt.Sprintf("release-%d.%d", v.Major(), v.Minor())
		}

		if err := c.Stage(ctx, l, application, stageVersion, dir, nodes); err != nil {
			return "", err
		}
	}

	return dstBinary, nil
}

// InstallFixtures copies the previously created fixtures (in
// pkg/cmd/roachtest/fixtures) for the given version to the nodes
// passed. After this step, the corresponding binary can be started on
// the cluster and it will use that store directory.
func InstallFixtures(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, nodes option.NodeListOption, v *Version,
) error {
	if err := c.RunE(ctx, option.WithNodes(nodes), "mkdir -p {store-dir}"); err != nil {
		return fmt.Errorf("creating store-dir: %w", err)
	}

	// The fixtures use cluster version (major.minor) but the input might be
	// a patch release.
	name := CheckpointName(
		roachpb.Version{Major: int32(v.Major()), Minor: int32(v.Minor())}.String(),
	)
	for n := 1; n <= len(nodes); n++ {
		if err := c.PutE(ctx, l,
			"pkg/cmd/roachtest/fixtures/"+strconv.Itoa(n)+"/"+name+".tgz",
			"{store-dir}/fixture.tgz", c.Node(nodes[n-1]),
		); err != nil {
			return err
		}
	}
	// Extract fixture. Fail if there's already an LSM in the store dir.
	if err := c.RunE(ctx, option.WithNodes(nodes), "ls {store-dir}/marker.* 1> /dev/null 2>&1 && exit 1 || (cd {store-dir} && tar -xf fixture.tgz)"); err != nil {
		return fmt.Errorf("extracting fixtures: %w", err)
	}

	return nil
}

// StartWithSettings starts cockroach and constructs settings according
// to the setting options passed.
func StartWithSettings(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	nodes option.NodeListOption,
	startOpts option.StartOpts,
	opts ...install.ClusterSettingOption,
) error {
	settings := install.MakeClusterSettings(opts...)
	return c.StartE(ctx, l, startOpts, settings, nodes)
}

func CockroachPathForVersion(t test.Test, v *Version) string {
	return BinaryPathForVersion(t, v, "cockroach")
}

func WorkloadPathForVersion(t test.Test, v *Version) string {
	return BinaryPathForVersion(t, v, "workload")
}

// BinaryPathForVersion shows where a certain binary (typically
// `cockroach`, `workload`) for the given version is expected to be
// found on roachprod nodes. The file will only actually exist if
// there was a previous call to `Upload*` with the same version
// parameter.
func BinaryPathForVersion(t test.Test, v *Version, binary string) string {
	if v.IsCurrent() {
		if binary == "cockroach" {
			return test.DefaultCockroachPath
		}
		return "./" + binary
	} else if _, ok := t.VersionsBinaryOverride()[v.String()]; ok && binary == "cockroach" {
		// If a cockroach override has been specified for `v`, use that binary.
		return "./cockroach-" + v.String()
	} else {
		return filepath.Join(v.String(), binary)
	}
}

// RestartNodesWithNewBinary uploads a given cockroach version to the
// nodes passed, and restarts the cockroach process.
func RestartNodesWithNewBinary(
	ctx context.Context,
	t test.Test,
	l *logger.Logger,
	c cluster.Cluster,
	nodes option.NodeListOption,
	startOpts option.StartOpts,
	newVersion *Version,
	settings ...install.ClusterSettingOption,
) error {
	const gracePeriod = 300 // 5 minutes

	// NB: We could technically stage the binary on all nodes before
	// restarting each one, but on Unix it's invalid to write to an
	// executable file while it is currently running. So we do the
	// simple thing and upload it serially instead.

	// Restart nodes in a random order; otherwise node 1 would be running all
	// the migrations and it probably also has all the leases.
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	// Stop the cockroach process gracefully in order to drain it properly.
	// This makes the upgrade closer to how users do it in production, but
	// it's also needed to eliminate flakiness. In particular, this will
	// make sure that DistSQL draining information is communicated through
	// gossip so that other nodes running an older version don't consider
	// this upgraded node for DistSQL plans (see #87154 for more details).
	stopOptions := []option.StartStopOption{option.Graceful(gracePeriod)}

	// If we are starting the cockroach process with a tag, we apply the
	// same tag when stopping.
	for _, s := range settings {
		if t, ok := s.(install.TagOption); ok {
			stopOptions = append(stopOptions, option.Tag(string(t)))
		}
	}

	for _, node := range nodes {
		l.Printf("restarting node %d into version %s", node, newVersion.String())
		if err := c.StopE(ctx, l, option.NewStopOpts(stopOptions...), c.Node(node)); err != nil {
			return err
		}

		binary, err := UploadCockroach(ctx, t, l, c, c.Node(node), newVersion)
		if err != nil {
			return err
		}
		// Never run init steps when restarting -- these should already
		// have happened by the time the cluster was first bootstrapped
		// and trying to run them again just adds noise to the logs.
		startOpts.RoachprodOpts.SkipInit = true
		if err := StartWithSettings(
			ctx, l, c, c.Node(node), startOpts, append(settings, install.BinaryOption(binary))...,
		); err != nil {
			return err
		}

		// We have seen cases where a transient error could occur when this
		// newly upgraded node serves as a gateway for a distributed query due
		// to remote nodes not being able to dial back to the gateway for some
		// reason (investigation of it is tracked in #87634). For now, we're
		// papering over these flakes by this sleep. For more context, see
		// #87104.
		// TODO(yuzefovich): remove this sleep once #87634 is fixed.
		time.Sleep(4 * time.Second)
	}

	return nil
}

// DefaultUpgradeTimeout is the default timeout used when waiting for
// an upgrade to finish (i.e., for all migrations to run and for the
// cluster version to propagate). This timeout should be sufficient
// for simple tests where there isn't a lot of data; in other
// situations, a custom timeout can be passed to
// `WaitForClusterUpgrade`.
var DefaultUpgradeTimeout = 10 * time.Minute

// WaitForClusterUpgrade waits for the cluster version to reach the
// first node's binary version. This function should only be called if
// every node in the cluster has been restarted to run the same binary
// version. We rely on the cluster's internal self-upgrading
// mechanism to update the underlying cluster version.
func WaitForClusterUpgrade(
	ctx context.Context,
	l *logger.Logger,
	nodes option.NodeListOption,
	dbFunc func(int) *gosql.DB,
	timeout time.Duration,
) error {
	firstNode := nodes[0]
	newVersion, err := BinaryVersion(ctx, dbFunc(firstNode))
	if err != nil {
		return err
	}

	// waitForUpgrade will wait for the given `node` to have the
	// expected cluster version within the given timeout.
	waitForUpgrade := func(node int, timeout time.Duration) error {
		var latestVersion roachpb.Version
		var opts retry.Options
		retryCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		err := opts.Do(retryCtx, func(ctx context.Context) error {
			currentVersion, err := ClusterVersion(ctx, dbFunc(node))
			if err != nil {
				return err
			}

			latestVersion = currentVersion
			if currentVersion != newVersion {
				return fmt.Errorf("not upgraded yet")
			}
			return nil
		})
		if err != nil {
			return errors.Wrapf(err,
				"timed out after %s: expected n%d to be at cluster version %s, but is still at %s",
				timeout, node, newVersion, latestVersion,
			)
		}
		l.Printf("%s: acked by n%d", newVersion, node)
		return nil
	}

	l.Printf("waiting for cluster to auto-upgrade to %s for %s", newVersion, timeout)
	if err := waitForUpgrade(firstNode, timeout); err != nil {
		return err
	}

	// Wait for `propagationTimeout` for all other nodes to also
	// acknowledge the same cluster version as the first node. This
	// should happen much faster, as migrations should already have
	// finished at this point.
	propagationTimeout := 3 * time.Minute
	for _, node := range nodes[1:] {
		if err := waitForUpgrade(node, propagationTimeout); err != nil {
			return fmt.Errorf("n%d is already at %s: %w", firstNode, newVersion, err)
		}
	}

	l.Printf("all nodes (%v) are upgraded to %s", nodes, newVersion)
	return nil
}

// CheckpointName returns the expected name of the checkpoint file
// under `pkg/cmd/roachtest/fixtures/{nodeID}` for the given binary
// version.
func CheckpointName(binaryVersion string) string {
	return "checkpoint-v" + binaryVersion
}
