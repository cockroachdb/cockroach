// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pyroscope

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/pyroscope/config"
	"github.com/cockroachdb/errors"
)

var (
	//go:embed templates/alloy.config
	alloyConfigTemplate string

	//go:embed templates/docker-compose.yml
	dockerComposeTemplate string

	//go:embed templates/pyroscope-config.yml
	pyroscopeConfigTemplate string

	//go:embed templates/pyroscope-datasource.yml
	pyroscopeDatasourceTemplate string
)

const pyroscopeDir = "pyroscope-stack"

// Start deploys and starts the Pyroscope profiling stack using docker compose on the
// specified cluster.
func Start(ctx context.Context, l *logger.Logger, c *install.SyncedCluster) error {
	l.Printf("installing docker")
	err := install.Install(ctx, l, c, []string{"docker"})
	if err != nil {
		return errors.Wrap(err, "failed to install docker")
	}

	grafanaDatasourceDir := fmt.Sprintf("%s/grafana-provisioning/datasources", pyroscopeDir)
	err = c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(c.Nodes), "create directories",
		fmt.Sprintf(`mkdir -p %[1]s/alloy %[1]s/config %[2]s %[1]s/pyroscope-data && \
chmod 777 %[1]s/pyroscope-data`, pyroscopeDir, grafanaDatasourceDir))
	if err != nil {
		return err
	}
	configFiles := map[string]string{
		fmt.Sprintf("%s/alloy/config.alloy", pyroscopeDir):               alloyConfigTemplate,
		fmt.Sprintf("%s/pyroscope-datasource.yml", grafanaDatasourceDir): pyroscopeDatasourceTemplate,
		fmt.Sprintf("%s/config/pyroscope.yml", pyroscopeDir):             pyroscopeConfigTemplate,
		fmt.Sprintf("%s/docker-compose.yml", pyroscopeDir):               dockerComposeTemplate,
	}

	for configPath, data := range configFiles {
		err = c.PutString(ctx, l, c.Nodes, data, configPath, 0644)
		if err != nil {
			return err
		}
	}

	l.Printf("starting pyroscope docker stack")
	err = c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(c.Nodes), "start docker",
		fmt.Sprintf("cd %s && sudo docker compose up -d", pyroscopeDir))
	if err != nil {
		return err
	}
	l.Printf("Grafana/Pyroscope is now running on http://%s:3000", c.Host(c.Nodes[0]))
	return nil
}

// retrieveAlloyConfig fetches and parses the Alloy configuration from the Pyroscope cluster.
func retrieveAlloyConfig(
	ctx context.Context, l *logger.Logger, pyroscopeCluster *install.SyncedCluster,
) (*config.AlloyConfig, error) {
	alloyConfigStr, err := pyroscopeCluster.GetString(ctx, l, pyroscopeCluster.Nodes[0],
		fmt.Sprintf("%s/alloy/config.alloy", pyroscopeDir))
	if err != nil {
		return nil, err
	}
	alloyConfig := &config.AlloyConfig{}
	err = config.Decode(strings.NewReader(alloyConfigStr), alloyConfig)
	if err != nil {
		return nil, err
	}
	return alloyConfig, nil
}

// updateAlloyConfig writes the Alloy configuration to the Pyroscope cluster and
// reloads the Alloy config via a signal.
func updateAlloyConfig(
	ctx context.Context,
	l *logger.Logger,
	pyroscopeCluster *install.SyncedCluster,
	cfg *config.AlloyConfig,
) error {
	var buf strings.Builder
	err := config.Encode(&buf, cfg)
	if err != nil {
		return errors.Wrap(err, "failed to encode alloy config")
	}

	alloyConfigPath := fmt.Sprintf("%s/alloy/config.alloy", pyroscopeDir)
	err = pyroscopeCluster.PutString(ctx, l, pyroscopeCluster.Nodes, buf.String(), alloyConfigPath, 0644)
	if err != nil {
		return errors.Wrap(err, "failed to write updated alloy config")
	}

	err = pyroscopeCluster.Run(ctx, l, l.Stdout, l.Stderr,
		install.WithNodes(pyroscopeCluster.Nodes).WithDisplay("reloading config"), "sighup alloy",
		fmt.Sprintf("cd %s && sudo docker compose kill --signal=SIGHUP alloy", pyroscopeDir))
	return err
}

// parseAuthCookie parses the authentication cookie from the output of
// "cockroach auth-session login" command.
func parseAuthCookie(authOutput string) (string, error) {
	sessionStart := strings.Index(authOutput, "session=")
	if sessionStart == -1 {
		return "", errors.Newf("failed to find 'session=' in output: %s", authOutput)
	}
	httpOnlyIdx := strings.Index(authOutput[sessionStart:], "HttpOnly")
	if httpOnlyIdx == -1 {
		return "", errors.Newf("failed to find 'HttpOnly' in output: %s", authOutput)
	}
	// Extract from "session=" to the end of "HttpOnly".
	cookie := authOutput[sessionStart : sessionStart+httpOnlyIdx+len("HttpOnly")]
	return cookie, nil
}

// InitTarget initializes or re-initializes a target cluster for Pyroscope profiling.
// This function sets up the Alloy configuration on the Pyroscope cluster to scrape profiles
// from the target cluster. If the target cluster is secure, it will create an authentication
// session for accessing the profiling endpoints. This function can be called multiple times
// to update the configuration (e.g., if security settings change).
func InitTarget(
	ctx context.Context,
	l *logger.Logger,
	pyroscopeCluster *install.SyncedCluster,
	targetCluster *install.SyncedCluster,
) error {
	cfg, err := retrieveAlloyConfig(ctx, l, pyroscopeCluster)
	if err != nil {
		return err
	}

	// If the cluster is secure, we need to acquire an auth session.
	secureCookie := ""
	if targetCluster.Secure {
		// Create an auth session that lasts for a very long time
		cmd := fmt.Sprintf(`./%s auth-session login %s --certs-dir=%q --expire-after=9999h`,
			targetCluster.Binary,
			install.DefaultUser,
			install.CockroachNodeCertsDir,
		)

		var stdout bytes.Buffer
		err = targetCluster.Run(ctx, l, &stdout, l.Stderr, install.WithNodes(targetCluster.Nodes[:1]), "create auth session", cmd)
		if err != nil {
			return errors.Wrap(err, "failed to create auth session")
		}

		// Extract the session cookie from the output
		secureCookie, err = parseAuthCookie(stdout.String())
		if err != nil {
			return errors.Wrap(err, "failed to parse authentication cookie")
		}
	}

	cfg.UpdateTarget(targetCluster.Name, secureCookie)
	return updateAlloyConfig(ctx, l, pyroscopeCluster, cfg)
}

// AddNodes adds the specified target cluster nodes to the Pyroscope scrape target.
func AddNodes(
	ctx context.Context,
	l *logger.Logger,
	pyroscopeCluster *install.SyncedCluster,
	targetCluster *install.SyncedCluster,
) error {
	cfg, err := retrieveAlloyConfig(ctx, l, pyroscopeCluster)
	if err != nil {
		return err
	}
	if validateErr := validateTarget(cfg, pyroscopeCluster, targetCluster); validateErr != nil {
		return validateErr
	}

	services, err := targetCluster.ServiceDescriptors(ctx, targetCluster.Nodes,
		install.SystemInterfaceName, install.ServiceTypeUI, 0)
	if err != nil {
		return errors.Wrap(err, "failed to get service descriptors")
	}

	// Build a map of node to ip:port addresses
	addresses := make(map[install.Node]string)
	for _, service := range services {
		ip, ipErr := targetCluster.GetInternalIP(service.Node)
		if ipErr != nil {
			return errors.Wrapf(ipErr, "failed to get internal IP for node %d", service.Node)
		}
		addresses[service.Node] = fmt.Sprintf("%s:%d", ip, service.Port)
	}

	err = cfg.AddNodes(targetCluster.Name, addresses)
	if err != nil {
		return err
	}

	err = updateAlloyConfig(ctx, l, pyroscopeCluster, cfg)
	if err != nil {
		return err
	}

	logScrapedNodes(l, cfg, targetCluster.Name)
	return nil
}

// RemoveNodes removes the specified target cluster nodes from the Pyroscope
// scrape target.
func RemoveNodes(
	ctx context.Context,
	l *logger.Logger,
	pyroscopeCluster *install.SyncedCluster,
	targetCluster *install.SyncedCluster,
) error {
	cfg, err := retrieveAlloyConfig(ctx, l, pyroscopeCluster)
	if err != nil {
		return err
	}
	if validateErr := validateTarget(cfg, pyroscopeCluster, targetCluster); validateErr != nil {
		return validateErr
	}

	err = cfg.RemoveNodes(targetCluster.Name, targetCluster.Nodes)
	if err != nil {
		return err
	}

	err = updateAlloyConfig(ctx, l, pyroscopeCluster, cfg)
	if err != nil {
		return err
	}

	logScrapedNodes(l, cfg, targetCluster.Name)
	return nil
}

// logScrapedNodes logs a formatted list of nodes currently being scraped.
func logScrapedNodes(l *logger.Logger, cfg *config.AlloyConfig, clusterName string) {
	nodeSet, err := cfg.GetNodes(clusterName)
	if err != nil {
		l.Printf("Warning: could not retrieve node list: %v", err)
		return
	}
	if len(nodeSet) == 0 {
		l.Printf("No nodes are currently being scraped for cluster '%s'", clusterName)
		return
	}

	// Convert set to int slice for sorting.
	nodes := make([]int, 0, len(nodeSet))
	for node := range nodeSet {
		nodes = append(nodes, int(node))
	}
	sort.Ints(nodes)

	var nodeStrs []string
	for _, node := range nodes {
		nodeStrs = append(nodeStrs, fmt.Sprintf("%d", node))
	}

	l.Printf("Nodes currently being scraped for cluster %q: [%s]",
		clusterName, strings.Join(nodeStrs, ", "))
}

// ListNodes displays the nodes currently being scraped for a target cluster.
func ListNodes(
	ctx context.Context,
	l *logger.Logger,
	pyroscopeCluster *install.SyncedCluster,
	targetCluster *install.SyncedCluster,
) error {
	cfg, err := retrieveAlloyConfig(ctx, l, pyroscopeCluster)
	if err != nil {
		return err
	}
	if validateErr := validateTarget(cfg, pyroscopeCluster, targetCluster); validateErr != nil {
		return validateErr
	}

	logScrapedNodes(l, cfg, targetCluster.Name)
	return nil
}

// Stop tears down the Pyroscope profiling stack and removes all containers and
// volumes.
func Stop(ctx context.Context, l *logger.Logger, c *install.SyncedCluster) error {
	l.Printf("destroying pyroscope docker stack")
	err := c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(c.Nodes), "destroy docker",
		fmt.Sprintf("cd %s && sudo docker compose down -v", pyroscopeDir))
	if err != nil {
		return err
	}
	return nil
}

// validateTarget validates that the target cluster is configured in the
// pyroscope cluster.
func validateTarget(
	cfg *config.AlloyConfig,
	pyroscopeCluster *install.SyncedCluster,
	targetCluster *install.SyncedCluster,
) error {
	if !cfg.ContainsTarget(targetCluster.Name) {
		return errors.Errorf(
			"Target cluster `%s` is not configured in pyroscope cluster `%s`.\n"+
				"Please run the `init-target` command to initialize this target.",
			targetCluster.Name, pyroscopeCluster.Name,
		)
	}
	return nil
}
