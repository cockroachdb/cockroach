// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

// SQLProxy is a helper to help spin up SQL proxy and a directory service for roachtests.
type SQLProxy struct {
	c             cluster.Cluster
	l             *logger.Logger
	tenantIDCache map[string]int
	proxyOpts     install.SQLProxyOpts
	proxyNode     option.NodeListOption
	directoryNode option.NodeListOption
	dirOpts       install.DirectoryServerOpts
	httpClient    *http.Client
	httpURL       string
}

func NewSQLProxy(
	c cluster.Cluster,
	l *logger.Logger,
	proxyNode option.NodeListOption,
	directoryNode option.NodeListOption,
) *SQLProxy {
	return &SQLProxy{
		c:             c,
		l:             l,
		tenantIDCache: make(map[string]int),
		proxyNode:     proxyNode,
		directoryNode: directoryNode,
		httpClient:    http.DefaultClient,
	}
}

// Start starts both the directory server and the SQL proxy.
func (p *SQLProxy) Start(ctx context.Context) error {
	p.dirOpts = install.DirectoryServerOpts{}
	err := roachprod.StartProxyDirectory(ctx, p.l, p.c.Name(), p.directoryNode.InstallNodes()[0], p.dirOpts)
	if err != nil {
		return err
	}
	externalIPs, err := p.c.ExternalIP(ctx, p.l, p.directoryNode)
	if err != nil {
		return err
	}
	internalIPs, err := p.c.InternalIP(ctx, p.l, p.directoryNode)
	if err != nil {
		return err
	}

	p.httpURL = fmt.Sprintf("http://%s:%d", externalIPs[0], install.DirectoryServerHTTPPort(p.dirOpts))
	p.proxyOpts = install.SQLProxyOpts{
		DirectoryAddr: fmt.Sprintf("%s:%d", internalIPs[0], install.DirectoryServerGRPCPort(p.dirOpts)),
		Insecure:      !p.c.IsSecure(),
		SkipVerify:    true,
	}
	return roachprod.StartSQLProxy(ctx, p.l, p.c.Name(), p.proxyNode.InstallNodes()[0], p.proxyOpts)
}

// AddTenant adds a new tenant to the directory server.
func (p *SQLProxy) AddTenant(ctx context.Context, name string) error {
	// Enable session revival tokens for connection migration
	db := p.c.Conn(ctx, p.l, 1)
	defer db.Close()
	_, err := db.ExecContext(ctx, `ALTER TENANT $1 SET CLUSTER SETTING server.user_login.session_revival_token.enabled = true`, name)
	if err != nil {
		return err
	}

	tenantID, err := TenantID(ctx, p.l, p.c, name)
	if err != nil {
		return err
	}

	reqBody := map[string]interface{}{
		"tenant_id":           tenantID,
		"name":                name,
		"allowed_cidr_ranges": []string{"0.0.0.0/0"}, // Allow all IPs for roachtests
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/create-tenant", p.httpURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create tenant failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	p.l.Printf("Created tenant %d (%s) in directory server", tenantID, name)
	return nil
}

func (p *SQLProxy) podOperation(
	ctx context.Context,
	nodes option.NodeListOption,
	virtualClusterName string,
	sqlInstance int,
	operation string,
) error {
	tenantID, err := TenantID(ctx, p.l, p.c, virtualClusterName)
	if err != nil {
		return err
	}
	addrs, err := p.PodAddr(ctx, nodes, virtualClusterName, sqlInstance)
	if err != nil {
		return err
	}

	for _, addr := range addrs {
		reqBody := map[string]interface{}{
			"tenant_id": tenantID,
			"addr":      addr,
		}

		bodyBytes, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}

		apiURL := fmt.Sprintf("%s/api/%s", p.httpURL, operation)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(bodyBytes))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := p.httpClient.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return errors.Errorf("%s failed: status=%d, body=%s", operation, resp.StatusCode, string(body))
		}
		resp.Body.Close()

		p.l.Printf("%s pod %s on tenant %s", operation, addr, virtualClusterName)
	}
	return nil
}

// AddPod adds a new pod for the specified tenant in the directory server.
func (p *SQLProxy) AddPod(
	ctx context.Context, nodes option.NodeListOption, virtualClusterName string, sqlInstance int,
) error {
	err := p.podOperation(ctx, nodes, virtualClusterName, sqlInstance, "add-pod")
	if err != nil {
		return err
	}
	return nil
}

func (p *SQLProxy) RemovePod(
	ctx context.Context, nodes option.NodeListOption, virtualClusterName string, sqlInstance int,
) error {
	err := p.podOperation(ctx, nodes, virtualClusterName, sqlInstance, "remove-pod")
	if err != nil {
		return err
	}
	return nil
}

func (p *SQLProxy) DrainPod(
	ctx context.Context, nodes option.NodeListOption, virtualClusterName string, sqlInstance int,
) error {
	err := p.podOperation(ctx, nodes, virtualClusterName, sqlInstance, "drain-pod")
	if err != nil {
		return err
	}
	return nil
}

func (p *SQLProxy) TenantID(ctx context.Context, virtualClusterName string) (int, error) {
	if id, found := p.tenantIDCache[virtualClusterName]; found {
		return id, nil
	}
	id, err := TenantID(ctx, p.l, p.c, virtualClusterName)
	if err != nil {
		return 0, err
	}
	p.tenantIDCache[virtualClusterName] = id
	return id, nil
}

func (p *SQLProxy) PodAddr(
	ctx context.Context, nodes option.NodeListOption, virtualClusterName string, sqlInstance int,
) ([]string, error) {
	internalIPs, err := p.c.InternalIP(ctx, p.l, nodes)
	if err != nil {
		return nil, err
	}

	sqlPorts, err := p.c.SQLPorts(ctx, p.l, nodes, virtualClusterName, sqlInstance)
	if err != nil {
		return nil, err
	}

	if len(sqlPorts) != len(internalIPs) {
		return nil, fmt.Errorf("SQL ports and IPs count mismatch: %d ports, %d IPs",
			len(sqlPorts), len(internalIPs))
	}

	var addrs []string
	for i := range internalIPs {
		addr := fmt.Sprintf("%s:%d", internalIPs[i], sqlPorts[i])
		addrs = append(addrs, addr)
	}

	return addrs, nil
}

func (p *SQLProxy) InternalURL(ctx context.Context, virtualClusterName string) (string, error) {
	tenantID, err := p.TenantID(ctx, virtualClusterName)
	if err != nil {
		return "", err
	}
	return roachprod.SQLProxyURL(p.l, p.c.MakeNodes(p.proxyNode), install.SimpleSecureOption(p.c.IsSecure()), virtualClusterName, tenantID, install.CockroachNodeCertsDir, p.proxyOpts)
}

func (p *SQLProxy) ExternalURL(ctx context.Context, virtualClusterName string) (string, error) {
	tenantID, err := p.TenantID(ctx, virtualClusterName)
	if err != nil {
		return "", err
	}
	return roachprod.SQLProxyURL(p.l, p.c.MakeNodes(p.proxyNode), install.SimpleSecureOption(p.c.IsSecure()), virtualClusterName, tenantID, p.c.LocalCertsDir(), p.proxyOpts)
}

func (p *SQLProxy) Conn(ctx context.Context, virtualClusterName string) (*gosql.DB, error) {
	proxyURL, err := p.ExternalURL(ctx, virtualClusterName)
	if err != nil {
		return nil, err
	}
	vals := make(url.Values)
	vals["allow_unsafe_internals"] = []string{"true"}
	dataSource := proxyURL + "&" + vals.Encode()
	return gosql.Open("postgres", dataSource)
}

// TenantID retrieves the tenant ID for a given virtual cluster name from the
// system.tenants table. It creates a connection to the system virtual cluster,
// queries the tenant ID, and closes the connection.
func TenantID(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, virtualClusterName string,
) (int, error) {
	systemDB := c.Conn(ctx, l, 1, option.VirtualClusterName("system"))
	defer systemDB.Close()

	var tenantID int
	err := systemDB.QueryRowContext(ctx, "SELECT id FROM system.tenants WHERE name = $1", virtualClusterName).Scan(&tenantID)
	if err != nil {
		return 0, err
	}
	return tenantID, nil
}
