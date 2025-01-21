package replicationtestutils

// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import (
	"fmt"
	"math/rand"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/stretchr/testify/require"
)

// TODO make the sql address unconnectable if useGatewayRoutingMode is true
var useGatewayRoutingMode = metamorphic.ConstantWithTestBool("stream-use-gateway-routing-mode", false)
var useExternalConnection = metamorphic.ConstantWithTestBool("stream-use-external-connection", true)

func GetExternalConnectionURI(
	t *testing.T,
	sourceCluster serverutils.ApplicationLayerInterface,
	destCluster serverutils.ApplicationLayerInterface,
	sourceConnOptions ...serverutils.SQLConnOption,
) url.URL {
	return getReplicationURI(t, sourceCluster, destCluster, useGatewayRoutingMode, true, sourceConnOptions...)
}

func GetReplicationURI(
	t *testing.T,
	sourceCluster serverutils.ApplicationLayerInterface,
	destCluster serverutils.ApplicationLayerInterface,
	sourceConnOptions ...serverutils.SQLConnOption,
) url.URL {
	return getReplicationURI(t, sourceCluster, destCluster, useGatewayRoutingMode, useExternalConnection, sourceConnOptions...)
}

func getReplicationURI(
	t *testing.T,
	sourceCluster serverutils.ApplicationLayerInterface,
	destCluster serverutils.ApplicationLayerInterface,
	useGateway bool,
	useExternal bool,
	sourceConnOptions ...serverutils.SQLConnOption,
) url.URL {
	sourceURI, cleanup := sourceCluster.PGUrl(t, sourceConnOptions...)
	t.Cleanup(cleanup)

	if useGateway {
		query := sourceURI.Query()
		query.Set(streamclient.RoutingModeKey, string(streamclient.RoutingModeGateway))
		sourceURI.RawQuery = query.Encode()
	}

	if useExternal {
		conn := destCluster.SQLConn(t)
		defer conn.Close()

		externalUri := url.URL{Scheme: "external", Host: fmt.Sprintf("replication-uri-%d", rand.Int63())}
		_, err := conn.Exec(fmt.Sprintf("CREATE EXTERNAL CONNECTION '%s' AS '%s'", externalUri.Host, sourceURI.String()))
		require.NoError(t, err)
		return externalUri
	}
	return sourceURI
}
