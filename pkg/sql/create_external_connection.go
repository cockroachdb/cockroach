// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const externalConnectionOp = "CREATE EXTERNAL CONNECTION"

type createExternalConnectionNode struct {
	zeroInputPlanNode
	n *tree.CreateExternalConnection
}

// CreateExternalConnection represents a CREATE EXTERNAL CONNECTION statement.
func (p *planner) CreateExternalConnection(
	ctx context.Context, n *tree.CreateExternalConnection,
) (planNode, error) {
	return &createExternalConnectionNode{n: n}, nil
}

func (c *createExternalConnectionNode) startExec(params runParams) error {
	return params.p.createExternalConnection(params, c.n)
}

type externalConnection struct {
	name     string
	endpoint string
}

func (p *planner) parseExternalConnection(
	ctx context.Context, n *tree.CreateExternalConnection,
) (ec externalConnection, err error) {
	exprEval := p.ExprEvaluator(externalConnectionOp)
	if ec.name, err = exprEval.String(
		ctx, n.ConnectionLabelSpec.Label,
	); err != nil {
		return externalConnection{}, errors.Wrap(err, "failed to resolve External Connection name")
	}
	if ec.endpoint, err = exprEval.String(ctx, n.As); err != nil {
		return externalConnection{}, errors.Wrap(err, "failed to resolve External Connection endpoint")
	}
	return ec, nil
}

func (p *planner) createExternalConnection(
	params runParams, n *tree.CreateExternalConnection,
) error {
	txn := p.InternalSQLTxn()

	if err := params.p.CheckPrivilege(params.ctx, syntheticprivilege.GlobalPrivilegeObject,
		privilege.EXTERNALCONNECTION); err != nil {
		return pgerror.New(
			pgcode.InsufficientPrivilege,
			"only users with the EXTERNALCONNECTION system privilege are allowed to CREATE EXTERNAL CONNECTION")
	}

	// TODO(adityamaru): Add some metrics to track CREATE EXTERNAL CONNECTION
	// usage.

	ec, err := p.parseExternalConnection(params.ctx, n)
	if err != nil {
		return err
	}
	ec.endpoint = v26_2_maybeStripEmptyTopicNameFromSinkURI(params.ctx, params.ExecCfg().Settings, ec.endpoint)

	ex := externalconn.NewMutableExternalConnection()
	// TODO(adityamaru): Revisit if we need to reject certain kinds of names.
	ex.SetConnectionName(ec.name)

	// TODO(adityamaru): Create an entry in the `system.privileges` table for the
	// newly created External Connection with the appropriate privileges. We will
	// grant root/admin, and the user that created the object ALL privileges.

	if err = logAndSanitizeExternalConnectionURI(params.ctx, ec.endpoint); err != nil {
		return errors.Wrap(err, "failed to log and sanitize External Connection")
	}

	var SkipCheckingExternalStorageConnection bool
	var SkipCheckingKMSConnection bool
	if tk := params.ExecCfg().ExternalConnectionTestingKnobs; tk != nil {
		if tk.SkipCheckingExternalStorageConnection != nil {
			SkipCheckingExternalStorageConnection = params.ExecCfg().ExternalConnectionTestingKnobs.SkipCheckingExternalStorageConnection()
		}
		if tk.SkipCheckingKMSConnection != nil {
			SkipCheckingKMSConnection = params.ExecCfg().ExternalConnectionTestingKnobs.SkipCheckingKMSConnection()
		}
	}

	env := externalconn.MakeExternalConnEnv(
		params.ExecCfg().Settings,
		&params.ExecCfg().ExternalIODirConfig,
		params.ExecCfg().InternalDB,
		p.User(),
		params.ExecCfg().DistSQLSrv.ExternalStorageFromURI,
		SkipCheckingExternalStorageConnection,
		SkipCheckingKMSConnection,
		&params.ExecCfg().DistSQLSrv.ServerConfig,
	)

	// Construct the ConnectionDetails for the external resource represented by
	// the External Connection.
	exConn, err := externalconn.ExternalConnectionFromURI(
		params.ctx, env, ec.endpoint,
	)
	if err != nil {
		return errors.Wrap(err, "failed to construct External Connection details")
	}
	ex.SetConnectionDetails(*exConn.ConnectionProto())
	ex.SetConnectionType(exConn.ConnectionType())
	ex.SetOwner(p.User())

	row, err := txn.QueryRowEx(params.ctx, `get-user-id`, txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT user_id FROM system.users WHERE username = $1`,
		p.User(),
	)
	if err != nil {
		return errors.Wrap(err, "failed to get owner ID for External Connection")
	}
	ownerID := tree.MustBeDOid(row[0]).Oid
	ex.SetOwnerID(ownerID)

	// Create the External Connection and persist it in the
	// `system.external_connections` table.
	if err := ex.Create(params.ctx, txn); err != nil {
		ifNotExists := n.ConnectionLabelSpec.IfNotExists
		if ifNotExists && pgerror.GetPGCode(err) == pgcode.DuplicateObject {
			return nil
		}
		return errors.Wrap(err, "failed to create external connection")
	}

	// Grant user `ALL` on the newly created External Connection.
	grantStatement := fmt.Sprintf(`GRANT ALL ON EXTERNAL CONNECTION "%s" TO %s`,
		ec.name, p.User().SQLIdentifier())
	_, err = txn.ExecEx(params.ctx,
		"grant-on-create-external-connection", txn.KV(),
		sessiondata.NodeUserSessionDataOverride, grantStatement)
	if err != nil {
		return errors.Wrap(err, "failed to grant on newly created External Connection")
	}
	return nil
}

func logAndSanitizeExternalConnectionURI(ctx context.Context, externalConnectionURI string) error {
	clean, err := cloud.SanitizeExternalStorageURI(externalConnectionURI, nil)
	if err != nil {
		return err
	}
	log.Ops.Infof(ctx, "external connection planning on connecting to destination %v", redact.Safe(clean))
	return nil
}

// v26_2_maybeStripEmptyTopicNameFromSinkURI strips an empty topic_name
// query param from a sink URI for kafka or pubsub schemes, which is fine
// because changefeeds (the only user of these schemes) effectively ignore
// an empty topic_name.
//
// This is gated between V26_2_ChangefeedsDiscardEmptyTopicName and
// V26_2_ChangefeedsRejectEmptyTopicName; after the latter, the validation
// rejects the URI with an error instead.
func v26_2_maybeStripEmptyTopicNameFromSinkURI(
	ctx context.Context, settings *cluster.Settings, uri string,
) string {
	if !settings.Version.IsActive(ctx, clusterversion.V26_2_ChangefeedsDiscardEmptyTopicName) ||
		settings.Version.IsActive(ctx, clusterversion.V26_2_ChangefeedsRejectEmptyTopicName) {
		return uri
	}
	u, err := url.Parse(uri)
	if err != nil {
		return uri
	}
	switch u.Scheme {
	case "kafka", "confluent-cloud", "azure-event-hub", "gcpubsub":
	default:
		return uri
	}
	q := u.Query()
	if !q.Has("topic_name") || q.Get("topic_name") != "" {
		return uri
	}
	q.Del("topic_name")
	u.RawQuery = q.Encode()
	return u.String()
}

func (c *createExternalConnectionNode) Next(_ runParams) (bool, error) { return false, nil }
func (c *createExternalConnectionNode) Values() tree.Datums            { return nil }
func (c *createExternalConnectionNode) Close(_ context.Context)        {}
