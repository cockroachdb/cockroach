// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// stripEmptyTopicNameFromChangefeeds removes empty topic_name params from all
// persisted changefeed SinkURIs and from external connections used by
// changefeeds (kafka and pubsub schemes). This is necessary because we added
// validation that rejects empty topic_name params in Kafka and Pub/Sub sink
// constructors, and existing changefeeds would fail on resume if their
// persisted SinkURI (or external connection URI) still contains the empty
// param. This is a no-op for existing changefeeds because, prior to
// V26_2_ChangefeedsRejectEmptyTopicName, an empty topic_name param is
// treated the same as not having one at all.
//
// Note: we don't update the job description, which may still contain the
// empty topic_name param. Updating it would require extracting and
// rewriting the URI embedded in a SQL statement string, which is not
// worth the complexity for a cosmetic field that does not affect
// changefeed behavior.
func stripEmptyTopicNameFromChangefeeds(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Strip from changefeed job payloads.
		rows, err := txn.QueryBufferedEx(
			ctx, "get-changefeed-job-ids", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			`SELECT id FROM system.jobs WHERE job_type = $1`,
			jobspb.TypeChangefeed.String(),
		)
		if err != nil {
			return err
		}
		for _, row := range rows {
			jobID := jobspb.JobID(tree.MustBeDInt(row[0]))
			if err := maybeStripEmptyTopicName(ctx, txn, jobID); err != nil {
				return err
			}
		}

		// Strip from external connections with kafka or pubsub schemes.
		ecRows, err := txn.QueryBufferedEx(
			ctx, "get-external-connection-names", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			`SELECT connection_name FROM system.external_connections`,
		)
		if err != nil {
			return err
		}
		for _, row := range ecRows {
			name := string(tree.MustBeDString(row[0]))
			if err := maybeStripEmptyTopicNameFromExternalConnection(ctx, txn, name); err != nil {
				return err
			}
		}

		return nil
	})
}

// maybeStripEmptyTopicName reads the payload of a single changefeed job, and
// if the SinkURI contains an empty topic_name param, removes it and writes the
// payload back.
func maybeStripEmptyTopicName(ctx context.Context, txn isql.Txn, jobID jobspb.JobID) error {
	infoStorage := jobs.InfoStorageForJob(txn, jobID)
	payloadBytes, exists, err := infoStorage.GetLegacyPayload(
		ctx, "strip-empty-topic-name")
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	var payload jobspb.Payload
	if err := protoutil.Unmarshal(payloadBytes, &payload); err != nil {
		return err
	}

	changefeedDetails := payload.GetChangefeed()
	if changefeedDetails == nil {
		return nil
	}

	sinkURI := changefeedDetails.SinkURI
	if sinkURI == "" {
		return nil
	}

	u, err := url.Parse(sinkURI)
	if err != nil {
		// Skip unparseable URIs rather than blocking the upgrade; the changefeed
		// will fail on its next resume when the sink is constructed.
		log.Dev.Warningf(ctx, "skipping empty topic_name migration on changefeed job %d "+
			"with unparseable SinkURI: %v", jobID, err)
		return nil //nolint:returnerrcheck
	}

	q := u.Query()
	if !q.Has("topic_name") || q.Get("topic_name") != "" {
		return nil
	}

	// Strip the empty topic_name param.
	q.Del("topic_name")
	u.RawQuery = q.Encode()
	changefeedDetails.SinkURI = u.String()

	newPayloadBytes, err := protoutil.Marshal(&payload)
	if err != nil {
		return err
	}
	return infoStorage.WriteLegacyPayload(ctx, newPayloadBytes)
}

// maybeStripEmptyTopicNameFromExternalConnection loads an external connection
// by name and, if it is a kafka or pubsub connection with an empty topic_name
// param, strips the param and writes it back.
func maybeStripEmptyTopicNameFromExternalConnection(
	ctx context.Context, txn isql.Txn, name string,
) error {
	ec, err := externalconn.LoadExternalConnection(ctx, name, txn)
	if err != nil {
		return err
	}

	proto := ec.ConnectionProto()
	if proto.Provider != connectionpb.ConnectionProvider_kafka &&
		proto.Provider != connectionpb.ConnectionProvider_gcpubsub {
		return nil
	}

	d, ok := proto.Details.(*connectionpb.ConnectionDetails_SimpleURI)
	if !ok || d.SimpleURI == nil {
		return nil
	}

	uri := d.SimpleURI.URI
	u, err := url.Parse(uri)
	if err != nil {
		log.Dev.Warningf(ctx, "skipping empty topic_name migration for "+
			"external connection %q with unparseable URI: %v", name, err)
		return nil //nolint:returnerrcheck
	}

	q := u.Query()
	if !q.Has("topic_name") || q.Get("topic_name") != "" {
		return nil
	}

	q.Del("topic_name")
	u.RawQuery = q.Encode()
	d.SimpleURI.URI = u.String()

	mut, ok := ec.(*externalconn.MutableExternalConnection)
	if !ok {
		return errors.AssertionFailedf("expected MutableExternalConnection for %q", name)
	}
	mut.SetConnectionDetails(*proto)
	return mut.Update(ctx, txn)
}
