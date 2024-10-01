// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// InfoStorage can be used to read and write rows to system.job_info table. All
// operations are scoped under the txn and are executed on behalf of Job j.
type InfoStorage struct {
	j   *Job
	txn isql.Txn

	// claimChecked is true if the claim session has already been
	// checked once in this transaction. It may be set directly by
	// callers if they know they've already checked the claim.
	claimChecked bool
}

// InfoStorage returns a new InfoStorage with the passed in job and txn.
func (j *Job) InfoStorage(txn isql.Txn) InfoStorage {
	return InfoStorage{j: j, txn: txn}
}

// InfoStorageForJob returns a new InfoStorage with the passed in
// job ID and txn. It avoids loading the job record. The resulting
// job_info writes will not check the job session ID.
func InfoStorageForJob(txn isql.Txn, jobID jobspb.JobID) InfoStorage {
	return InfoStorage{j: &Job{id: jobID}, txn: txn}
}

func (i *InfoStorage) checkClaimSession(ctx context.Context) error {
	if i.claimChecked {
		return nil
	}

	row, err := i.txn.QueryRowEx(ctx, "check-claim-session", i.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT claim_session_id FROM system.jobs WHERE id = $1`, i.j.ID())
	if err != nil {
		return err
	}

	if row == nil {
		return errors.Errorf(
			"expected session %q for job ID %d but found none", i.j.Session().ID(), i.j.ID())
	}

	storedSession := []byte(*row[0].(*tree.DBytes))
	if !bytes.Equal(storedSession, i.j.Session().ID().UnsafeBytes()) {
		return errors.Errorf(
			"expected session %q but found %q", i.j.Session().ID(), sqlliveness.SessionID(storedSession))
	}
	i.claimChecked = true

	return nil
}

func (i InfoStorage) get(ctx context.Context, infoKey string) ([]byte, bool, error) {
	if i.txn == nil {
		return nil, false, errors.New("cannot access the job info table without an associated txn")
	}

	ctx, sp := tracing.ChildSpan(ctx, "get-job-info")
	defer sp.Finish()

	j := i.j

	// We expect there to be only a single row for a given <job_id, info_key>.
	// This is because all older revisions are deleted before a new one is
	// inserted in `InfoStorage.Write`.
	row, err := i.txn.QueryRowEx(
		ctx, "job-info-get", i.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		"SELECT value FROM system.job_info WHERE job_id = $1 AND info_key::string = $2 ORDER BY written DESC LIMIT 1",
		j.ID(), infoKey,
	)

	if err != nil {
		return nil, false, err
	}

	if row == nil {
		return nil, false, nil
	}

	value, ok := row[0].(*tree.DBytes)
	if !ok {
		return nil, false, errors.AssertionFailedf("job info: expected value to be DBytes (was %T)", row[0])
	}

	return []byte(*value), true, nil
}

func (i InfoStorage) write(ctx context.Context, infoKey string, value []byte) error {
	return i.doWrite(ctx, func(ctx context.Context, j *Job, txn isql.Txn) error {
		// First clear out any older revisions of this info.
		_, err := txn.ExecEx(
			ctx, "write-job-info-delete", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			"DELETE FROM system.job_info WHERE job_id = $1 AND info_key::string = $2",
			j.ID(), infoKey,
		)
		if err != nil {
			return err
		}

		if value == nil {
			// Nothing else to do.
			return nil
		}
		// Write the new info, using the same transaction.
		_, err = txn.ExecEx(
			ctx, "write-job-info-insert", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			`INSERT INTO system.job_info (job_id, info_key, written, value) VALUES ($1, $2, now(), $3)`,
			j.ID(), infoKey, value,
		)
		return err
	})
}

func (i InfoStorage) doWrite(
	ctx context.Context, fn func(ctx context.Context, job *Job, txn isql.Txn) error,
) error {
	if i.txn == nil {
		return errors.New("cannot write to the job info table without an associated txn")
	}

	ctx, sp := tracing.ChildSpan(ctx, "write-job-info")
	defer sp.Finish()

	j := i.j

	if j.Session() != nil {
		if err := i.checkClaimSession(ctx); err != nil {
			return err
		}
	} else {
		log.VInfof(ctx, 1, "job %d: writing to the system.job_info with no session ID", j.ID())
	}

	return fn(ctx, j, i.txn)
}

func (i InfoStorage) iterate(
	ctx context.Context,
	iterMode iterateMode,
	infoPrefix string,
	fn func(infoKey string, value []byte) error,
) (retErr error) {
	if i.txn == nil {
		return errors.New("cannot iterate over the job info table without an associated txn")
	}

	var iterConfig string
	switch iterMode {
	// Note: the ORDER BY clauses below are tuned to ensure that the
	// query uses the index directly, without additional plan stages. If
	// you change this, verify the structure of the resulting plan with
	// EXPLAIN and tweak index use accordingly.
	case iterateAll:
		iterConfig = `ORDER BY info_key ASC, written DESC` // with no LIMIT.
	case getLast:
		iterConfig = `ORDER BY info_key DESC, written ASC LIMIT 1`
	default:
		return errors.AssertionFailedf("unhandled iteration mode %v", iterMode)
	}

	rows, err := i.txn.QueryIteratorEx(
		ctx, "job-info-iter", i.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT info_key, value
		FROM system.job_info
		WHERE job_id = $1 AND info_key >= $2 AND info_key < $3
		`+iterConfig,
		i.j.ID(), infoPrefix, string(roachpb.Key(infoPrefix).PrefixEnd()),
	)
	if err != nil {
		return err
	}
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(rows)

	var prevKey string
	var ok bool
	for ok, err = rows.Next(ctx); ok; ok, err = rows.Next(ctx) {
		if err != nil {
			return err
		}
		row := rows.Cur()

		key, ok := row[0].(*tree.DString)
		if !ok {
			return errors.AssertionFailedf("job info: expected info_key to be string (was %T)", row[0])
		}
		infoKey := string(*key)

		if infoKey == prevKey {
			continue
		}
		prevKey = infoKey

		value, ok := row[1].(*tree.DBytes)
		if !ok {
			return errors.AssertionFailedf("job info: expected value to be DBytes (was %T)", row[1])
		}
		if err = fn(infoKey, []byte(*value)); err != nil {
			return err
		}
	}

	return err
}

// Get fetches the latest info record for the given job and infoKey.
func (i InfoStorage) Get(ctx context.Context, infoKey string) ([]byte, bool, error) {
	return i.get(ctx, infoKey)
}

// Write writes the provided value to an info record for the provided jobID and
// infoKey after removing any existing info records for that job and infoKey
// using the same transaction, effectively replacing any older row with a row
// with the new value.
func (i InfoStorage) Write(ctx context.Context, infoKey string, value []byte) error {
	if value == nil {
		return errors.AssertionFailedf("missing value (infoKey %q)", infoKey)
	}
	return i.write(ctx, infoKey, value)
}

// Delete removes the info record for the provided infoKey.
func (i InfoStorage) Delete(ctx context.Context, infoKey string) error {
	return i.write(ctx, infoKey, nil /* value */)
}

// DeleteRange removes the info records between the provided
// start key (inclusive) and end key (exclusive).
func (i InfoStorage) DeleteRange(
	ctx context.Context, startInfoKey, endInfoKey string, limit int,
) error {
	return i.doWrite(ctx, func(ctx context.Context, j *Job, txn isql.Txn) error {
		if limit > 0 {
			_, err := txn.ExecEx(
				ctx, "write-job-info-delete", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				"DELETE FROM system.job_info WHERE job_id = $1 AND info_key >= $2 AND info_key < $3 "+
					"ORDER BY info_key ASC LIMIT $4",
				j.ID(), startInfoKey, endInfoKey, limit,
			)
			return err
		} else {
			_, err := txn.ExecEx(
				ctx, "write-job-info-delete", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				"DELETE FROM system.job_info WHERE job_id = $1 AND info_key >= $2 AND info_key < $3",
				j.ID(), startInfoKey, endInfoKey,
			)
			return err
		}
	})
}

// Count counts the info records in the range [start, end).
func (i InfoStorage) Count(ctx context.Context, startInfoKey, endInfoKey string) (int, error) {
	if i.txn == nil {
		return 0, errors.New("cannot access the job info table without an associated txn")
	}

	ctx, sp := tracing.ChildSpan(ctx, "count-job-info")
	defer sp.Finish()

	row, err := i.txn.QueryRowEx(
		ctx, "job-info-count", i.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		"SELECT count(*) FROM system.job_info WHERE job_id = $1 AND info_key >= $2 AND info_key < $3",
		i.j.ID(), startInfoKey, endInfoKey,
	)

	if err != nil || row == nil {
		return 0, err
	}

	value, ok := row[0].(*tree.DInt)
	if !ok {
		return 0, errors.AssertionFailedf("job info: expected value to be DInt (was %T)", row[0])
	}

	return int(*value), nil
}

// Iterate iterates though the info records for a given job and info key prefix.
func (i InfoStorage) Iterate(
	ctx context.Context, infoPrefix string, fn func(infoKey string, value []byte) error,
) (retErr error) {
	return i.iterate(ctx, iterateAll, infoPrefix, fn)
}

// GetLast calls fn on the last info record whose key matches the
// given prefix.
func (i InfoStorage) GetLast(
	ctx context.Context, infoPrefix string, fn func(infoKey string, value []byte) error,
) (retErr error) {
	return i.iterate(ctx, getLast, infoPrefix, fn)
}

type iterateMode bool

const (
	iterateAll iterateMode = false
	getLast    iterateMode = true
)

const (
	LegacyPayloadKey  = "legacy_payload"
	LegacyProgressKey = "legacy_progress"
)

// GetLegacyPayloadKey returns the info_key whose value is the jobspb.Payload of
// the job.
func GetLegacyPayloadKey() string {
	return LegacyPayloadKey
}

// GetLegacyProgressKey returns the info_key whose value is the jobspb.Progress
// of the job.
func GetLegacyProgressKey() string {
	return LegacyProgressKey
}

// GetLegacyPayload returns the job's Payload from the system.job_info table.
func (i InfoStorage) GetLegacyPayload(ctx context.Context) ([]byte, bool, error) {
	return i.Get(ctx, LegacyPayloadKey)
}

// WriteLegacyPayload writes the job's Payload to the system.job_info table.
func (i InfoStorage) WriteLegacyPayload(ctx context.Context, payload []byte) error {
	return i.Write(ctx, LegacyPayloadKey, payload)
}

// GetLegacyProgress returns the job's Progress from the system.job_info table.
func (i InfoStorage) GetLegacyProgress(ctx context.Context) ([]byte, bool, error) {
	return i.Get(ctx, LegacyProgressKey)
}

// WriteLegacyProgress writes the job's Progress to the system.job_info table.
func (i InfoStorage) WriteLegacyProgress(ctx context.Context, progress []byte) error {
	return i.Write(ctx, LegacyProgressKey, progress)
}
