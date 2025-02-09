// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var (
	retainedProgressHistory = settings.RegisterIntSetting(
		settings.ApplicationLevel, "jobs.retained_progress_entries", "number of historical progress entries to retain per job", 1000,
	)
	retainedMessageHistory = settings.RegisterIntSetting(
		settings.ApplicationLevel, "jobs.retained_messages", "number of historical messages of each kind to retain per job", 100,
	)
)

// ProgressStorage reads and writes progress rows.
type ProgressStorage jobspb.JobID

// ProgressStorage returns a new ProgressStorage with the passed in job and txn.
func (j *Job) ProgressStorage() ProgressStorage {
	return ProgressStorage(j.id)
}

// Get returns the latest progress report for the job along with when it was
// written. If the fraction is null it is returned as NaN, and if the resolved
// ts is null, it is empty.
func (i ProgressStorage) Get(
	ctx context.Context, txn isql.Txn,
) (float64, hlc.Timestamp, time.Time, error) {
	ctx, sp := tracing.ChildSpan(ctx, "get-job-progress")
	defer sp.Finish()

	row, err := txn.QueryRowEx(
		ctx, "job-progress-get", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		"SELECT written, fraction, resolved FROM system.job_progress WHERE job_id = $1", i,
	)

	if err != nil || row == nil {
		return 0, hlc.Timestamp{}, time.Time{}, err
	}

	written, ok := row[0].(*tree.DTimestampTZ)
	if !ok {
		return 0, hlc.Timestamp{}, time.Time{}, errors.AssertionFailedf("job progress: expected value to be DTimestampTZ (was %T)", row[1])
	}

	var fraction float64
	if row[1] == tree.DNull {
		fraction = math.NaN()
	} else {
		fracDatum, ok := row[1].(*tree.DFloat)
		if !ok {
			return 0, hlc.Timestamp{}, time.Time{}, errors.AssertionFailedf("job progress: expected value to be DFloat (was %T)", row[1])
		}
		fraction = float64(*fracDatum)
	}

	var ts hlc.Timestamp
	if row[2] != tree.DNull {
		resolved, ok := row[2].(*tree.DDecimal)
		if !ok {
			return 0, hlc.Timestamp{}, time.Time{}, errors.AssertionFailedf("job progress: expected value to be DDecimal (was %T)", row[1])
		}
		ts, err = hlc.DecimalToHLC(&resolved.Decimal)
		if err != nil {
			return 0, hlc.Timestamp{}, time.Time{}, err
		}
	}

	return fraction, ts, written.Time, nil
}

// Set records a progress update. If fraction is NaN or resolved is empty, that
// field is left null. The time at which the progress was reported is recorded.
func (i ProgressStorage) Set(
	ctx context.Context, txn isql.Txn, fraction float64, resolved hlc.Timestamp,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "write-job-progress")
	defer sp.Finish()

	if _, err := txn.ExecEx(
		ctx, "write-job-progress-delete", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.job_progress WHERE job_id = $1`, i,
	); err != nil {
		return err
	}

	var frac, ts interface{}
	if !math.IsNaN(fraction) {
		frac = fraction
	}
	if resolved.IsSet() {
		ts = resolved.AsOfSystemTime()
	}

	if _, err := txn.ExecEx(
		ctx, "write-job-progress-insert", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`INSERT INTO system.job_progress (job_id, written, fraction, resolved) VALUES ($1, now(), $2, $3)`,
		i, frac, ts,
	); err != nil {
		return err
	}

	if _, err := txn.ExecEx(
		ctx, "write-job-progress-history-insert", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.job_progress_history (job_id, written, fraction, resolved) VALUES ($1, now(), $2, $3)`,
		i, frac, ts,
	); err != nil {
		return err
	}

	if _, err := txn.ExecEx(
		ctx, "write-job-progress-history-prune", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.job_progress_history WHERE job_id = $1 AND written IN 
			(SELECT written FROM system.job_progress_history WHERE job_id = $1 ORDER BY written DESC OFFSET $2)`,
		i, retainedProgressHistory.Get(txn.KV().DB().SettingsValues()),
	); err != nil {
		return err
	}

	return nil
}

// StatusStorage reads and writes the "status" row for a job.
//
// A status is an optional short string that describes to a human what a job is
// doing at any given point while it is running or reverting, particularly if a
// given job does many distinct things over the course of running. If a job just
// does one thing when it is running, it likely does not need to set a status -
// the fact that the job of that type is running is all one needs to know.
//
// Status messages are strictly for human consumption; they should not be read,
// parsed or compared by code; if your job wants to store something and read it
// back later use InfoStorage instead.
//
// A job can only have one status at a time -- setting it will clears any
// previously set status. To just surface a message to the user that can be
// presented along side other messages rather than change its "current status"
// use MessageStorage.
type StatusStorage jobspb.JobID

// Status returns the StatusStorage for the job.
func (j *Job) StatusStorage() StatusStorage {
	return StatusStorage(j.id)
}

// Clear clears the status message row for the job, if it exists.
func (i StatusStorage) Clear(ctx context.Context, txn isql.Txn) error {
	_, err := txn.ExecEx(
		ctx, "clear-job-status-delete", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.job_status WHERE job_id = $1`, i,
	)
	return err
}

// Sets writes the current status, replacing the current one if it exists.
// Setting an empty status is the same as calling Clear().
func (i StatusStorage) Set(ctx context.Context, txn isql.Txn, status string) error {
	ctx, sp := tracing.ChildSpan(ctx, "write-job-status")
	defer sp.Finish()

	// Delete any existing status row in the same transaction before replacing it
	// with the new one.
	if err := i.Clear(ctx, txn); err != nil {
		return err
	}

	if _, err := txn.ExecEx(
		ctx, "write-job-status-insert", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`INSERT INTO system.job_status (job_id, written, status) VALUES ($1, now(), $2)`,
		i, status,
	); err != nil {
		return err
	}

	if err := MessageStorage(i).Record(ctx, txn, "status", status); err != nil {
		return err
	}

	return nil
}

// Get gets the current status mesasge for a job, if any.
func (i StatusStorage) Get(ctx context.Context, txn isql.Txn) (string, time.Time, error) {
	ctx, sp := tracing.ChildSpan(ctx, "get-job-status")
	defer sp.Finish()

	row, err := txn.QueryRowEx(
		ctx, "job-status-get", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		"SELECT written, status FROM system.job_status WHERE job_id = $1",
		i,
	)

	if err != nil {
		return "", time.Time{}, err
	}

	if row == nil {
		return "", time.Time{}, nil
	}

	written, ok := row[0].(*tree.DTimestampTZ)
	if !ok {
		return "", time.Time{}, errors.AssertionFailedf("job Status: expected value to be DTimestampTZ (was %T)", row[1])
	}
	status, ok := row[1].(*tree.DString)
	if !ok {
		return "", time.Time{}, errors.AssertionFailedf("job Status: expected value to be DString (was %T)", row[1])
	}

	return string(*status), written.Time, nil
}

// MessageStorage stores human-readable messages emitted by the execution of a
// job, including when it changes states in the job system, when it wishes to
// communicate its own custom status, or additional messages it wishes to
// surface to the user. Messages include a string identifier of the kind of
// message, and retention limits are enforced on the number of messages of each
// kind per job, so a more frequently emitted kind of message will not cause
// messages of other kinds to be pruned. For example, if a job emitted a "retry"
// every minute, after a couple hours older messages about retries would be
// pruned but a "state" message from days prior indicating it was unpaused would
// still be retrained.
type MessageStorage jobspb.JobID

// Messages returns the MessageStorage for the job.
func (j *Job) Messages() MessageStorage {
	return MessageStorage(j.id)
}

// Record writes a human readable message of the specified kind to the message
// log for this job, and prunes retained messages of the same kind based on the
// configured limit to keep the total number of retained messages bounded.
func (i MessageStorage) Record(ctx context.Context, txn isql.Txn, kind, message string) error {
	ctx, sp := tracing.ChildSpan(ctx, "write-job-message")
	defer sp.Finish()

	// Insert the new message.
	if _, err := txn.ExecEx(
		ctx, "write-job-message-insert", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`UPSERT INTO system.job_message (job_id, written, kind, message) VALUES ($1, now(),$2, $3)`,
		i, kind, message,
	); err != nil {
		return err
	}

	// Prune old messages of the same kind to bound historical data.
	if _, err := txn.ExecEx(
		ctx, "write-job-message-prune", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.job_message WHERE job_id = $1 AND kind = $2 AND written IN (
			SELECT written FROM system.job_message WHERE job_id = $1 AND kind = $2 ORDER BY written DESC OFFSET $3
		)`,
		i, kind, retainedMessageHistory.Get(txn.KV().DB().SettingsValues()),
	); err != nil {
		return err
	}

	return nil
}

type JobMessage struct {
	Kind, Message string
	Written       time.Time
}

func (i MessageStorage) Fetch(ctx context.Context, txn isql.Txn) (_ []JobMessage, retErr error) {
	ctx, sp := tracing.ChildSpan(ctx, "get-all-job-message")
	defer sp.Finish()

	rows, err := txn.QueryIteratorEx(
		ctx, "get-job-messages", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`SELECT written, kind, message FROM system.job_message WHERE job_id = $1 ORDER BY written DESC`,
		i,
	)
	if err != nil {
		return nil, err
	}
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(rows)
	var res []JobMessage
	for {
		ok, err := rows.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		row := rows.Cur()
		written, ok := row[0].(*tree.DTimestampTZ)
		if !ok {
			return nil, errors.AssertionFailedf("job message: expected written to be DTimestampTZ (was %T)", row[0])
		}
		kind, ok := row[1].(*tree.DString)
		if !ok {
			return nil, errors.AssertionFailedf("job message: expected kind to be DString (was %T)", row[1])
		}
		message, ok := row[2].(*tree.DString)
		if !ok {
			return nil, errors.AssertionFailedf("job message: expected message to be DString (was %T)", row[2])
		}
		res = append(res, JobMessage{
			Kind:    string(*kind),
			Message: string(*message),
			Written: written.Time,
		})
	}
	return res, nil
}

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

func (i InfoStorage) get(ctx context.Context, opName, infoKey string) ([]byte, bool, error) {
	if i.txn == nil {
		return nil, false, errors.New("cannot access the job info table without an associated txn")
	}

	ctx, sp := tracing.ChildSpan(ctx, opName)
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
func (i InfoStorage) Get(ctx context.Context, opName, infoKey string) ([]byte, bool, error) {
	return i.get(ctx, opName, infoKey)
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
func (i InfoStorage) GetLegacyPayload(ctx context.Context, opName string) ([]byte, bool, error) {
	return i.Get(ctx, opName, LegacyPayloadKey)
}

// WriteLegacyPayload writes the job's Payload to the system.job_info table.
func (i InfoStorage) WriteLegacyPayload(ctx context.Context, payload []byte) error {
	return i.Write(ctx, LegacyPayloadKey, payload)
}

// GetLegacyProgress returns the job's Progress from the system.job_info table.
func (i InfoStorage) GetLegacyProgress(ctx context.Context, opName string) ([]byte, bool, error) {
	return i.Get(ctx, opName, LegacyProgressKey)
}

// WriteLegacyProgress writes the job's Progress to the system.job_info table.
func (i InfoStorage) WriteLegacyProgress(ctx context.Context, progress []byte) error {
	return i.Write(ctx, LegacyProgressKey, progress)
}
