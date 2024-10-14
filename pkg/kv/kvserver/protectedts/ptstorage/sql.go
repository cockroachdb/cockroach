// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ptstorage

const (

	// currentMetaCTE is used by all queries which access the meta row.
	// The query returns a default row if there currently is no meta row.
	// At the time of writing, there will never be a physical row in the meta
	// table with version zero.
	currentMetaCTE = `
SELECT
    version, num_records, num_spans, total_bytes
FROM
    system.protected_ts_meta
UNION ALL
    SELECT 0 AS version, 0 AS num_records, 0 AS num_spans, 0 AS total_bytes
ORDER BY
    version DESC
LIMIT
    1
`

	protectQuery = `
WITH
    current_meta AS (` + currentMetaCTE + `),
    checks AS (` + protectChecksCTE + `),
    updated_meta AS (` + protectUpsertMetaCTE + `),
    new_record AS (` + protectInsertRecordCTEWithChecks + `)
SELECT
    failed,
    total_bytes AS prev_total_bytes,
    version AS prev_version
FROM
    checks, current_meta;`

	// The `target` column was added to `system.protected_ts_records` as part of
	// the tenant migration `AlterSystemProtectedTimestampAddColumn` necessitating
	// queries that write to the table with and without the column. In a
	// mixed-version state (prior to the migration) we use the queries without tha
	// target column.
	//
	// TODO(adityamaru): Delete this in 22.2.
	protectQueryWithoutTarget = `
WITH
    current_meta AS (` + currentMetaCTE + `),
    checks AS (` + protectChecksCTE + `),
    updated_meta AS (` + protectUpsertMetaCTE + `),
    new_record AS (` + protectInsertRecordWithoutTargetCTE + `)
SELECT
    failed,
    num_spans AS prev_spans,
    total_bytes AS prev_total_bytes,
    version AS prev_version
FROM
    checks, current_meta;`

	protectChecksCTE = `
SELECT
    new_version, 
    new_num_records,
    new_num_spans, 
    new_total_bytes,
    (
       ($1 > 0 AND new_num_spans > $1)
       OR ($2 > 0 AND new_total_bytes > $2)
       OR EXISTS(SELECT * FROM system.protected_ts_records WHERE id = $4)
    ) AS failed
FROM (
    SELECT
        version + 1 AS new_version,
        num_records + 1 AS new_num_records, 
        num_spans + $3 AS new_num_spans, 
        total_bytes + length($9) + length($6) + coalesce(length($7:::BYTES),0) AS new_total_bytes
    FROM
        current_meta
)
`

	protectUpsertMetaCTE = `
UPSERT
INTO
    system.protected_ts_meta
(version, num_records, num_spans, total_bytes)
(
    SELECT
        new_version, new_num_records, new_num_spans, new_total_bytes
    FROM
        checks
    WHERE
        NOT failed
)
RETURNING
    version, num_records, num_spans, total_bytes
`

	protectInsertRecordCTE = `
WITH
checks AS (SELECT EXISTS(SELECT * FROM system.protected_ts_records WHERE id = $1) as failed),
new_record AS (
	INSERT INTO
   		system.protected_ts_records (id, ts, meta_type, meta, num_spans, spans, target)
 	(
		SELECT $1, $2, $3, $4, $5, $6, $7
		WHERE NOT EXISTS(SELECT * FROM checks WHERE failed)
	)
	RETURNING id
)
SELECT
	failed
FROM checks;
`

	protectInsertRecordCTEWithChecks = `
INSERT
INTO
    system.protected_ts_records (id, ts, meta_type, meta, num_spans, spans, target)
(
    SELECT
        $4, $5, $6, $7, $8, $9, $10
    WHERE
        NOT EXISTS(SELECT * FROM checks WHERE failed)
)
RETURNING
    id
`

	protectInsertRecordWithoutTargetCTE = `
INSERT
INTO
    system.protected_ts_records (id, ts, meta_type, meta, num_spans, spans)
(
    SELECT
        $4, $5, $6, $7, $8, $9
    WHERE
        NOT EXISTS(SELECT * FROM checks WHERE failed)
)
RETURNING
    id
`

	// The `target` column was added to `system.protected_ts_records` as part of
	// the tenant migration `AlterSystemProtectedTimestampAddColumn` necessitating
	// queries that read from the table with and without the column. In a
	// mixed-version state (prior to the migration) we use the queries without tha
	// target column.
	//
	// TODO(adityamaru): Delete this in 22.2.
	getRecordsWithoutTargetQueryBase = `
SELECT
    id, ts, meta_type, meta, spans, verified
FROM
    system.protected_ts_records`

	getRecordsWithoutTargetQuery = getRecordsWithoutTargetQueryBase + ";"
	getRecordWithoutTargetQuery  = getRecordsWithoutTargetQueryBase + `
WHERE
    id = $1;`

	getRecordsQueryBase = `
SELECT
    id, ts, meta_type, meta, spans, verified, target
FROM
    system.protected_ts_records`

	getRecordsQuery = getRecordsQueryBase + ";"
	getRecordQuery  = getRecordsQueryBase + `
WHERE
    id = $1;`

	markVerifiedQuery = `
UPDATE
    system.protected_ts_records
SET
    verified = true
WHERE
    id = $1
RETURNING
    true
`

	releaseQueryWithMeta = `
WITH
    current_meta AS (` + currentMetaCTE + `),
    record AS (` + releaseSelectRecordCTE + `),
    updated_meta AS (` + releaseUpsertMetaCTE + `)
DELETE FROM
    system.protected_ts_records AS r
WHERE
    EXISTS(SELECT NULL FROM record WHERE r.id = record.id)
RETURNING
    NULL;`

	releaseQuery = `
DELETE FROM
    system.protected_ts_records AS r
WHERE
    r.id = $1 
RETURNING
    r.id;`

	// Collect the number of spans for the record identified by $1.
	releaseSelectRecordCTE = `
SELECT
    id,
    num_spans AS record_spans,
    length(spans) + length(meta_type) + coalesce(length(meta),0) AS record_bytes
FROM
    system.protected_ts_records
WHERE
    id = $1
`

	// Updates the meta row if there was a record.
	releaseUpsertMetaCTE = `
UPSERT
INTO
    system.protected_ts_meta (version, num_records, num_spans, total_bytes)
(
    SELECT
        version, num_records, num_spans, total_bytes
    FROM
        (
            SELECT
                version + 1 AS version,
                num_records - 1 AS num_records,
                num_spans - record_spans AS num_spans,
                total_bytes - record_bytes AS total_bytes
            FROM
                current_meta RIGHT JOIN record ON true
        )
)
RETURNING
    1
`

	updateTimestampQuery = `
WITH
    current_meta AS (` + currentMetaCTE + `),
    updated_meta AS (` + updateTimestampUpsertMetaCTE + `),
    updated_record AS (` + updateTimestampUpsertRecordCTE + `)
SELECT
    id
FROM
    updated_record;`

	updateTimestampUpsertMetaCTE = `
UPSERT
INTO
    system.protected_ts_meta (version, num_records, num_spans, total_bytes)
(
    SELECT
        version + 1,
        num_records,
        num_spans,
        total_bytes
    FROM
        current_meta
)
RETURNING
    NULL
`

	updateTimestampUpsertRecordCTE = `
UPDATE
    system.protected_ts_records
SET
    ts = $2
WHERE
    id = $1
RETURNING
    id
`

	getMetadataQuery = `
WITH
    current_meta AS (` + currentMetaCTE + `)
SELECT
    version, num_records, num_spans, total_bytes
FROM
    current_meta;`
)
