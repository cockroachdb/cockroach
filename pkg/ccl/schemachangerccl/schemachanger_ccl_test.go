// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachangerccl

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/sctest"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// MultiRegionTestClusterFactory is a multi-region implementation of the
// sctest.TestServerFactory interface.
type MultiRegionTestClusterFactory struct {
	scexec *scexec.TestingKnobs
	server *server.TestingKnobs
}

var _ sctest.TestServerFactory = MultiRegionTestClusterFactory{}

// WithSchemaChangerKnobs implements the sctest.TestServerFactory interface.
func (f MultiRegionTestClusterFactory) WithSchemaChangerKnobs(
	knobs *scexec.TestingKnobs,
) sctest.TestServerFactory {
	f.scexec = knobs
	return f
}

// WithMixedVersion implements the sctest.TestServerFactory interface.
func (f MultiRegionTestClusterFactory) WithMixedVersion() sctest.TestServerFactory {
	f.server = &server.TestingKnobs{
		ClusterVersionOverride:         sctest.OldVersionKey.Version(),
		DisableAutomaticVersionUpgrade: make(chan struct{}),
	}
	return f
}

// Run implements the sctest.TestServerFactory interface.
func (f MultiRegionTestClusterFactory) Run(
	ctx context.Context, t *testing.T, fn func(_ serverutils.TestServerInterface, _ *gosql.DB),
) {
	const numServers = 3
	knobs := base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		SQLExecutor: &sql.ExecutorTestingKnobs{
			UseTransactionalDescIDGenerator: true,
		},
	}
	if f.server != nil {
		knobs.Server = f.server
	}
	if f.scexec != nil {
		knobs.SQLDeclarativeSchemaChanger = f.scexec
	}
	c, db, _ := multiregionccltestutils.TestingCreateMultiRegionCluster(t, numServers, knobs)
	defer c.Stopper().Stop(ctx)
	fn(c.Server(0), db)
}

func TestDecomposeToElements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sctest.DecomposeToElements(
		t,
		datapathutils.TestDataPath(t, "decomp"),
		MultiRegionTestClusterFactory{},
	)
}

func TestSubzonesRemovedByGCAfterIndexSwap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "timing dependent")
	skip.UnderRace(t, "timing dependent")

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	systemConn := s.SystemLayer().SQLConn(t)
	systemRunner := sqlutils.MakeSQLRunner(systemConn)
	systemRunner.Exec(t, "SET CLUSTER SETTING sql.gc_job.wait_for_gc.interval = '5s';")

	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, `
CREATE TABLE person (
    name STRING,
    country STRING,
    birth_date DATE,
    INDEX (birth_date),
    PRIMARY KEY (country, birth_date, name)
)
    PARTITION BY LIST (country) (
            PARTITION australia
                VALUES IN ('AU', 'NZ')
                PARTITION BY RANGE (birth_date)
                    (
                        PARTITION old_au VALUES FROM (minvalue) TO ('1995-01-01'),
                        PARTITION yung_au VALUES FROM ('1995-01-01') TO (maxvalue)
                    ),
            PARTITION north_america
                VALUES IN ('US', 'CA')
                PARTITION BY RANGE (birth_date)
                    (
                        PARTITION old_na VALUES FROM (minvalue) TO ('1995-01-01'),
                        PARTITION yung_na VALUES FROM ('1995-01-01') TO (maxvalue)
                    ),
            PARTITION default
                VALUES IN (default)
        );`)

	subzonesQuery := `
	WITH subzone_json AS (
	  SELECT
	    value AS config,
	    ordinality AS array_index
	  FROM system.zones,
	       LATERAL json_array_elements(
	         crdb_internal.pb_to_json('cockroach.config.zonepb.ZoneConfig', config) -> 'subzones'
	       ) WITH ORDINALITY
	  WHERE id = 'person'::REGCLASS::OID
	),
	subzones AS (
		SELECT
			(config->'indexId')::INT index_id,
			(config->>'partitionName')::TEXT partition_name,
			(config->'config'->'gc'->'ttlSeconds')::INT AS ttl_seconds,
			array_index - 1 AS array_index
		FROM subzone_json
	),
	subzone_span_json AS (
		SELECT json_array_elements(crdb_internal.pb_to_json('cockroach.config.zonepb.ZoneConfig', config) -> 'subzoneSpans') AS subzone_span
		FROM system.zones
		WHERE id = 'person'::REGCLASS::OID
	),
	subzone_spans AS (
		SELECT
			crdb_internal.pretty_key(decode(subzone_span->>'key', 'base64'), 0) AS start_key,
			crdb_internal.pretty_key(decode(subzone_span->>'endKey', 'base64'), 0) AS end_key,
			COALESCE((subzone_span->>'subzoneIndex')::INT, 0) AS subzone_index
		FROM subzone_span_json
	)
	SELECT
		subzones.index_id,
		subzones.partition_name,
		subzones.ttl_seconds,
		subzone_spans.start_key,
		subzone_spans.end_key
	FROM subzones
	LEFT JOIN subzone_spans ON subzones.array_index = subzone_spans.subzone_index
	ORDER BY 3, 1, 2, 4, 5;`

	runner.Exec(t, `ALTER PARTITION north_america OF TABLE person CONFIGURE ZONE USING gc.ttlseconds = 4;`)
	runner.Exec(t, `ALTER PARTITION old_au OF TABLE person CONFIGURE ZONE USING gc.ttlseconds = 5;`)
	runner.Exec(t, `ALTER PARTITION yung_au OF TABLE person CONFIGURE ZONE USING gc.ttlseconds = 6;`)
	runner.Exec(t, `ALTER INDEX person@person_pkey CONFIGURE ZONE USING gc.ttlseconds = 7;`)
	runner.Exec(t, `ALTER INDEX person@person_birth_date_idx CONFIGURE ZONE USING gc.ttlseconds = 8;`)

	runner.CheckQueryResults(t, subzonesQuery, [][]string{
		{"1", "north_america", "4", `/1/"CA"`, "NULL"},
		{"1", "north_america", "4", `/1/"US"`, "NULL"},
		{"1", "old_au", "5", `/1/"AU"`, `/1/"AU"/9131`},
		{"1", "old_au", "5", `/1/"NZ"`, `/1/"NZ"/9131`},
		{"1", "yung_au", "6", `/1/"AU"/9131`, `/1/"AU"/PrefixEnd`},
		{"1", "yung_au", "6", `/1/"NZ"/9131`, `/1/"NZ"/PrefixEnd`},
		{"1", "NULL", "7", "/1", `/1/"AU"`},
		{"1", "NULL", "7", `/1/"AU"/PrefixEnd`, `/1/"CA"`},
		{"1", "NULL", "7", `/1/"CA"/PrefixEnd`, `/1/"NZ"`},
		{"1", "NULL", "7", `/1/"NZ"/PrefixEnd`, `/1/"US"`},
		{"1", "NULL", "7", `/1/"US"/PrefixEnd`, "/2"},
		{"2", "NULL", "8", "/2", "NULL"},
	})

	// Cause a primary key change.
	runner.Exec(t, `ALTER TABLE person ADD COLUMN new_col INT DEFAULT 0;`)

	// Right after the primary key change, the subzones for the old index and
	// temporary index should still be present.
	runner.CheckQueryResults(t, subzonesQuery, [][]string{
		{"1", "north_america", "4", `/1/"CA"`, "NULL"},
		{"1", "north_america", "4", `/1/"US"`, "NULL"},
		{"3", "north_america", "4", `/3/"CA"`, "NULL"},
		{"3", "north_america", "4", `/3/"US"`, "NULL"},
		{"4", "north_america", "4", `/4/"CA"`, "NULL"},
		{"4", "north_america", "4", `/4/"US"`, "NULL"},
		{"1", "old_au", "5", `/1/"AU"`, `/1/"AU"/9131`},
		{"1", "old_au", "5", `/1/"NZ"`, `/1/"NZ"/9131`},
		{"3", "old_au", "5", `/3/"AU"`, `/3/"AU"/9131`},
		{"3", "old_au", "5", `/3/"NZ"`, `/3/"NZ"/9131`},
		{"4", "old_au", "5", `/4/"AU"`, `/4/"AU"/9131`},
		{"4", "old_au", "5", `/4/"NZ"`, `/4/"NZ"/9131`},
		{"1", "yung_au", "6", `/1/"AU"/9131`, `/1/"AU"/PrefixEnd`},
		{"1", "yung_au", "6", `/1/"NZ"/9131`, `/1/"NZ"/PrefixEnd`},
		{"3", "yung_au", "6", `/3/"AU"/9131`, `/3/"AU"/PrefixEnd`},
		{"3", "yung_au", "6", `/3/"NZ"/9131`, `/3/"NZ"/PrefixEnd`},
		{"4", "yung_au", "6", `/4/"AU"/9131`, `/4/"AU"/PrefixEnd`},
		{"4", "yung_au", "6", `/4/"NZ"/9131`, `/4/"NZ"/PrefixEnd`},
		{"1", "NULL", "7", "/1", `/1/"AU"`},
		{"1", "NULL", "7", `/1/"AU"/PrefixEnd`, `/1/"CA"`},
		{"1", "NULL", "7", `/1/"CA"/PrefixEnd`, `/1/"NZ"`},
		{"1", "NULL", "7", `/1/"NZ"/PrefixEnd`, `/1/"US"`},
		{"1", "NULL", "7", `/1/"US"/PrefixEnd`, "/2"},
		{"3", "NULL", "7", "/3", `/3/"AU"`},
		{"3", "NULL", "7", `/3/"AU"/PrefixEnd`, `/3/"CA"`},
		{"3", "NULL", "7", `/3/"CA"/PrefixEnd`, `/3/"NZ"`},
		{"3", "NULL", "7", `/3/"NZ"/PrefixEnd`, `/3/"US"`},
		{"3", "NULL", "7", `/3/"US"/PrefixEnd`, "/4"},
		{"4", "NULL", "7", "/4", `/4/"AU"`},
		{"4", "NULL", "7", `/4/"AU"/PrefixEnd`, `/4/"CA"`},
		{"4", "NULL", "7", `/4/"CA"/PrefixEnd`, `/4/"NZ"`},
		{"4", "NULL", "7", `/4/"NZ"/PrefixEnd`, `/4/"US"`},
		{"4", "NULL", "7", `/4/"US"/PrefixEnd`, "/5"},
		{"2", "NULL", "8", "/2", "NULL"},
	})

	// Keep retrying until the old index and temporary index are removed by the GC job.
	runner.SucceedsSoonDuration = 12 * time.Second
	runner.CheckQueryResultsRetry(t, subzonesQuery, [][]string{
		{"3", "north_america", "4", `/3/"CA"`, "NULL"},
		{"3", "north_america", "4", `/3/"US"`, "NULL"},
		{"3", "old_au", "5", `/3/"AU"`, `/3/"AU"/9131`},
		{"3", "old_au", "5", `/3/"NZ"`, `/3/"NZ"/9131`},
		{"3", "yung_au", "6", `/3/"AU"/9131`, `/3/"AU"/PrefixEnd`},
		{"3", "yung_au", "6", `/3/"NZ"/9131`, `/3/"NZ"/PrefixEnd`},
		{"3", "NULL", "7", "/3", `/3/"AU"`},
		{"3", "NULL", "7", `/3/"AU"/PrefixEnd`, `/3/"CA"`},
		{"3", "NULL", "7", `/3/"CA"/PrefixEnd`, `/3/"NZ"`},
		{"3", "NULL", "7", `/3/"NZ"/PrefixEnd`, `/3/"US"`},
		{"3", "NULL", "7", `/3/"US"/PrefixEnd`, "/4"},
		{"2", "NULL", "8", "/2", "NULL"},
	})
}
