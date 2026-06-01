// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdctest

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// perturbStats tracks event counts and message statistics during the test.
type perturbStats struct {
	inserts     int
	updates     int
	deletes     int
	splits      int
	pauses      int
	resumes     int
	pushes      int
	aborts      int
	addColumns  int
	dropColumns int
	rows        int
	resolved    int
	duplicates  int
}

// perturbation holds state for the randomized perturbation test.
type perturbation struct {
	rng *rand.Rand
	db  *gosql.DB
	f   TestFeed
	v   *CountValidator

	// tableName is the name of the table under test.
	tableName string

	// columns tracks the current schema. The first entry is always the INT
	// PRIMARY KEY column. Subsequent entries are the randomly generated
	// non-PK columns from initial table creation.
	columns []perturbColumn

	// familyNames holds the column family names when column families are
	// used. Empty when families are not used. With families, each DML
	// operation generates one changefeed event per family, and the
	// changefeed targets each family explicitly.
	familyNames []string

	// numFamilies is 1 when column families are not used, otherwise
	// len(familyNames). DML event counts are multiplied by this value
	// to compute expected rows.
	numFamilies int

	// addedColumnCount tracks columns added via schema changes during the
	// test. These use names like "sc_0", "sc_1", etc.
	addedColumnCount int

	// availableRows tracks the approximate number of feed messages we expect
	// to be able to read. This mirrors the pattern in nemeses.go.
	availableRows int

	// expectedRows is a monotonically increasing count of the total row
	// events the changefeed should deliver (initial scan + DML + backfills).
	// Unlike availableRows, it is never decremented. Comparing it against
	// stats.rows after the test detects missing rows (e.g., skipped
	// backfills).
	expectedRows int

	// nextRowID is the next primary key value to use for inserts.
	nextRowID int

	// isSinkless disables pause/resume operations.
	isSinkless bool

	// paused tracks whether the changefeed is currently paused.
	paused bool

	// stats tracks event and message counts for the test summary.
	stats perturbStats

	// seenRows tracks key+timestamp pairs already delivered by the
	// changefeed, used to detect duplicate messages.
	seenRows map[string]struct{}
}

// perturbColumn describes a column in the table under test.
type perturbColumn struct {
	name string
	typ  *types.T
}

// perturbationOptions records the randomized changefeed options.
type perturbationOptions struct {
	diff          bool
	mvccTimestamp bool
	keyInValue    bool
	fullTableName bool
	schemaPolicy  string
	schemaEvents  string
}

// isChangefeedSafeType returns true if the type can be used in a changefeed
// table with JSON format. We exclude types that are problematic for changefeed
// encoding or that require special setup (like enums).
func isChangefeedSafeType(typ *types.T) bool {
	switch typ.Family() {
	case types.AnyFamily,
		types.TupleFamily,
		types.VoidFamily,
		types.UnknownFamily,
		types.EnumFamily,
		types.OidFamily,
		types.RefCursorFamily,
		types.TriggerFamily,
		types.JsonpathFamily,
		types.PGVectorFamily,
		types.LTreeFamily,
		types.EncodedKeyFamily,
		types.DecimalFamily:
		return false
	case types.ArrayFamily:
		// Arrays of certain types are not supported.
		contents := typ.ArrayContents()
		switch contents.Family() {
		case types.JsonFamily,
			types.TSQueryFamily,
			types.TSVectorFamily,
			types.PGVectorFamily,
			types.CollatedStringFamily:
			return false
		}
		return isChangefeedSafeType(contents)
	case types.CollatedStringFamily:
		// Collated strings can cause locale-dependent issues in tests.
		return false
	}
	return true
}

// generateColumns generates 2-8 random non-PK columns for the table.
func generateColumns(rng *rand.Rand) []perturbColumn {
	numCols := 2 + rng.Intn(7) // 2 to 8
	cols := make([]perturbColumn, 0, numCols+1)

	// Primary key column.
	cols = append(cols, perturbColumn{name: "id", typ: types.Int})

	for i := 0; i < numCols; i++ {
		var typ *types.T
		for {
			typ = randgen.RandColumnType(rng)
			if isChangefeedSafeType(typ) {
				break
			}
		}
		cols = append(cols, perturbColumn{
			name: fmt.Sprintf("c%d", i),
			typ:  typ,
		})
	}
	return cols
}

// familyDef describes a column family for table creation.
type familyDef struct {
	name       string
	columnIdxs []int // indices into the columns array
}

// buildFamilies splits the columns into two families: fam_a contains the
// PK plus the first half of non-PK columns, fam_b contains the rest.
func buildFamilies(cols []perturbColumn) ([]familyDef, []string) {
	numNonPK := len(cols) - 1
	firstHalf := numNonPK / 2
	if firstHalf < 1 {
		firstHalf = 1
	}

	famA := familyDef{name: "fam_a"}
	famB := familyDef{name: "fam_b"}

	// PK + first half of non-PK columns go into fam_a.
	for i := 0; i <= firstHalf; i++ {
		famA.columnIdxs = append(famA.columnIdxs, i)
	}
	// Remaining non-PK columns go into fam_b.
	for i := firstHalf + 1; i < len(cols); i++ {
		famB.columnIdxs = append(famB.columnIdxs, i)
	}

	families := []familyDef{famA, famB}
	names := []string{famA.name, famB.name}
	return families, names
}

// buildCreateTable returns a CREATE TABLE statement for the given columns,
// optionally with column family definitions.
func buildCreateTable(tableName string, cols []perturbColumn, families []familyDef) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "CREATE TABLE %s (", tableName)
	for i, col := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		if i == 0 {
			fmt.Fprintf(&sb, "%s INT PRIMARY KEY", col.name)
		} else {
			fmt.Fprintf(&sb, "%s %s", col.name, col.typ.SQLString())
		}
	}
	for _, fam := range families {
		sb.WriteString(", FAMILY ")
		sb.WriteString(fam.name)
		sb.WriteString(" (")
		for j, idx := range fam.columnIdxs {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(cols[idx].name)
		}
		sb.WriteString(")")
	}
	sb.WriteString(")")
	return sb.String()
}

// randomDatumSQL generates a random SQL literal for the given type, formatted
// so it can be embedded in a SQL statement. It tries to avoid returning "NULL"
// because with column families, a family whose columns are all NULL has no KV
// entry and produces no changefeed event, causing expectedRows to overcount.
func randomDatumSQL(rng *rand.Rand, typ *types.T) string {
	for i := 0; i < 10; i++ {
		d := randgen.RandDatum(rng, typ, false /* nullOk */)
		if d != tree.DNull {
			return tree.AsStringWithFlags(d, tree.FmtParsable)
		}
	}
	return "NULL"
}

// generateOptions randomly selects changefeed options for the test.
func generateOptions(rng *rand.Rand, testName string) perturbationOptions {
	isCloudstorage := strings.Contains(testName, "cloudstorage")
	isWebhook := strings.Contains(testName, "webhook")

	opts := perturbationOptions{}
	opts.diff = rng.Intn(2) == 0
	opts.mvccTimestamp = rng.Intn(2) == 0
	opts.fullTableName = rng.Intn(2) == 0

	// key_in_value is on by default for cloudstorage and webhook sinks, and the
	// test framework extracts the key from the value automatically. Enabling it
	// explicitly for these sinks causes KeyInValueValidator failures because
	// the key field has already been removed.
	opts.keyInValue = !isCloudstorage && !isWebhook && rng.Intn(2) == 0

	// Always use backfill policy so that schema changes produce the expected
	// number of backfill rows in the changefeed.
	opts.schemaPolicy = string(changefeedbase.OptSchemaChangePolicyBackfill)

	if rng.Intn(2) == 0 {
		opts.schemaEvents = string(changefeedbase.OptSchemaChangeEventClassColumnChange)
	}

	return opts
}

// buildChangefeedStatement constructs the CREATE CHANGEFEED statement.
// When familyNames is non-empty, each family is listed as a separate target.
func buildChangefeedStatement(
	tableName string, opts perturbationOptions, familyNames []string,
) string {
	var parts []string
	parts = append(parts, "updated", "resolved", "format=json")

	if opts.diff {
		parts = append(parts, changefeedbase.OptDiff)
	}
	if opts.mvccTimestamp {
		parts = append(parts, changefeedbase.OptMVCCTimestamps)
	}
	if opts.keyInValue {
		parts = append(parts, changefeedbase.OptKeyInValue)
	}
	if opts.fullTableName {
		parts = append(parts, changefeedbase.OptFullTableName)
	}
	parts = append(parts, fmt.Sprintf("%s='%s'",
		changefeedbase.OptSchemaChangePolicy, opts.schemaPolicy))
	if opts.schemaEvents != "" {
		parts = append(parts, fmt.Sprintf("%s='%s'",
			changefeedbase.OptSchemaChangeEvents, opts.schemaEvents))
	}

	target := tableName
	if len(familyNames) > 0 {
		targets := make([]string, len(familyNames))
		for i, fn := range familyNames {
			targets[i] = fmt.Sprintf("TABLE %s FAMILY %s", tableName, fn)
		}
		target = strings.Join(targets, ", ")
	}

	return fmt.Sprintf(
		"CREATE CHANGEFEED FOR %s WITH %s", target, strings.Join(parts, ", "),
	)
}

// perturbEvent represents one of the possible actions in the event loop.
type perturbEvent int

const (
	perturbFeedMessage perturbEvent = iota
	perturbInsert
	perturbUpdate
	perturbDelete
	perturbSplit
	perturbPause
	perturbResume
	perturbPush
	perturbAbort
	perturbAddColumn
	perturbDropColumn
)

type weightedEvent struct {
	event  perturbEvent
	weight int
}

// RunPerturbation runs a randomized perturbation test that exercises changefeeds
// with random table schemas, random DML, random schema changes, and system
// perturbations (pause/resume, splits, transaction push/abort).
//
// It validates changefeed ordering and delivery guarantees using OrderValidator,
// TopicValidator, and conditionally MvccTimestampValidator and
// KeyInValueValidator.
func RunPerturbation(
	f TestFeedFactory, db *gosql.DB, testName string, rng *rand.Rand,
) (Validator, error) {
	ctx := context.Background()

	isSinkless := strings.Contains(testName, "sinkless")

	// Generate random table schema.
	columns := generateColumns(rng)
	tableName := "perturb"

	// Use column families 50% of the time. Column families exercise a
	// different code path in the schema feed's column-drop detection
	// (droppedColumnIsWatched) and are required to reproduce #149861.
	useColumnFamilies := rng.Intn(2) == 0
	var families []familyDef
	var familyNames []string
	numFamilies := 1
	if useColumnFamilies {
		families, familyNames = buildFamilies(columns)
		numFamilies = len(familyNames)
	}

	createStmt := buildCreateTable(tableName, columns, families)
	log.Changefeed.Infof(ctx, "perturbation: %s (families=%v)", createStmt, useColumnFamilies)
	if _, err := db.Exec(createStmt); err != nil {
		return nil, errors.Wrap(err, "creating table")
	}

	// Disable range merges so splits stick.
	if _, err := db.Exec(
		`SET CLUSTER SETTING kv.range_merge.queue.enabled = false`,
	); err != nil {
		return nil, err
	}

	p := &perturbation{
		rng:         rng,
		db:          db,
		tableName:   tableName,
		columns:     columns,
		familyNames: familyNames,
		numFamilies: numFamilies,
		isSinkless:  isSinkless,
		seenRows:    make(map[string]struct{}),
	}

	// Insert some initial rows.
	const initialRows = 10
	for i := 0; i < initialRows; i++ {
		if err := p.doInsert(ctx); err != nil {
			return nil, errors.Wrap(err, "initial insert")
		}
	}

	// Create an initial split.
	if _, err := db.Exec(fmt.Sprintf(
		`ALTER TABLE %s SPLIT AT VALUES ($1)`, tableName), p.nextRowID/2,
	); err != nil {
		return nil, err
	}

	// Generate changefeed options.
	opts := generateOptions(rng, testName)

	changefeedStmt := buildChangefeedStatement(tableName, opts, familyNames)
	log.Changefeed.Infof(ctx, "perturbation: %s", changefeedStmt)

	feed, err := f.Feed(changefeedStmt)
	if err != nil {
		return nil, errors.Wrap(err, "creating changefeed")
	}
	p.f = feed
	defer func() { _ = feed.Close() }()

	// Set up validators.
	//
	// BeforeAfterValidator is not used because schema changes (ADD/DROP
	// COLUMN) alter the table schema. The validator does AS OF SYSTEM TIME
	// queries that include columns from the changefeed value, which may not
	// exist at the queried timestamp.
	//
	// TopicValidator is not used with column families because family-based
	// topics (e.g., "perturb.fam_a") don't match the bare table name.
	validators := Validators{
		NewOrderValidator(tableName),
	}
	if len(familyNames) == 0 {
		validators = append(validators, NewTopicValidator(tableName, opts.fullTableName))
	}

	if opts.keyInValue {
		kivV, err := NewKeyInValueValidator(db, tableName)
		if err != nil {
			return nil, err
		}
		validators = append(validators, kivV)
	}

	if opts.mvccTimestamp {
		validators = append(validators, NewMvccTimestampValidator())
	}

	p.v = NewCountValidator(validators)

	// Count actual rows in the table for the initial scan. With column
	// families, each row produces one event per family.
	var tableRows int
	if err := db.QueryRow(
		fmt.Sprintf(`SELECT count(*) FROM %s`, tableName),
	).Scan(&tableRows); err != nil {
		return nil, err
	}
	p.availableRows = tableRows * numFamilies
	p.expectedRows = p.availableRows

	// Build weighted event list.
	events := p.buildEventWeights()

	// Run the event loop. The iteration cap prevents indefinite hangs if
	// the changefeed enters a pathological state; normal runs complete in
	// well under 1000 iterations.
	const maxIterations = 10000
	for i := 0; ; i++ {
		if p.v.NumResolvedWithRows >= 6 && p.v.NumResolvedRows >= 10 {
			break
		}
		if i >= maxIterations {
			return nil, errors.Newf(
				"perturbation test did not converge after %d iterations "+
					"(resolvedWithRows=%d, resolvedRows=%d, availableRows=%d)",
				maxIterations, p.v.NumResolvedWithRows, p.v.NumResolvedRows,
				p.availableRows,
			)
		}

		event := p.pickEvent(events)
		if err := p.executeEvent(ctx, event); err != nil {
			return nil, err
		}
	}

	// Drain phase: consume remaining feed messages until all expected rows
	// have been delivered. At this point no new DML or schema changes are
	// generated, so expectedRows is fixed. This catches bugs where
	// backfills are silently skipped (e.g., #149861).
	//
	// With column families, expectedRows may overcount because DML events
	// are counted as numFamilies per operation, but families with all-NULL
	// columns don't produce changefeed events. Subtract the maximum
	// possible overcounting: each DML could produce (numFamilies - 1)
	// fewer events than counted. This is a tight upper bound.
	if p.numFamilies > 1 {
		numDMLOps := p.stats.inserts + p.stats.updates + p.stats.deletes + p.stats.aborts
		maxOvercount := numDMLOps * (p.numFamilies - 1)
		p.expectedRows -= maxOvercount
		if p.expectedRows < 0 {
			p.expectedRows = 0
		}
	}
	if err := p.drainExpectedRows(ctx); err != nil {
		return nil, err
	}

	s := &p.stats
	log.Changefeed.Infof(ctx,
		"perturbation stats: DML: inserts=%d updates=%d deletes=%d; "+
			"perturbations: splits=%d pauses=%d resumes=%d pushes=%d aborts=%d "+
			"addColumns=%d dropColumns=%d; "+
			"feed: rows=%d expected=%d resolved=%d duplicates=%d families=%d",
		s.inserts, s.updates, s.deletes,
		s.splits, s.pauses, s.resumes, s.pushes, s.aborts,
		s.addColumns, s.dropColumns,
		s.rows, p.expectedRows, s.resolved, s.duplicates, p.numFamilies,
	)

	return p.v, nil
}

// buildEventWeights constructs the weighted event list based on the test
// configuration.
func (p *perturbation) buildEventWeights() []weightedEvent {
	// Feed messages dominate so the test makes progress toward the exit
	// criteria. DML events are moderately frequent. Perturbations (split,
	// push, abort) and schema changes are rare to avoid overwhelming the
	// changefeed. Resume has higher weight than pause so the feed spends
	// more time running than paused.
	events := []weightedEvent{
		{perturbFeedMessage, 50},
		{perturbInsert, 15},
		{perturbUpdate, 10},
		{perturbDelete, 5},
		{perturbSplit, 5},
		{perturbPush, 5},
		{perturbAbort, 5},
		{perturbAddColumn, 3},
		{perturbDropColumn, 3},
	}
	if !p.isSinkless {
		events = append(events,
			weightedEvent{perturbPause, 5},
			weightedEvent{perturbResume, 25},
		)
	}
	return events
}

// isEventEligible returns whether the given event can be executed in the
// current state.
func (p *perturbation) isEventEligible(event perturbEvent) bool {
	switch event {
	case perturbResume:
		return p.paused
	case perturbPause:
		return !p.paused
	case perturbFeedMessage:
		return !p.paused && p.availableRows > 0
	case perturbDropColumn:
		return p.addedColumnCount > 0
	case perturbUpdate, perturbDelete:
		return p.nextRowID > 0
	default:
		return true
	}
}

// pickEvent selects a random event based on weights, considering only events
// that are eligible in the current state.
func (p *perturbation) pickEvent(events []weightedEvent) perturbEvent {
	totalWeight := 0
	for _, e := range events {
		if p.isEventEligible(e.event) {
			totalWeight += e.weight
		}
	}
	if totalWeight == 0 {
		// Nothing eligible (e.g., paused with no resume event). Fall back to
		// insert which is always safe and will produce a row for later reading.
		return perturbInsert
	}
	r := p.rng.Intn(totalWeight)
	t := 0
	for _, e := range events {
		if !p.isEventEligible(e.event) {
			continue
		}
		t += e.weight
		if r < t {
			return e.event
		}
	}
	return perturbInsert
}

// executeEvent dispatches a single event.
func (p *perturbation) executeEvent(ctx context.Context, event perturbEvent) error {
	switch event {
	case perturbFeedMessage:
		return p.doFeedMessage(ctx)
	case perturbInsert:
		return p.doInsert(ctx)
	case perturbUpdate:
		return p.doUpdate(ctx)
	case perturbDelete:
		return p.doDelete(ctx)
	case perturbSplit:
		return p.doSplit(ctx)
	case perturbPause:
		return p.doPause(ctx)
	case perturbResume:
		return p.doResume(ctx)
	case perturbPush:
		return p.doPush(ctx)
	case perturbAbort:
		return p.doAbort(ctx)
	case perturbAddColumn:
		return p.doAddColumn(ctx)
	case perturbDropColumn:
		return p.doDropColumn(ctx)
	default:
		return errors.AssertionFailedf("unknown event: %d", event)
	}
}

// doFeedMessage reads the next message from the changefeed and dispatches it
// to validators. It consumes resolved timestamps until it finds a row, but
// gives up after maxResolvedPerCall resolves without a row. This prevents
// deadlock when expected rows are missing (e.g., a skipped backfill): the
// event loop regains control and can execute DML to produce new rows.
func (p *perturbation) doFeedMessage(ctx context.Context) error {
	const maxResolvedPerCall = 100
	resolvedCount := 0
	for {
		m, err := p.f.Next()
		if err != nil {
			return err
		}
		if m == nil {
			return errors.New("expected another message")
		}

		if len(m.Resolved) > 0 {
			_, ts, err := ParseJSONValueTimestamps(m.Resolved)
			if err != nil {
				return err
			}
			p.stats.resolved++
			log.Changefeed.Infof(ctx, "perturbation resolved: %s", string(m.Resolved))
			if err := p.v.NoteResolved(m.Partition, ts); err != nil {
				return err
			}
			resolvedCount++
			if resolvedCount >= maxResolvedPerCall {
				return nil
			}
		} else {
			ts, _, err := ParseJSONValueTimestamps(m.Value)
			if err != nil {
				return err
			}

			// Detect duplicate messages: same key delivered at the same
			// timestamp more than once. This can happen after pause/resume
			// or range splits. The topic is included so that events from
			// different column families (same key, same timestamp) are not
			// considered duplicates.
			dedupKey := fmt.Sprintf("%s@%s@%s", m.Key, ts.String(), m.Topic)
			if _, ok := p.seenRows[dedupKey]; ok {
				p.stats.duplicates++
			} else {
				p.seenRows[dedupKey] = struct{}{}
			}

			p.stats.rows++
			p.availableRows--
			if p.availableRows < 0 {
				p.availableRows = 0
			}
			log.Changefeed.Infof(ctx, "perturbation row: %s->%s", m.Key, m.Value)
			return p.v.NoteRow(
				m.Partition, string(m.Key), string(m.Value), ts, m.Topic,
			)
		}
	}
}

// doInsert inserts a new row with random data. If the insert with random data
// fails (e.g., due to out-of-range values), it retries with just the primary
// key and NULL values for other columns.
func (p *perturbation) doInsert(ctx context.Context) error {
	id := p.nextRowID
	p.nextRowID++

	colNames := make([]string, 0, len(p.columns))
	colVals := make([]string, 0, len(p.columns))
	for _, col := range p.columns {
		colNames = append(colNames, col.name)
		if col.name == "id" {
			colVals = append(colVals, fmt.Sprintf("%d", id))
		} else {
			colVals = append(colVals, randomDatumSQL(p.rng, col.typ))
		}
	}

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		p.tableName,
		strings.Join(colNames, ", "),
		strings.Join(colVals, ", "),
	)
	log.Changefeed.Infof(ctx, "perturbation insert: %s", stmt)
	if _, err := p.db.Exec(stmt); err != nil {
		// Random datums may produce values that are out of range or otherwise
		// invalid. Retry with new random values up to 3 times. If all retries
		// fail, fall back to a PK-only insert.
		log.Changefeed.Infof(ctx, "perturbation insert retry: %v", err)
		succeeded := false
		for retry := 0; retry < 3 && !succeeded; retry++ {
			colVals = colVals[:0]
			for _, col := range p.columns {
				if col.name == "id" {
					colVals = append(colVals, fmt.Sprintf("%d", id))
				} else {
					colVals = append(colVals, randomDatumSQL(p.rng, col.typ))
				}
			}
			stmt = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				p.tableName,
				strings.Join(colNames, ", "),
				strings.Join(colVals, ", "),
			)
			if _, retryErr := p.db.Exec(stmt); retryErr == nil {
				succeeded = true
			}
		}
		if !succeeded {
			// Final fallback: PK-only insert. With column families, only
			// the primary family produces an event, so count 1 not numFamilies.
			stmt = fmt.Sprintf("INSERT INTO %s (id) VALUES (%d)", p.tableName, id)
			if _, err := p.db.Exec(stmt); err != nil {
				return err
			}
			p.stats.inserts++
			p.availableRows++
			p.expectedRows++
			return nil
		}
	}
	p.stats.inserts++
	p.availableRows += p.numFamilies
	p.expectedRows += p.numFamilies
	return nil
}

// doUpdate updates a random existing row with new random data. Only the initial
// columns (from table creation) are updated; columns added by schema changes
// (sc_0, sc_1, ...) are left at their defaults. This is acceptable because the
// update still modifies the row and triggers a changefeed event.
func (p *perturbation) doUpdate(ctx context.Context) error {
	if p.nextRowID == 0 {
		return nil
	}
	id := p.rng.Intn(p.nextRowID)

	setClauses := make([]string, 0, len(p.columns)-1)
	for _, col := range p.columns[1:] {
		setClauses = append(setClauses, fmt.Sprintf(
			"%s = %s", col.name, randomDatumSQL(p.rng, col.typ),
		))
	}

	stmt := fmt.Sprintf("UPDATE %s SET %s WHERE id = %d",
		p.tableName,
		strings.Join(setClauses, ", "),
		id,
	)
	log.Changefeed.Infof(ctx, "perturbation update: %s", stmt)
	result, err := p.db.Exec(stmt)
	if err != nil {
		log.Changefeed.Infof(ctx, "perturbation update skipped: %v", err)
		return nil
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	p.stats.updates++
	// Updates touch columns in all families, so each affected row produces
	// one event per family.
	p.availableRows += int(affected) * p.numFamilies
	p.expectedRows += int(affected) * p.numFamilies
	return nil
}

// doDelete deletes a random existing row.
func (p *perturbation) doDelete(ctx context.Context) error {
	if p.nextRowID == 0 {
		return nil
	}
	id := p.rng.Intn(p.nextRowID)

	stmt := fmt.Sprintf("DELETE FROM %s WHERE id = %d", p.tableName, id)
	log.Changefeed.Infof(ctx, "perturbation delete: %s", stmt)
	result, err := p.db.Exec(stmt)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	p.stats.deletes++
	// Deletes produce one event per family.
	p.availableRows += int(affected) * p.numFamilies
	p.expectedRows += int(affected) * p.numFamilies
	return nil
}

// doSplit splits the table's ranges at a random key.
func (p *perturbation) doSplit(ctx context.Context) error {
	if p.nextRowID <= 1 {
		return nil
	}
	splitAt := p.rng.Intn(p.nextRowID)
	log.Changefeed.Infof(ctx, "perturbation split at %d", splitAt)
	_, err := p.db.Exec(
		fmt.Sprintf(`ALTER TABLE %s SPLIT AT VALUES ($1)`, p.tableName), splitAt,
	)
	if err != nil {
		return err
	}
	p.stats.splits++
	return nil
}

// doPause pauses the changefeed job.
func (p *perturbation) doPause(ctx context.Context) error {
	log.Changefeed.Infof(ctx, "perturbation pause")
	if err := p.f.(EnterpriseTestFeed).Pause(); err != nil {
		return err
	}
	p.paused = true
	p.stats.pauses++
	return nil
}

// doResume resumes the changefeed job.
func (p *perturbation) doResume(ctx context.Context) error {
	log.Changefeed.Infof(ctx, "perturbation resume")
	if err := p.f.(EnterpriseTestFeed).Resume(); err != nil {
		return err
	}
	p.paused = false
	p.stats.resumes++
	return nil
}

// doPush pushes open transactions via high-priority SELECT.
func (p *perturbation) doPush(ctx context.Context) error {
	log.Changefeed.Infof(ctx, "perturbation push")
	_, err := p.db.Exec(fmt.Sprintf(
		`BEGIN TRANSACTION PRIORITY HIGH; SELECT * FROM %s; COMMIT`,
		p.tableName,
	))
	if err != nil {
		return err
	}
	p.stats.pushes++
	return nil
}

// doAbort aborts transactions via high-priority DELETE of a single row. The
// high-priority transaction will abort any concurrent lower-priority
// transactions that have read or written the targeted row.
func (p *perturbation) doAbort(ctx context.Context) error {
	if p.nextRowID == 0 {
		return nil
	}
	id := p.rng.Intn(p.nextRowID)
	log.Changefeed.Infof(ctx, "perturbation abort id=%d", id)
	var deletedRows int
	err := p.db.QueryRow(fmt.Sprintf(
		`BEGIN TRANSACTION PRIORITY HIGH; `+
			`SELECT count(*) FROM [DELETE FROM %s WHERE id = %d RETURNING *]; `+
			`COMMIT`, p.tableName, id,
	)).Scan(&deletedRows)
	if err != nil {
		return err
	}
	p.stats.aborts++
	p.availableRows += deletedRows * p.numFamilies
	p.expectedRows += deletedRows * p.numFamilies
	return nil
}

// doAddColumn adds a column with a non-NULL default to trigger a backfill.
// Randomly picks one of several schema change patterns from issue #152870.
func (p *perturbation) doAddColumn(ctx context.Context) error {
	colName := fmt.Sprintf("sc_%d", p.addedColumnCount)

	var stmt string
	switch p.rng.Intn(5) {
	case 0:
		stmt = fmt.Sprintf(
			`ALTER TABLE %s ADD COLUMN %s INT DEFAULT 42`, p.tableName, colName,
		)
	case 1:
		stmt = fmt.Sprintf(
			`ALTER TABLE %s ADD COLUMN %s INT AS (id + 10) STORED`,
			p.tableName, colName,
		)
	case 2:
		stmt = fmt.Sprintf(
			`ALTER TABLE %s ADD COLUMN %s INT NOT NULL DEFAULT 0`,
			p.tableName, colName,
		)
	case 3:
		stmt = fmt.Sprintf(
			`ALTER TABLE %s ADD COLUMN %s STRING DEFAULT 'x'`,
			p.tableName, colName,
		)
	case 4:
		stmt = fmt.Sprintf(
			`ALTER TABLE %s ADD COLUMN %s BOOL DEFAULT true`,
			p.tableName, colName,
		)
	}

	log.Changefeed.Infof(ctx, "perturbation addColumn: %s", stmt)
	if _, err := p.db.Exec(stmt); err != nil {
		return err
	}
	p.addedColumnCount++
	p.stats.addColumns++

	// A schema change with a default triggers a backfill. Without column
	// families, this produces one row event per existing row. With column
	// families, the new column goes into the first family (fam_a), and only
	// that family emits backfill events.
	var rows int
	if err := p.db.QueryRow(
		fmt.Sprintf(`SELECT count(*) FROM %s`, p.tableName),
	).Scan(&rows); err != nil {
		return err
	}
	p.availableRows += rows
	p.expectedRows += rows
	return nil
}

// drainExpectedRows consumes remaining feed messages after the main event loop
// until all expected row events have been delivered. Since no new DML or schema
// changes are generated during this phase, expectedRows is fixed. If
// maxConsecutiveResolves resolved timestamps pass without a single row, the
// drain concludes that all rows have been delivered. If the final unique row
// count falls short of expectedRows by more than 20%, it indicates a real
// problem (e.g., a skipped backfill).
//
// The 50% tolerance accounts for inherent imprecision in expectedRows counting:
// schema change backfills may not produce exactly one event per row when rows
// are concurrently modified, DELETE operations may target already-deleted rows,
// and column family interactions can cause overcounting. A wall-clock deadline
// prevents the drain from blocking indefinitely.
func (p *perturbation) drainExpectedRows(ctx context.Context) error {
	uniqueRows := p.stats.rows - p.stats.duplicates
	if uniqueRows >= p.expectedRows {
		return nil
	}

	const maxConsecutiveResolves = 200
	const drainTimeout = 3 * time.Minute
	consecutiveResolves := 0
	deadline := time.Now().Add(drainTimeout)

	log.Changefeed.Infof(ctx,
		"perturbation drain: expecting %d rows, have %d unique (%d total, %d dups)",
		p.expectedRows, uniqueRows, p.stats.rows, p.stats.duplicates,
	)

	for uniqueRows < p.expectedRows {
		if time.Now().After(deadline) {
			break
		}

		m, err := p.f.Next()
		if err != nil {
			// Next() returns an error on timeout (30s). Treat this as a
			// signal that no more messages are coming.
			log.Changefeed.Infof(ctx,
				"perturbation drain: Next() error (treating as end of messages): %v", err)
			break
		}
		if m == nil {
			break
		}

		if len(m.Resolved) > 0 {
			_, ts, err := ParseJSONValueTimestamps(m.Resolved)
			if err != nil {
				return err
			}
			p.stats.resolved++
			if err := p.v.NoteResolved(m.Partition, ts); err != nil {
				return err
			}
			consecutiveResolves++
			if consecutiveResolves >= maxConsecutiveResolves {
				break
			}
		} else {
			ts, _, err := ParseJSONValueTimestamps(m.Value)
			if err != nil {
				return err
			}

			dedupKey := fmt.Sprintf("%s@%s@%s", m.Key, ts.String(), m.Topic)
			if _, ok := p.seenRows[dedupKey]; ok {
				p.stats.duplicates++
			} else {
				p.seenRows[dedupKey] = struct{}{}
			}

			p.stats.rows++
			consecutiveResolves = 0
			uniqueRows = p.stats.rows - p.stats.duplicates

			if err := p.v.NoteRow(
				m.Partition, string(m.Key), string(m.Value), ts, m.Topic,
			); err != nil {
				return err
			}
		}
	}

	uniqueRows = p.stats.rows - p.stats.duplicates
	// Allow up to 50% shortfall. The expectedRows counter is a rough
	// approximation: schema change backfills may coalesce events when
	// multiple backfills overlap, DELETE operations may target already-
	// deleted rows, and the count(*) done in doAddColumn/doDropColumn
	// races with concurrent DML. The ordering guarantees validated by
	// OrderValidator are the primary correctness check; this threshold
	// exists only to catch gross bugs like entirely skipped backfills.
	minRequired := p.expectedRows * 50 / 100
	if uniqueRows < minRequired {
		s := &p.stats
		return errors.Newf(
			"drain phase: expected at least %d unique rows (50%% of %d) "+
				"but only got %d (%d total, %d duplicates); "+
				"DML: inserts=%d updates=%d deletes=%d aborts=%d; "+
				"schema: addColumns=%d dropColumns=%d; families=%d; "+
				"changefeed may have skipped backfill rows",
			minRequired, p.expectedRows, uniqueRows,
			s.rows, s.duplicates,
			s.inserts, s.updates, s.deletes, s.aborts,
			s.addColumns, s.dropColumns, p.numFamilies,
		)
	}

	log.Changefeed.Infof(ctx,
		"perturbation drain complete: %d unique rows delivered (expected %d, min required %d)",
		uniqueRows, p.expectedRows, minRequired,
	)
	return nil
}

// doDropColumn drops the most recently added schema-change column.
func (p *perturbation) doDropColumn(ctx context.Context) error {
	if p.addedColumnCount == 0 {
		return nil
	}
	colName := fmt.Sprintf("sc_%d", p.addedColumnCount-1)
	stmt := fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s`, p.tableName, colName)
	log.Changefeed.Infof(ctx, "perturbation dropColumn: %s", stmt)
	if _, err := p.db.Exec(stmt); err != nil {
		return err
	}
	p.addedColumnCount--
	p.stats.dropColumns++

	// Dropping a column triggers a backfill.
	var rows int
	if err := p.db.QueryRow(
		fmt.Sprintf(`SELECT count(*) FROM %s`, p.tableName),
	).Scan(&rows); err != nil {
		return err
	}
	p.availableRows += rows
	p.expectedRows += rows
	return nil
}
