// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"context"
	"crypto/tls"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

type tpccMultiDB struct {
	*tpcc

	// dbListFile contains the list of databases that tpcc schema will be
	// created on and have the workload executed on.
	dbListFile string
	dbList     []*tree.ObjectNamePrefix
	dbNames    map[string]struct{}

	adminUrlStr string
	adminUrls   []string

	consoleAPICommandFile string
	consoleAPICommands    []string
	consoleAPITime        *histogram.NamedHistogram
	consoleAPIRetries     *histogram.NamedHistogram
	consoleAPIUsername    string
	consoleAPIPassword    string

	mu struct {
		// cachedRestAPISessions sessions which are cached so that authentication
		// not needed for each call.
		cachedRestAPISessions []string
		syncutil.Mutex
	}

	// nextDatabase selects the next database in a round robin manner.
	nextDatabase atomic.Uint64

	// initLogic executes the init logic one time.
	initLogic sync.Once
	// precreateLogic ensures database/schema creation only runs once.
	precreateLogic sync.Once
}

var tpccMultiDBMeta = workload.Meta{
	Name: `tpccmultidb`,
	Description: `TPC-C simulates a transaction processing workload` +
		` using a rich schema of multiple tables. This has been modified ` +
		` to run against multiple instances of the same schema`,
	Version:    `2.2.0`,
	RandomSeed: RandomSeed,
	New: func() workload.Generator {
		g := tpccMultiDB{}
		g.tpcc = tpccMeta.New().(*tpcc)
		g.tpcc.workloadName = "tpccmultidb"
		g.flags.Meta["txn-preamble-file"] = workload.FlagMeta{RuntimeOnly: true}
		g.flags.Meta["admin-urls"] = workload.FlagMeta{RuntimeOnly: true}
		g.flags.Meta["console-api-file"] = workload.FlagMeta{RuntimeOnly: true}
		g.flags.Meta["console-api-username"] = workload.FlagMeta{RuntimeOnly: true}
		g.flags.Meta["console-api-password"] = workload.FlagMeta{RuntimeOnly: true}
		// Support accessing multiple databases via the client driver.
		g.flags.StringVar(&g.dbListFile, "db-list-file", "", "a file containing a list of databases.")
		g.flags.StringVar(&g.adminUrlStr, "admin-urls", "", "a list of admin URLs, seperated by commas")
		g.flags.StringVar(&g.consoleAPICommandFile,
			"console-api-file",
			"",
			"a list of commands to run at the start of each txn")
		g.flags.StringVar(&g.consoleAPIUsername,
			"console-api-username",
			"",
			"username used to authenticate the console API")
		g.flags.StringVar(&g.consoleAPIPassword,
			"console-api-password",
			"",
			"password used to authenticate the console API")
		// Because this workload can create a large number of objects, the import
		// concurrent may need to be limited.
		g.flags.Int(workload.ImportDataLoaderConcurrencyFlag, 16, workload.ImportDataLoaderConcurrencyFlagDescription)
		return &g
	},
}

// getRestAPISession gets a session ID for the web API, which will attempt
// to use a cached session or generate a new one if the cache is empty. A
// function is returned to allow the session to be returned back to the cache.
func (t *tpccMultiDB) getRestAPISession(
	ctx context.Context, client *http.Client, adminUrl string,
) (string, func(), error) {
	// Used to fetch a session ID from the cache.
	getCached := func() string {
		t.mu.Lock()
		defer t.mu.Unlock()
		if len(t.mu.cachedRestAPISessions) > 0 {
			token := t.mu.cachedRestAPISessions[0]
			t.mu.cachedRestAPISessions = t.mu.cachedRestAPISessions[1:]
			return token
		}
		return ""
	}

	// Attempt to get a cached token first.
	token := getCached()
	// We did not find any cached session ID, so invoke the login
	// end poin t.
	if token == "" {
		loginUrl := fmt.Sprintf("%s/api/v2/login/", adminUrl)
		values := url.Values{
			"username": {t.consoleAPIUsername},
			"password": {t.consoleAPIPassword},
		}
		req, err := http.NewRequestWithContext(ctx, "POST", loginUrl, strings.NewReader(values.Encode()))
		if err != nil {
			return "", nil, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		loginResp, err := client.Do(req)
		if err != nil {
			return "", nil, err
		}
		defer loginResp.Body.Close()
		if loginResp.StatusCode != http.StatusOK {
			return "", nil, errors.AssertionFailedf("unexpected status from end point during auth %s (%d)",
				loginResp.Status,
				loginResp.StatusCode)
		}
		d := json.NewDecoder(loginResp.Body)
		var sessionInfo struct {
			Session string
		}
		err = d.Decode(&sessionInfo)
		if err != nil {
			return "", nil, err
		}
		token = sessionInfo.Session
	}

	return token, func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.cachedRestAPISessions = append(t.mu.cachedRestAPISessions, token)
	}, nil
}

// runWebAPICommands before txn executes any calls into the API.
func (t *tpccMultiDB) runWebAPICommands(
	ctx context.Context, targetDb string, adminUrl string,
) error {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			// Roachprod clusters may have invalid certificates.
			InsecureSkipVerify: true,
		},
	}
	client := http.Client{Transport: transport}
	defer client.CloseIdleConnections()
	const maxAPIRetries = 10
	var sessionID string
	var releaseFunc func()

	// For resilience, tolerate any internal errors from the server.
	numAttempts := int64(0)
	if err := retry.WithMaxAttempts(ctx, retry.Options{}, maxAPIRetries, func() error {
		numAttempts++
		var err error
		sessionID, releaseFunc, err = t.getRestAPISession(ctx, &client, adminUrl)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if numAttempts > 0 {
		t.consoleAPIRetries.RecordValue(numAttempts)
	}
	defer releaseFunc()

	invokeApi := func(apiCommand string) error {
		targetUrl := fmt.Sprintf("%s/%s", adminUrl, apiCommand)
		getTablesRequest, err := http.NewRequestWithContext(ctx, "GET", targetUrl, nil)
		if err != nil {
			return err
		}
		getTablesRequest.Header.Add("X-Cockroach-API-Session", sessionID)
		resp, err := client.Do(getTablesRequest)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return errors.AssertionFailedf("unexpected status from end point (%q) %s (%d)",
				apiCommand,
				resp.Status,
				resp.StatusCode)
		}
		_, err = io.ReadAll(resp.Body)
		return err
	}

	var totalAPITime time.Duration
	for _, apiCommand := range t.consoleAPICommands {
		apiCommandResolved := os.Expand(apiCommand, func(s string) string {
			switch s {
			case "targetDb":
				return targetDb
			default:
				return s
			}
		})
		// Attempt the end point multiple times in case we hit internal errors
		// due to the load on the server.
		if err := retry.WithMaxAttempts(ctx, retry.Options{}, maxAPIRetries, /*max attempts */
			func() error {
				startTime := timeutil.Now()
				if err := invokeApi(apiCommandResolved); err != nil {
					return err
				}
				// Track once the attempt has been successful.
				totalAPITime += timeutil.Since(startTime)
				return nil
			}); err != nil {
			return err
		}
	}
	// Record the time that the invocation took.
	t.consoleAPITime.Record(totalAPITime)
	return nil
}

// runBeforeEachTxn is executed at the start of each transaction
// inside normal tpcc.
func (t *tpccMultiDB) runBeforeEachTxn(ctx context.Context, tx pgx.Tx) error {
	// If multiple DBs are specified via list, select one
	// in a roundrobin manner.
	nextIdx := t.nextDatabase.Add(1)
	targetDb := "tpccmultidb"
	if t.dbList != nil {
		databaseIdx := int(nextIdx % uint64(len(t.dbList)))
		targetDb = t.dbList[databaseIdx].Catalog()
		if _, err := tx.Exec(ctx, "USE $1", t.dbList[databaseIdx].Catalog()); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET search_path = %s", t.dbList[databaseIdx].Schema())); err != nil {
			return err
		}
	}
	if len(t.adminUrls) > 0 {
		adminUrl := t.adminUrls[int(nextIdx%uint64(len(t.adminUrls)))]
		if err := t.runWebAPICommands(ctx, targetDb, adminUrl); err != nil {
			return err
		}
	}
	return nil
}

// Ops implements the Opser interface.
func (t *tpccMultiDB) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	if err := t.runInit(); err != nil {
		return workload.QueryLoad{}, err
	}
	// Only track console API times if we are going to benchmark endpoints.
	if t.consoleAPICommandFile != "" {
		if t.consoleAPITime == nil {
			t.consoleAPITime = reg.GetHandle().Get("consoleAPITime")
		}
		if t.consoleAPIRetries == nil {
			t.consoleAPIRetries = reg.GetHandle().Get("consoleAPIRetries")
		}
	}
	return t.tpcc.Ops(ctx, urls, reg)
}

// Tables implements the Generator interface.
func (t *tpccMultiDB) Tables() []workload.Table {
	existingTables := t.tpcc.Tables()
	if len(t.dbList) == 0 {
		return existingTables
	}
	// Take the normal TPCC tables and make a copy for each
	// database in the list.
	tablesPerDb := make([]workload.Table, 0, len(existingTables)*len(t.dbList))
	// We are going to order the list such that we are working on different
	// databases in a round-robin fashion.
	for _, tbl := range existingTables {
		for _, db := range t.dbList {
			tbl.ObjectPrefix = db
			tablesPerDb = append(tablesPerDb, tbl)
		}
	}
	return tablesPerDb
}

func (*tpccMultiDB) Meta() workload.Meta { return tpccMultiDBMeta }

func (t *tpccMultiDB) runInit() error {
	var err error
	t.initLogic.Do(func() {
		if t.dbListFile != "" {
			file, err := os.ReadFile(t.dbListFile)
			if err != nil {
				return
			}
			strDbList := strings.Split(string(file), "\n")
			if v := len(strDbList); v > 0 && len(strDbList[v-1]) == 0 {
				strDbList = strDbList[:v-1]
			}
			// First, sort the prefixes by database name.
			dbToBucket := make(map[string]int)
			t.dbNames = make(map[string]struct{})
			var dbNameListBuckets [][]*tree.ObjectNamePrefix
			maxBucketLen := 0
			for _, dbAndSchema := range strDbList {
				parts := strings.Split(dbAndSchema, ".")
				prefix := &tree.ObjectNamePrefix{
					CatalogName:     tree.Name(parts[0]),
					ExplicitCatalog: true,
					SchemaName:      "public",
					ExplicitSchema:  true,
				}
				if len(parts) > 1 {
					prefix.SchemaName = tree.Name(parts[1])
				}
				// Assign an index based on the database bucket.
				if _, exists := dbToBucket[parts[0]]; !exists {
					dbNameListBuckets = append(dbNameListBuckets, nil)
					dbToBucket[parts[0]] = len(dbNameListBuckets) - 1
					t.dbNames[parts[0]] = struct{}{}
				}
				bucket := dbToBucket[parts[0]]
				dbNameListBuckets[bucket] = append(dbNameListBuckets[bucket], prefix)
				maxBucketLen = max(maxBucketLen, len(dbNameListBuckets[bucket]))
			}
			// Next, generate the dbList slice by doing a round-robin across the
			// databases in the map. This minimizes deadlocks by ensuring we are
			// concurrently working on separate databases.
			t.dbList = make([]*tree.ObjectNamePrefix, 0, len(strDbList))
			for range maxBucketLen {
				for idx := range dbNameListBuckets {
					if len(dbNameListBuckets[idx]) == 0 {
						continue
					}
					t.dbList = append(t.dbList, dbNameListBuckets[idx][0])
					dbNameListBuckets[idx] = dbNameListBuckets[idx][1:]
				}
			}
		}
		// Validate that both options must be specified together.
		if (len(t.adminUrlStr) == 0) !=
			(len(t.consoleAPICommandFile) == 0) {
			err = errors.Newf("console-api-file must be specified with admin-rls must be speicifed together")
			return
		}
		if t.adminUrlStr != "" {
			t.adminUrls = strings.Split(t.adminUrlStr, ",")
		}
		if t.consoleAPICommandFile != "" {
			file, err := os.ReadFile(t.consoleAPICommandFile)
			if err != nil {
				return
			}
			strConsoleAPIList := strings.Split(string(file), "\n")
			// Skip any empty lines from this file, since we may hit connection
			// refused if no API end point is specified.
			for _, command := range strConsoleAPIList {
				command = strings.TrimSpace(command)
				if len(command) == 0 {
					continue
				}
				t.consoleAPICommands = append(t.consoleAPICommands, command)
			}
		}
		// Execute extra logic at the start of each txn.
		t.onTxnStartFns = append(t.onTxnStartFns, t.runBeforeEachTxn)

	})
	return err
}

func (t *tpccMultiDB) Hooks() workload.Hooks {
	hooks := t.tpcc.Hooks()
	oldPrecreate := hooks.PreCreate
	hooks.PreCreate = func(db *gosql.DB) error {
		if err := t.runInit(); err != nil {
			return err
		}
		ctx := context.Background()
		// Use sync.Once to ensure database/schema creation only runs once,
		// even if PreCreate is called multiple times.
		var precreateErr error
		t.precreateLogic.Do(func() {
			// Create databases and schemas in batches to avoid waiting for too many
			// schema change jobs at once. Each batch commits and waits for its jobs
			// before starting the next batch.
			const batchSize = 5000
			batchNum := 0
			currentEntry := 0
			for i := 0; i < len(t.dbList); i += batchSize {
				batchStart := timeutil.Now()
				batchNum++
				end := min(i+batchSize, len(t.dbList))
				batch := t.dbList[i:end]

				if err := crdb.ExecuteTx(ctx, db, &gosql.TxOptions{}, func(tx *gosql.Tx) error {
					// Disable autocommit before DDL to batch statements in a single
					// transaction, reducing round trips on multi-region.
					if _, err := tx.Exec("SET LOCAL autocommit_before_ddl = false"); err != nil {
						return err
					}
					for _, dbName := range batch {
						if _, err := tx.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName.Catalog())); err != nil {
							return err
						}
						if _, err := tx.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", dbName.Catalog(), dbName.Schema())); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					precreateErr = err
					return
				}
				currentEntry = end
				log.Dev.Infof(ctx, "created %d/%d databases/schemas (batch %d, %d entries in %s)",
					currentEntry, len(t.dbList), batchNum, len(batch), timeutil.Since(batchStart).Round(time.Millisecond))
			}

			// Next configure all the databases as multi-region.
			// Note: Precreates are run once per database since TPCC usess them to setup
			// multiregion.
			for dbName := range t.dbNames {
				if _, err := db.Exec("USE $1", dbName); err != nil {
					precreateErr = err
					return
				}
				if _, err := db.Exec("SET search_path = public"); err != nil {
					precreateErr = err
					return
				}
				// Run the usual TPCC pre-create logic after.
				if oldPrecreate == nil {
					continue
				}
				if err := oldPrecreate(db); err != nil {
					precreateErr = err
					return
				}
			}
			if _, err := db.Exec("RESET search_path"); err != nil {
				precreateErr = err
				return
			}
		})
		return precreateErr
	}

	// Execute the original post-load logic across all the databases.
	hooks.PostLoad = func(ctx context.Context, db *gosql.DB) error {
		grp := ctxgroup.WithContext(ctx)
		grp.SetLimit(8)
		for _, dbName := range t.dbList {
			grp.GoCtx(func(ctx context.Context) (err error) {
				conn, err := db.Conn(ctx)
				if err != nil {
					return err
				}
				defer func() {
					err = errors.WithSecondaryError(err, conn.Close())
				}()
				if _, err := conn.ExecContext(ctx, "USE $1", dbName.Catalog()); err != nil {
					return err
				}
				if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET search_path = %s", dbName.Schema())); err != nil {
					return err
				}
				return t.tpcc.postLoadImpl(ctx, conn)
			})
		}
		return grp.Wait()
	}

	return hooks
}

func init() {
	workload.Register(tpccMultiDBMeta)
}
