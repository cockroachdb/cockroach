// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blathers

import "regexp"

var teamToKeyword = map[string][]string{
	"appdev": {`(?i)\borm\b`, `ORM`, `(?i)django`, `(?i)hibernate`,
		`(?i)spring`, `(?i)\becto\b`, `(?i)activerecord`, `(?i)rails`, `(?i)sqlalchemy`,
		`(?i)sequelize`, `(?i)jooq`, `(?i)pgjdbc`, `(?i)pgx`, `(?i)libpq`,
		`(?i)cockroach-go`, `(?i)ponyorm`, `(?i)postico`, `(?i)peewee`, `(?i)flyway`,
		`(?i)hasura`, `(?i)liquibase`, `(?i)gorm`, `(?i)alembic`, `(?i)knex`,
		`(?i)prisma`, `(?i)psycopg2`, `(?i)tableplus`,
	},
	"bulk-io":        {`(?i)backup`, `(?i)import\b`, `(?i)export`, `(?i)cockroach dump`, `(?i)restore`, `(?i)changefeed`, `(?i)cdc`},
	"sql-schema":     {`(?i)(alter|drop)\s+(table|index|database)`, `(?i)(table|database|schema|index|column)\s+descriptor`},
	"kv":             {`(?i)\bkv\b`, `(?i)HLC`, `(?i)raft`},
	"sql-experience": {`(?i)sql statement`, `(?i)join\b`, `(?i)pg_`},
	"vectorized":     {`(?i)vectorize`},
	"observability":  {`(?i)admin\s+ui\b`, `(?i)web\s+ui\b`, `(?i)db\s+console`},
	"storage":        {`(?i)rocks\s*db`, `(?i)pebble`},
	"optimizer":      {`(?i)explain(^ your problem)`, `(?i)plan`},
	"cloud":          {`(?i)kubernetes`, `(?i)cloud`},
}

var teamToContacts = map[string][]string{
	"appdev":         {`rafiss`},
	"bulk-io":        {`cockroachdb/bulk-io`},
	"sql-schema":     {`ajwerner`, `jordanlewis`},
	"kv":             {`nvanbenschoten`},
	"sql-experience": {`solongordon`, `rafiss`},
	"vectorized":     {`asubiotto`},
	"observability":  {`dhartunian`},
	"storage":        {`petermattis`},
	"optimizer":      {`RaduBerinde`},
	"cloud":          {`joshimhoff`},
}

type board struct {
	owner  string
	repo   string
	name   string
	column string
}

var teamToBoards = map[string]board{
	"appdev":         {"cockroachdb", "", "AppDev", "Triage"},
	"bulk-io":        {"cockroachdb", "cockroach", "Bulk I/O Backlog", "Triage"},
	"sql-schema":     {"cockroachdb", "cockroach", "SQL Schema", "Triage"},
	"kv":             {"cockroachdb", "cockroach", "KV Backlog", "Incoming"},
	"sql-experience": {"cockroachdb", "", "SQL Experience", "Triage"},
	"vectorized":     {"cockroachdb", "cockroach", "SQL Execution", "Triage"},
	"observability":  {"cockroachdb", "cockroach", "Observability", "Backlog"},
	"storage":        {"cockroachdb", "", "Storage", "Incoming"},
	"optimizer":      {"cockroachdb", "cockroach", "SQL Optimizer", "Triage"},
}

// findTeamsFromKeywords maps from owner to a list of suspect keywords.
func findTeamsFromKeywords(body string) map[string][]string {
	// Not efficient at all, but w/e.
	foundTeamToKeywords := map[string][]string{}
	for team, keywords := range teamToKeyword {
		for _, keyword := range keywords {
			keywordRe := regexp.MustCompile(keyword)
			matches := keywordRe.FindStringSubmatch(body)
			if len(matches) > 0 {
				foundTeamToKeywords[team] = append(foundTeamToKeywords[team], matches[0])
			}
		}
	}
	return foundTeamToKeywords
}
