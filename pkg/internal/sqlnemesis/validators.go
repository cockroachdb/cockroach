// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlnemesis

import (
	gosql "database/sql"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/errors"
)

type InspectValidator struct {
	smither *sqlsmith.Smither
}

var _ Validator = &InspectValidator{}

func (i *InspectValidator) Init(db *gosql.DB, rng *rand.Rand) error {
	// TODO: perhaps randomize setup choice.
	setup := sqlsmith.Setups[sqlsmith.SeedSetupName](rng)
	setup = append(setup, []string{
		`ALTER ROLE ALL SET enable_inspect_command = true;`,
		`SET enable_inspect_command = true;`,
	}...)
	for _, stmt := range setup {
		log(stmt)
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	var err error
	// TODO: using sqlsmith might break assumptions of other validators. Perhaps
	// we'll need to use a different database (if we keep using sqlsmith).
	i.smither, err = sqlsmith.NewSmither(
		db, rng, sqlsmith.InsUpdDelOnly(), sqlsmith.SimpleNames(),
		// TODO: perhaps set a statement_timeout instead of disabling joins.
		sqlsmith.DisableJoins(),
	)
	return err
}

func (i *InspectValidator) GenerateRandomOperation(rng *rand.Rand) (_ string, ignoreErrors bool) {
	return i.smither.Generate(), true // sqlsmith can generate invalid queries
}

func (i *InspectValidator) Validate(db *gosql.DB) error {
	var dbNames []string
	rows, err := db.Query(`SELECT database_name FROM [SHOW DATABASES]`)
	if err != nil {
		return errors.Wrap(err, "when showing databases")
	}
	defer rows.Close()
	for rows.Next() {
		var dbName string
		if err = rows.Scan(&dbName); err != nil {
			return errors.Wrap(err, "when scanning databases")
		}
		dbNames = append(dbNames, dbName)
	}
	for _, dbName := range dbNames {
		log(`INSPECT DATABASE ` + dbName)
		_, err = db.Exec(`INSPECT DATABASE ` + dbName)
		if err != nil {
			// TODO: maybe also query the inspect errors.
			return errors.Wrapf(err, "when INSPECTing %s database", dbName)
		}
	}
	return nil
}
