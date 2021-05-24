// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package movr

import (
	gosql "database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/faker"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/exp/rand"
)

// maybeFormatWithCity formats %[1]s with `city,` and %[2]s with `vehicle_city`
// if we are not using a multi-region movr.
func (g *movr) maybeFormatWithCity(fmtString string) string {
	var posOne, posTwo string
	if !g.multiRegion {
		posOne = "city, "
		posTwo = "vehicle_city, "
	}
	return fmt.Sprintf(fmtString, posOne, posTwo)
}

// Indexes into the slice returned by `Tables`.
const (
	TablesUsersIdx                    = 0
	TablesVehiclesIdx                 = 1
	TablesRidesIdx                    = 2
	TablesVehicleLocationHistoriesIdx = 3
	TablesPromoCodesIdx               = 4
	TablesUserPromoCodesIdx           = 5
)

func (g *movr) movrUsersSchema() string {
	return g.maybeFormatWithCity(`(
  id UUID NOT NULL,
  city VARCHAR NOT NULL,
  name VARCHAR NULL,
  address VARCHAR NULL,
  credit_card VARCHAR NULL,
  PRIMARY KEY (%[1]sid ASC)
)`)
}

// Indexes into the rows in movrUsers.
const (
	usersIDIdx   = 0
	usersCityIdx = 1
)

func (g *movr) movrVehiclesSchema() string {
	return g.maybeFormatWithCity(`(
  id UUID NOT NULL,
  city VARCHAR NOT NULL,
  type VARCHAR NULL,
  owner_id UUID NULL,
  creation_time TIMESTAMP NULL,
  status VARCHAR NULL,
  current_location VARCHAR NULL,
  ext JSONB NULL,
  PRIMARY KEY (%[1]sid ASC),
  INDEX vehicles_auto_index_fk_city_ref_users (%[1]sowner_id ASC)
)`,
	)
}

// Indexes into the rows in movrVehicles.
const (
	vehiclesIDIdx   = 0
	vehiclesCityIdx = 1
)

func (g *movr) movrRidesSchema() string {
	return g.maybeFormatWithCity(`(
  id UUID NOT NULL,
  city VARCHAR NOT NULL,
  vehicle_city VARCHAR NULL,
  rider_id UUID NULL,
  vehicle_id UUID NULL,
  start_address VARCHAR NULL,
  end_address VARCHAR NULL,
  start_time TIMESTAMP NULL,
  end_time TIMESTAMP NULL,
  revenue DECIMAL(10,2) NULL,
  PRIMARY KEY (%[1]sid ASC),
  INDEX rides_auto_index_fk_city_ref_users (%[1]srider_id ASC),
  INDEX rides_auto_index_fk_vehicle_city_ref_vehicles (%[2]svehicle_id ASC),
  CONSTRAINT check_vehicle_city_city CHECK (vehicle_city = city)
)`)
}

// Indexes into the rows in movrRides.
const (
	ridesIDIdx   = 0
	ridesCityIdx = 1
)

func (g *movr) movrVehicleLocationHistoriesSchema() string {
	return g.maybeFormatWithCity(`(
  city VARCHAR NOT NULL,
  ride_id UUID NOT NULL,
  "timestamp" TIMESTAMP NOT NULL,
  lat FLOAT8 NULL,
  long FLOAT8 NULL,
  PRIMARY KEY (%[1]sride_id ASC, "timestamp" ASC)
)`)
}

const movrPromoCodesSchema = `(
  code VARCHAR NOT NULL,
  description VARCHAR NULL,
  creation_time TIMESTAMP NULL,
  expiration_time TIMESTAMP NULL,
  rules JSONB NULL,
  PRIMARY KEY (code ASC)
)`

func (g *movr) movrUserPromoCodesSchema() string {
	return g.maybeFormatWithCity(`(
  city VARCHAR NOT NULL,
  user_id UUID NOT NULL,
  code VARCHAR NOT NULL,
  "timestamp" TIMESTAMP NULL,
  usage_count INT NULL,
  PRIMARY KEY (%[1]suser_id ASC, code ASC)
)`)
}

var rbrTables = []string{
	"users",
	"vehicles",
	"rides",
	"vehicle_location_histories",
	"user_promo_codes",
}

var globalTables = []string{
	"promo_codes",
}

const (
	timestampFormat = "2006-01-02 15:04:05.999999-07:00"
)

var cities = []struct {
	city   string
	region string
}{
	{city: "new york", region: "us-east1"},
	{city: "boston", region: "us-east1"},
	{city: "washington dc", region: "us-east1"},
	{city: "seattle", region: "us-west1"},
	{city: "san francisco", region: "us-west1"},
	{city: "los angeles", region: "us-west1"},
	{city: "amsterdam", region: "europe-west1"},
	{city: "paris", region: "europe-west1"},
	{city: "rome", region: "europe-west1"},
}

type movr struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed                              uint64
	users, vehicles, rides, histories cityDistributor
	numPromoCodes                     int
	ranges                            int

	multiRegion           bool
	inferCRDBRegionColumn bool
	survivalGoal          string

	creationTime time.Time

	fakerOnce sync.Once
	faker     faker.Faker
}

func init() {
	workload.Register(movrMeta)
}

var movrMeta = workload.Meta{
	Name:         `movr`,
	Description:  `MovR is a fictional vehicle sharing company`,
	Version:      `1.0.0`,
	PublicFacing: true,
	New: func() workload.Generator {
		g := &movr{}
		g.flags.FlagSet = pflag.NewFlagSet(`movr`, pflag.ContinueOnError)
		g.flags.Uint64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.IntVar(&g.users.numRows, `num-users`, 50, `Initial number of users.`)
		g.flags.IntVar(&g.vehicles.numRows, `num-vehicles`, 15, `Initial number of vehicles.`)
		g.flags.IntVar(&g.rides.numRows, `num-rides`, 500, `Initial number of rides.`)
		g.flags.BoolVar(
			&g.multiRegion,
			`multi-region`,
			false,
			`Whether to use the multi-region configuration for partitions.`,
		)
		g.flags.BoolVar(
			&g.inferCRDBRegionColumn,
			`infer-crdb-region-column`,
			true,
			`Whether to infer crdb_region for multi-region REGIONAL BY ROW columns using the city column.
Otherwise defaults to the gateway_region.`,
		)
		g.flags.StringVar(
			&g.survivalGoal,
			"survive",
			"az",
			"Survival goal for multi-region workloads. Supported goals: az, region.",
		)
		g.flags.IntVar(&g.histories.numRows, `num-histories`, 1000,
			`Initial number of ride location histories.`)
		g.flags.IntVar(&g.numPromoCodes, `num-promo-codes`, 1000, `Initial number of promo codes.`)
		g.flags.IntVar(&g.ranges, `num-ranges`, 9, `Initial number of ranges to break the tables into`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		g.creationTime = time.Date(2019, 1, 2, 3, 4, 5, 6, time.UTC)
		return g
	},
}

// Meta implements the Generator interface.
func (*movr) Meta() workload.Meta { return movrMeta }

// Flags implements the Flagser interface.
func (g *movr) Flags() workload.Flags { return g.flags }

// Hooks implements the Hookser interface.
func (g *movr) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			// Force there to be at least one user/vehicle/ride/history per city.
			// Otherwise, some cities will be empty, which means we can't construct
			// the FKs we need.
			if g.users.numRows < len(cities) {
				return errors.Errorf(`at least %d users are required`, len(cities))
			}
			if g.vehicles.numRows < len(cities) {
				return errors.Errorf(`at least %d vehicles are required`, len(cities))
			}
			if g.rides.numRows < len(cities) {
				return errors.Errorf(`at least %d rides are required`, len(cities))
			}
			if g.histories.numRows < len(cities) {
				return errors.Errorf(`at least %d histories are required`, len(cities))
			}
			return nil
		},
		PostLoad: func(db *gosql.DB) error {
			fkStmts := []string{
				g.maybeFormatWithCity(
					`ALTER TABLE vehicles ADD FOREIGN KEY
						(%[1]sowner_id) REFERENCES users (%[1]sid)`,
				),
				g.maybeFormatWithCity(
					`ALTER TABLE rides ADD FOREIGN KEY
						(%[1]srider_id) REFERENCES users (%[1]sid)`,
				),
				g.maybeFormatWithCity(
					`ALTER TABLE rides ADD FOREIGN KEY
						(%[2]svehicle_id) REFERENCES vehicles (%[1]sid)`,
				),
				g.maybeFormatWithCity(
					`ALTER TABLE vehicle_location_histories ADD FOREIGN KEY
						(%[1]sride_id) REFERENCES rides (%[1]sid)`,
				),
				g.maybeFormatWithCity(
					`ALTER TABLE user_promo_codes ADD FOREIGN KEY
						(%[1]suser_id) REFERENCES users (%[1]sid)`,
				),
			}

			for _, fkStmt := range fkStmts {
				if _, err := db.Exec(fkStmt); err != nil {
					// If the statement failed because the fk already exists,
					// ignore it. Return the error for any other reason.
					const duplicateFKErr = "columns cannot be used by multiple foreign key constraints"
					if !strings.Contains(err.Error(), duplicateFKErr) {
						return err
					}
				}
			}
			return nil
		},
		// This partitioning step is intended for a 3 region cluster, which have the localities region=us-east1,
		// region=us-west1, region=europe-west1.
		Partition: func(db *gosql.DB) error {
			if g.multiRegion {
				var survivalGoal string
				switch g.survivalGoal {
				case "az":
					survivalGoal = "ZONE"
				case "region":
					survivalGoal = "REGION"
				default:
					return errors.Errorf("unsupported survival goal: %s", g.survivalGoal)
				}
				q := fmt.Sprintf(
					`
ALTER DATABASE %[1]s SET PRIMARY REGION "us-east1";
ALTER DATABASE %[1]s ADD REGION "us-west1";
ALTER DATABASE %[1]s ADD REGION "europe-west1";
ALTER DATABASE %[1]s SURVIVE %s FAILURE
`,
					g.Meta().Name,
					survivalGoal,
				)
				if _, err := db.Exec(q); err != nil {
					return err
				}
				for _, rbrTable := range rbrTables {
					if g.inferCRDBRegionColumn {
						regionToCities := make(map[string][]string)
						for _, city := range cities {
							regionToCities[city.region] = append(regionToCities[city.region], city.city)
						}
						cityClauses := make([]string, 0, len(regionToCities))
						for region, cities := range regionToCities {
							cityClauses = append(
								cityClauses,
								fmt.Sprintf(
									`WHEN city IN (%s) THEN '%s'`,
									`'`+strings.Join(cities, `', '`)+`'`,
									region,
								),
							)
						}
						sort.Strings(cityClauses)
						if _, err := db.Exec(
							fmt.Sprintf(
								`ALTER TABLE %s ADD COLUMN crdb_region crdb_internal_region NOT NULL AS (
								CASE
									%s
									ELSE 'us-east1'
							END) STORED`,
								rbrTable,
								strings.Join(cityClauses, "\n"),
							),
						); err != nil {
							return err
						}
					}
					if _, err := db.Exec(
						fmt.Sprintf(
							`ALTER TABLE %s SET LOCALITY REGIONAL BY ROW`,
							rbrTable,
						),
					); err != nil {
						return err
					}
				}
				for _, globalTable := range globalTables {
					if _, err := db.Exec(
						fmt.Sprintf(
							`ALTER TABLE %s SET LOCALITY GLOBAL`,
							globalTable,
						),
					); err != nil {
						return err
					}
				}

				return nil
			}

			// Create us-west, us-east and europe-west partitions.
			q := `
		ALTER TABLE users PARTITION BY LIST (city) (
			PARTITION us_west VALUES IN ('seattle', 'san francisco', 'los angeles'),
			PARTITION us_east VALUES IN ('new york', 'boston', 'washington dc'),
			PARTITION europe_west VALUES IN ('amsterdam', 'paris', 'rome')
		);
		ALTER TABLE vehicles PARTITION BY LIST (city) (
			PARTITION us_west VALUES IN ('seattle', 'san francisco', 'los angeles'),
			PARTITION us_east VALUES IN ('new york', 'boston', 'washington dc'),
			PARTITION europe_west VALUES IN ('amsterdam', 'paris', 'rome')
		);
		ALTER INDEX vehicles_auto_index_fk_city_ref_users PARTITION BY LIST (city) (
			PARTITION us_west VALUES IN ('seattle', 'san francisco', 'los angeles'),
			PARTITION us_east VALUES IN ('new york', 'boston', 'washington dc'),
			PARTITION europe_west VALUES IN ('amsterdam', 'paris', 'rome')
		);
		ALTER TABLE rides PARTITION BY LIST (city) (
			PARTITION us_west VALUES IN ('seattle', 'san francisco', 'los angeles'),
			PARTITION us_east VALUES IN ('new york', 'boston', 'washington dc'),
			PARTITION europe_west VALUES IN ('amsterdam', 'paris', 'rome')
		);
		ALTER INDEX rides_auto_index_fk_city_ref_users PARTITION BY LIST (city) (
			PARTITION us_west VALUES IN ('seattle', 'san francisco', 'los angeles'),
			PARTITION us_east VALUES IN ('new york', 'boston', 'washington dc'),
			PARTITION europe_west VALUES IN ('amsterdam', 'paris', 'rome')
		);
		ALTER INDEX rides_auto_index_fk_vehicle_city_ref_vehicles PARTITION BY LIST (vehicle_city) (
			PARTITION us_west VALUES IN ('seattle', 'san francisco', 'los angeles'),
			PARTITION us_east VALUES IN ('new york', 'boston', 'washington dc'),
			PARTITION europe_west VALUES IN ('amsterdam', 'paris', 'rome')
		);
		ALTER TABLE user_promo_codes PARTITION BY LIST (city) (
			PARTITION us_west VALUES IN ('seattle', 'san francisco', 'los angeles'),
			PARTITION us_east VALUES IN ('new york', 'boston', 'washington dc'),
			PARTITION europe_west VALUES IN ('amsterdam', 'paris', 'rome')
		);
		ALTER TABLE vehicle_location_histories PARTITION BY LIST (city) (
			PARTITION us_west VALUES IN ('seattle', 'san francisco', 'los angeles'),
			PARTITION us_east VALUES IN ('new york', 'boston', 'washington dc'),
			PARTITION europe_west VALUES IN ('amsterdam', 'paris', 'rome')
		);
	`
			if _, err := db.Exec(q); err != nil {
				return err
			}

			// Alter the partitions to place replicas in the appropriate zones.
			q = `
		ALTER PARTITION us_west OF INDEX users@* CONFIGURE ZONE USING CONSTRAINTS='["+region=us-west1"]';
		ALTER PARTITION us_east OF INDEX users@* CONFIGURE ZONE USING CONSTRAINTS='["+region=us-east1"]';
		ALTER PARTITION europe_west OF INDEX users@* CONFIGURE ZONE USING CONSTRAINTS='["+region=europe-west1"]';

		ALTER PARTITION us_west OF INDEX vehicles@* CONFIGURE ZONE USING CONSTRAINTS='["+region=us-west1"]';
		ALTER PARTITION us_east OF INDEX vehicles@* CONFIGURE ZONE USING CONSTRAINTS='["+region=us-east1"]';
		ALTER PARTITION europe_west OF INDEX vehicles@* CONFIGURE ZONE USING CONSTRAINTS='["+region=europe-west1"]';

		ALTER PARTITION us_west OF INDEX rides@* CONFIGURE ZONE USING CONSTRAINTS='["+region=us-west1"]';
		ALTER PARTITION us_east OF INDEX rides@* CONFIGURE ZONE USING CONSTRAINTS='["+region=us-east1"]';
		ALTER PARTITION europe_west OF INDEX rides@* CONFIGURE ZONE USING CONSTRAINTS='["+region=europe-west1"]';

		ALTER PARTITION us_west OF INDEX user_promo_codes@* CONFIGURE ZONE USING CONSTRAINTS='["+region=us-west1"]';
		ALTER PARTITION us_east OF INDEX user_promo_codes@* CONFIGURE ZONE USING CONSTRAINTS='["+region=us-east1"]';
		ALTER PARTITION europe_west OF INDEX user_promo_codes@* CONFIGURE ZONE USING CONSTRAINTS='["+region=europe-west1"]';

		ALTER PARTITION us_west OF INDEX vehicle_location_histories@* CONFIGURE ZONE USING CONSTRAINTS='["+region=us-west1"]';
		ALTER PARTITION us_east OF INDEX vehicle_location_histories@* CONFIGURE ZONE USING CONSTRAINTS='["+region=us-east1"]';
		ALTER PARTITION europe_west OF INDEX vehicle_location_histories@* CONFIGURE ZONE USING CONSTRAINTS='["+region=europe-west1"]';
	`
			if _, err := db.Exec(q); err != nil {
				return err
			}

			// Create some duplicate indexes for the promo_codes table.
			q = `
		CREATE INDEX promo_codes_idx_us_west ON promo_codes (code) STORING (description, creation_time, expiration_time, rules);
		CREATE INDEX promo_codes_idx_europe_west ON promo_codes (code) STORING (description, creation_time, expiration_time, rules);
	`
			if _, err := db.Exec(q); err != nil {
				return err
			}

			// Apply configurations to the index for fast reads.
			q = `
		ALTER TABLE promo_codes CONFIGURE ZONE USING num_replicas = 3,
			constraints = '{"+region=us-east1": 1}',
			lease_preferences = '[[+region=us-east1]]';
		ALTER INDEX promo_codes@promo_codes_idx_us_west CONFIGURE ZONE USING
			num_replicas = 3,
			constraints = '{"+region=us-west1": 1}',
			lease_preferences = '[[+region=us-west1]]';
		ALTER INDEX promo_codes@promo_codes_idx_europe_west CONFIGURE ZONE USING
			num_replicas = 3,
			constraints = '{"+region=europe-west1": 1}',
			lease_preferences = '[[+region=europe-west1]]';
	`
			if _, err := db.Exec(q); err != nil {
				return err
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (g *movr) Tables() []workload.Table {
	g.fakerOnce.Do(func() {
		g.faker = faker.NewFaker()
	})
	tables := make([]workload.Table, 6)
	tables[TablesUsersIdx] = workload.Table{
		Name:   `users`,
		Schema: g.movrUsersSchema(),
		InitialRows: workload.Tuples(
			g.users.numRows,
			g.movrUsersInitialRow,
		),
		Splits: workload.Tuples(
			g.ranges-1,
			func(splitIdx int) []interface{} {
				row := g.movrUsersInitialRow((splitIdx + 1) * (g.users.numRows / g.ranges))
				if g.multiRegion {
					return []interface{}{row[usersIDIdx]}
				}
				// The split tuples returned must be valid primary key columns.
				return []interface{}{row[usersCityIdx], row[usersIDIdx]}
			},
		),
	}
	tables[TablesVehiclesIdx] = workload.Table{
		Name:   `vehicles`,
		Schema: g.movrVehiclesSchema(),
		InitialRows: workload.Tuples(
			g.vehicles.numRows,
			g.movrVehiclesInitialRow,
		),
		Splits: workload.Tuples(
			g.ranges-1,
			func(splitIdx int) []interface{} {
				row := g.movrVehiclesInitialRow((splitIdx + 1) * (g.vehicles.numRows / g.ranges))
				if g.multiRegion {
					return []interface{}{row[vehiclesIDIdx]}
				}
				// The split tuples returned must be valid primary key columns.
				return []interface{}{row[vehiclesCityIdx], row[vehiclesIDIdx]}
			},
		),
	}
	tables[TablesRidesIdx] = workload.Table{
		Name:   `rides`,
		Schema: g.movrRidesSchema(),
		InitialRows: workload.Tuples(
			g.rides.numRows,
			g.movrRidesInitialRow,
		),
		Splits: workload.Tuples(
			g.ranges-1,
			func(splitIdx int) []interface{} {
				row := g.movrRidesInitialRow((splitIdx + 1) * (g.rides.numRows / g.ranges))
				if g.multiRegion {
					return []interface{}{row[ridesIDIdx]}
				}
				// The split tuples returned must be valid primary key columns.
				return []interface{}{row[ridesCityIdx], row[ridesIDIdx]}
			},
		),
	}
	tables[TablesVehicleLocationHistoriesIdx] = workload.Table{
		Name:   `vehicle_location_histories`,
		Schema: g.movrVehicleLocationHistoriesSchema(),
		InitialRows: workload.Tuples(
			g.histories.numRows,
			g.movrVehicleLocationHistoriesInitialRow,
		),
	}
	tables[TablesPromoCodesIdx] = workload.Table{
		Name:   `promo_codes`,
		Schema: movrPromoCodesSchema,
		InitialRows: workload.Tuples(
			g.numPromoCodes,
			g.movrPromoCodesInitialRow,
		),
	}
	tables[TablesUserPromoCodesIdx] = workload.Table{
		Name:   `user_promo_codes`,
		Schema: g.movrUserPromoCodesSchema(),
		InitialRows: workload.Tuples(
			0,
			func(_ int) []interface{} { panic(`unimplemented`) },
		),
	}
	return tables
}

// cityDistributor deterministically maps each of numRows to a city. It also
// maps a city back to a range of rows. This allows the generator functions
// below to select random rows from the same city in another table. numRows is
// required to be at least `len(cities)`.
type cityDistributor struct {
	numRows int
}

func (d cityDistributor) cityForRow(rowIdx int) int {
	if d.numRows < len(cities) {
		panic(errors.Errorf(`a minimum of %d rows are required got %d`, len(cities), d.numRows))
	}
	numPerCity := float64(d.numRows) / float64(len(cities))
	cityIdx := int(float64(rowIdx) / numPerCity)
	return cityIdx
}

func (d cityDistributor) rowsForCity(cityIdx int) (min, max int) {
	if d.numRows < len(cities) {
		panic(errors.Errorf(`a minimum of %d rows are required got %d`, len(cities), d.numRows))
	}
	numPerCity := float64(d.numRows) / float64(len(cities))
	min = int(math.Ceil(float64(cityIdx) * numPerCity))
	max = int(math.Ceil(float64(cityIdx+1) * numPerCity))
	if min >= d.numRows {
		min = d.numRows
	}
	if max >= d.numRows {
		max = d.numRows
	}
	return min, max
}

func (d cityDistributor) randRowInCity(rng *rand.Rand, cityIdx int) int {
	min, max := d.rowsForCity(cityIdx)
	return min + rng.Intn(max-min)
}

func (g *movr) movrUsersInitialRow(rowIdx int) []interface{} {
	rng := rand.New(rand.NewSource(g.seed + uint64(rowIdx)))
	cityIdx := g.users.cityForRow(rowIdx)
	city := cities[cityIdx]

	// Make evenly-spaced UUIDs sorted in the same order as the rows.
	var id uuid.UUID
	id.DeterministicV4(uint64(rowIdx), uint64(g.users.numRows))

	return []interface{}{
		id.String(),                // id
		city.city,                  // city
		g.faker.Name(rng),          // name
		g.faker.StreetAddress(rng), // address
		randCreditCard(rng),        // credit_card
	}
}

func (g *movr) movrVehiclesInitialRow(rowIdx int) []interface{} {
	rng := rand.New(rand.NewSource(g.seed + uint64(rowIdx)))
	cityIdx := g.vehicles.cityForRow(rowIdx)
	city := cities[cityIdx]

	// Make evenly-spaced UUIDs sorted in the same order as the rows.
	var id uuid.UUID
	id.DeterministicV4(uint64(rowIdx), uint64(g.vehicles.numRows))

	vehicleType := randVehicleType(rng)
	ownerRowIdx := g.users.randRowInCity(rng, cityIdx)
	ownerID := g.movrUsersInitialRow(ownerRowIdx)[0]

	return []interface{}{
		id.String(),                            // id
		city.city,                              // city
		vehicleType,                            // type
		ownerID,                                // owner_id
		g.creationTime.Format(timestampFormat), // creation_time
		randVehicleStatus(rng),                 // status
		g.faker.StreetAddress(rng),             // current_location
		randVehicleMetadata(rng, vehicleType),  // ext
	}
}

func (g *movr) movrRidesInitialRow(rowIdx int) []interface{} {
	rng := rand.New(rand.NewSource(g.seed + uint64(rowIdx)))
	cityIdx := g.rides.cityForRow(rowIdx)
	city := cities[cityIdx]

	// Make evenly-spaced UUIDs sorted in the same order as the rows.
	var id uuid.UUID
	id.DeterministicV4(uint64(rowIdx), uint64(g.rides.numRows))

	riderRowIdx := g.users.randRowInCity(rng, cityIdx)
	riderID := g.movrUsersInitialRow(riderRowIdx)[0]
	vehicleRowIdx := g.vehicles.randRowInCity(rng, cityIdx)
	vehicleID := g.movrVehiclesInitialRow(vehicleRowIdx)[0]
	startTime := g.creationTime.Add(-time.Duration(rng.Intn(30)) * 24 * time.Hour)
	endTime := startTime.Add(time.Duration(rng.Intn(60)) * time.Hour)

	return []interface{}{
		id.String(),                       // id
		city.city,                         // city
		city.city,                         // vehicle_city
		riderID,                           // rider_id
		vehicleID,                         // vehicle_id
		g.faker.StreetAddress(rng),        // start_address
		g.faker.StreetAddress(rng),        // end_address
		startTime.Format(timestampFormat), // start_time
		endTime.Format(timestampFormat),   // end_time
		rng.Intn(100),                     // revenue
	}
}

func (g *movr) movrVehicleLocationHistoriesInitialRow(rowIdx int) []interface{} {
	rng := rand.New(rand.NewSource(g.seed + uint64(rowIdx)))
	cityIdx := g.histories.cityForRow(rowIdx)
	city := cities[cityIdx]

	rideRowIdx := g.rides.randRowInCity(rng, cityIdx)
	rideID := g.movrRidesInitialRow(rideRowIdx)[0]
	time := g.creationTime.Add(time.Duration(rowIdx) * time.Millisecond)
	lat, long := randLatLong(rng)

	return []interface{}{
		city.city,                    // city
		rideID,                       // ride_id,
		time.Format(timestampFormat), // timestamp
		lat,                          // lat
		long,                         // long
	}
}

func (g *movr) movrPromoCodesInitialRow(rowIdx int) []interface{} {
	rng := rand.New(rand.NewSource(g.seed + uint64(rowIdx)))
	code := strings.ToLower(strings.Join(g.faker.Words(rng, 3), `_`))
	code = fmt.Sprintf("%d_%s", rowIdx, code)
	description := g.faker.Paragraph(rng)
	expirationTime := g.creationTime.Add(time.Duration(rng.Intn(30)) * 24 * time.Hour)
	// TODO(dan): This is nil in the reference impl, is that intentional?
	creationTime := expirationTime.Add(-time.Duration(rng.Intn(30)) * 24 * time.Hour)
	const rulesJSON = `{"type": "percent_discount", "value": "10%"}`

	return []interface{}{
		code,           // code
		description,    // description
		creationTime,   // creation_time
		expirationTime, // expiration_time
		rulesJSON,      // rules
	}
}
