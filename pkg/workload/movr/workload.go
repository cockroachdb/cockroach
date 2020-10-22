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
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/faker"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"golang.org/x/exp/rand"
)

type rideInfo struct {
	id   string
	city string
}

type movrWorker struct {
	db           *workload.RoundRobinDB
	hists        *histogram.Histograms
	activeRides  []rideInfo
	rng          *rand.Rand
	faker        faker.Faker
	creationTime time.Time
}

func (m *movrWorker) getRandomUser(city string) (string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	var user string
	q := `
		SELECT
			IFNULL(a, b)
		FROM
			(
				SELECT
					(SELECT id FROM users WHERE city = $1 AND id > $2 ORDER BY id LIMIT 1)
						AS a,
					(SELECT id FROM users WHERE city = $1 ORDER BY id LIMIT 1) AS b
			);
		`
	err = m.db.QueryRow(q, city, id.String()).Scan(&user)
	return user, err
}

func (m *movrWorker) getRandomPromoCode() (string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	q := `
		SELECT
			IFNULL(a, b)
		FROM
			(
				SELECT
					(SELECT code FROM promo_codes WHERE code > $1 ORDER BY code LIMIT 1)
						AS a,
					(SELECT code FROM promo_codes ORDER BY code LIMIT 1) AS b
			);
		`
	var code string
	err = m.db.QueryRow(q, id.String()).Scan(&code)
	return code, err
}

func (m *movrWorker) getRandomVehicle(city string) (string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	q := `
		SELECT
			IFNULL(a, b)
		FROM
			(
				SELECT
					(SELECT id FROM vehicles WHERE city = $1 AND id > $2 ORDER BY id LIMIT 1)
						AS a,
					(SELECT id FROM vehicles WHERE city = $1 ORDER BY id LIMIT 1) AS b
			);
		`
	var vehicle string
	err = m.db.QueryRow(q, city, id.String()).Scan(&vehicle)
	return vehicle, err
}

func (m *movrWorker) readVehicles(city string) error {
	q := `SELECT city, id FROM vehicles WHERE city = $1`
	_, err := m.db.Exec(q, city)
	return err
}

func (m *movrWorker) updateActiveRides() error {
	for i, ride := range m.activeRides {
		if i >= 10 {
			break
		}
		lat, long := randLatLong(m.rng)
		q := `UPSERT INTO vehicle_location_histories VALUES ($1, $2, now(), $3, $4)`
		_, err := m.db.Exec(q, ride.city, ride.id, lat, long)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *movrWorker) addUser(id uuid.UUID, city string) error {
	q := `INSERT INTO users VALUES ($1, $2, $3, $4, $5)`
	_, err := m.db.Exec(
		q, id.String(), city, m.faker.Name(m.rng), m.faker.StreetAddress(m.rng), randCreditCard(m.rng))
	return err
}

func (m *movrWorker) createPromoCode(id uuid.UUID, _ string) error {
	expirationTime := m.creationTime.Add(time.Duration(m.rng.Intn(30)) * 24 * time.Hour)
	creationTime := expirationTime.Add(-time.Duration(m.rng.Intn(30)) * 24 * time.Hour)
	const rulesJSON = `{"type": "percent_discount", "value": "10%"}`
	q := `INSERT INTO promo_codes VALUES ($1, $2, $3, $4, $5)`
	_, err := m.db.Exec(q, id.String(), m.faker.Paragraph(m.rng), creationTime, expirationTime, rulesJSON)
	return err
}

func (m *movrWorker) applyPromoCode(id uuid.UUID, city string) error {
	user, err := m.getRandomUser(city)
	if err != nil {
		return err
	}
	code, err := m.getRandomPromoCode()
	if err != nil {
		return err
	}
	// See if the promo code has been used.
	var count int
	q := `SELECT count(*) FROM user_promo_codes WHERE city = $1 AND user_id = $2 AND code = $3`
	err = m.db.QueryRow(q, city, user, code).Scan(&count)
	if err != nil {
		return err
	}
	// If is has not been, apply the promo code.
	if count == 0 {
		q = `INSERT INTO user_promo_codes VALUES ($1, $2, $3, now(), 1)`
		_, err = m.db.Exec(q, city, user, code)
		return err
	}
	return nil
}

func (m *movrWorker) addVehicle(id uuid.UUID, city string) error {
	ownerID, err := m.getRandomUser(city)
	if err != nil {
		return err
	}
	typ := randVehicleType(m.rng)
	q := `INSERT INTO vehicles VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	_, err = m.db.Exec(
		q, id.String(), city, typ, ownerID,
		m.creationTime.Format(timestampFormat),
		randVehicleStatus(m.rng),
		m.faker.StreetAddress(m.rng),
		randVehicleMetadata(m.rng, typ),
	)
	return err
}

func (m *movrWorker) startRide(id uuid.UUID, city string) error {
	rider, err := m.getRandomUser(city)
	if err != nil {
		return err
	}
	vehicle, err := m.getRandomVehicle(city)
	if err != nil {
		return err
	}
	q := `INSERT INTO rides VALUES ($1, $2, $2, $3, $4, $5, NULL, now(), NULL, $6)`
	_, err = m.db.Exec(q, id.String(), city, rider, vehicle, m.faker.StreetAddress(m.rng), m.rng.Intn(100))
	if err != nil {
		return err
	}
	m.activeRides = append(m.activeRides, rideInfo{id.String(), city})
	return err
}

func (m *movrWorker) endRide(id uuid.UUID, city string) error {
	if len(m.activeRides) > 1 {
		ride := m.activeRides[0]
		m.activeRides = m.activeRides[1:]
		q := `UPDATE rides SET end_address = $3, end_time = now() WHERE city = $1 AND id = $2`
		_, err := m.db.Exec(q, ride.city, ride.id, m.faker.StreetAddress(m.rng))
		return err
	}
	return nil
}

func (m *movrWorker) generateWorkSimulation() func(context.Context) error {
	const readPercentage = 0.95
	movrWorkloadFns := []struct {
		weight float32
		key    string
		work   func(uuid.UUID, string) error
	}{
		{
			weight: 0.03,
			key:    "createPromoCode",
			work:   m.createPromoCode,
		},
		{
			weight: 0.1,
			key:    "applyPromoCode",
			work:   m.applyPromoCode,
		},
		{
			weight: 0.3,
			key:    "addUser",
			work:   m.addUser,
		},
		{
			weight: 0.1,
			key:    "addVehicle",
			work:   m.addVehicle,
		},
		{
			weight: 0.4,
			key:    "startRide",
			work:   m.startRide,
		},
		{
			weight: 0.07,
			key:    "endRide",
			work:   m.endRide,
		},
	}

	sum := float32(0.0)
	for _, s := range movrWorkloadFns {
		sum += s.weight
	}

	runAndRecord := func(key string, work func() error) error {
		start := timeutil.Now()
		err := work()
		elapsed := timeutil.Since(start)
		if err == nil {
			m.hists.Get(key).Record(elapsed)
		}
		return err
	}

	return func(ctx context.Context) error {
		activeCity := randCity(m.rng)
		id, err := uuid.NewV4()
		if err != nil {
			return err
		}
		// Our workload is as follows: with 95% chance, do a simple read operation.
		// Else, update all active vehicle locations, then pick a random "write" operation
		// weighted by the weights in movrWorkloadFns.
		if m.rng.Float64() <= readPercentage {
			return runAndRecord("readVehicles", func() error {
				return m.readVehicles(activeCity)
			})
		}
		err = runAndRecord("updateActiveRides", func() error {
			return m.updateActiveRides()
		})
		if err != nil {
			return err
		}
		randVal := m.rng.Float32() * sum
		w := float32(0.0)
		for _, s := range movrWorkloadFns {
			w += s.weight
			if w >= randVal {
				return runAndRecord(s.key, func() error {
					return s.work(id, activeCity)
				})
			}
		}
		panic("unreachable")
	}
}

// Ops implements the Opser interface
func (m *movr) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	// Initialize the faker in case it hasn't been setup already.
	m.fakerOnce.Do(func() {
		m.faker = faker.NewFaker()
	})
	sqlDatabase, err := workload.SanitizeUrls(m, m.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	db, err := workload.NewRoundRobinDB(urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	worker := movrWorker{
		db:           db,
		rng:          rand.New(rand.NewSource(m.seed)),
		faker:        m.faker,
		creationTime: m.creationTime,
		activeRides:  []rideInfo{},
		hists:        reg.GetHandle(),
	}
	ql.WorkerFns = append(ql.WorkerFns, worker.generateWorkSimulation())

	return ql, nil
}
