// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/term"
)

// BcryptCost is the cost to use when hashing passwords. It is exposed for
// testing.
//
// BcryptCost should increase along with computation power.
// For estimates, see: http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt
// For now, we use the library's default cost.
var BcryptCost = bcrypt.DefaultCost

// ErrEmptyPassword indicates that an empty password was attempted to be set.
var ErrEmptyPassword = errors.New("empty passwords are not permitted")

// ErrPasswordTooShort indicates that a client provided a password
// that was too short according to policy.
var ErrPasswordTooShort = errors.New("password too short")

var sha256NewSum = sha256.New().Sum(nil)

// TODO(mjibson): properly apply SHA-256 to the password. The current code
// erroneously appends the SHA-256 of the empty hash to the unhashed password
// instead of actually hashing the password. Fixing this requires a somewhat
// complicated backwards compatibility dance. This is not a security issue
// because the round of SHA-256 was only intended to achieve a fixed-length
// input to bcrypt; it is bcrypt that provides the cryptographic security, and
// bcrypt is correctly applied.
func appendEmptySha256(password string) []byte {
	// In the past we incorrectly called the hash.Hash.Sum method. That
	// method uses its argument as a place to put the current hash:
	// it does not add its argument to the current hash. Thus, using
	// h.Sum([]byte(password))) is the equivalent to the below append.
	return append([]byte(password), sha256NewSum...)
}

// CompareHashAndPassword tests that the provided bytes are equivalent to the
// hash of the supplied password. If they are not equivalent, returns an
// error.
func CompareHashAndPassword(ctx context.Context, hashedPassword []byte, password string) error {
	sem := getBcryptSem(ctx)
	alloc, err := sem.Acquire(ctx, 1)
	if err != nil {
		return err
	}
	defer alloc.Release()
	return bcrypt.CompareHashAndPassword(hashedPassword, appendEmptySha256(password))
}

// HashPassword takes a raw password and returns a bcrypt hashed password.
func HashPassword(ctx context.Context, password string) ([]byte, error) {
	sem := getBcryptSem(ctx)
	alloc, err := sem.Acquire(ctx, 1)
	if err != nil {
		return nil, err
	}
	defer alloc.Release()
	return bcrypt.GenerateFromPassword(appendEmptySha256(password), BcryptCost)
}

// PromptForPassword prompts for a password.
// This is meant to be used when using a password.
func PromptForPassword() (string, error) {
	fmt.Print("Enter password: ")
	password, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", err
	}
	// Make sure stdout moves on to the next line.
	fmt.Print("\n")

	return string(password), nil
}

// MinPasswordLength is the cluster setting that configures the
// minimum SQL password length.
var MinPasswordLength = settings.RegisterIntSetting(
	"server.user_login.min_password_length",
	"the minimum length accepted for passwords set in cleartext via SQL. "+
		"Note that a value lower than 1 is ignored: passwords cannot be empty in any case.",
	1,
	settings.NonNegativeInt,
)

// bcryptSemOnce wraps a semaphore that limits the number of concurrent calls
// to the bcrypt hash functions. This is needed to avoid the risk of a
// DoS attacks by malicious users or broken client apps that would
// starve the server of CPU resources just by computing bcrypt hashes.
//
// We use a sync.Once to delay the creation of the semaphore to the
// first time the password functions are used. This gives a chance to
// the server process to update GOMAXPROCS before we compute the
// maximum amount of concurrency for the semaphore.
var bcryptSemOnce struct {
	sem  *quotapool.IntPool
	once sync.Once
}

// envMaxBcryptConcurrency allows a user to override the semaphore
// configuration using an environment variable.
// If the env var is set to a value >= 1, that value is used.
// Otherwise, a default is computed from the configure GOMAXPROCS.
var envMaxBcryptConcurrency = envutil.EnvOrDefaultInt("COCKROACH_MAX_BCRYPT_CONCURRENCY", 0)

// getBcryptSem retrieves the bcrypt semaphore.
func getBcryptSem(ctx context.Context) *quotapool.IntPool {
	bcryptSemOnce.once.Do(func() {
		var n int
		if envMaxBcryptConcurrency >= 1 {
			// The operator knows better. Use what they tell us to use.
			n = envMaxBcryptConcurrency
		} else {
			// We divide by 8 so that the max CPU usage of bcrypt checks
			// never exceeds ~10% of total CPU resources allocated to this
			// process.
			n = runtime.GOMAXPROCS(-1) / 8
		}
		if n < 1 {
			n = 1
		}
		log.VInfof(ctx, 1, "configured maximum bcrypt concurrency: %d", n)
		bcryptSemOnce.sem = quotapool.NewIntPool("bcrypt", uint64(n))
	})
	return bcryptSemOnce.sem
}
