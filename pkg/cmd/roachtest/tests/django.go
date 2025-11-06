// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

var djangoReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)(\.(?P<point>\d+))?$`)
var djangoCockroachDBReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)$`)

// WARNING: DO NOT MODIFY the name of the below constant/variable without approval from the docs team.
// This is used by docs automation to produce a list of supported versions for ORM's.
var djangoSupportedTag = "cockroach-4.1.x"
var djangoCockroachDBSupportedTag = "4.1.*"

func registerDjango(r registry.Registry) {
	runDjango := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Start(ctx, t.L(), option.NewStartOpts(sqlClientsInMemoryDB), install.MakeClusterSettings(), c.All())

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		err = alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		t.Status("cloning django and installing prerequisites")

		if err := repeatRunE(
			ctx, t, c, node, "update apt-get",
			`sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install make python3 libpq-dev python3-dev gcc python3-venv python3-pip python3-setuptools build-essential python3-distutils-extra python3-apt libmemcached-dev`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "create virtualenv", `python3 -m venv --clear venv`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install pytest",
			`source venv/bin/activate && pip3 install pytest pytest-xdist psycopg2`,
		); err != nil {
			t.Fatal(err)
		}

		djangoCockroachDBLatestTag, err := repeatGetLatestTag(
			ctx, t, "cockroachdb", "django-cockroachdb", djangoCockroachDBReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest django-cockroachdb release is %s.", djangoCockroachDBLatestTag)
		t.L().Printf("Supported django-cockroachdb release is %s.", djangoCockroachDBSupportedTag)

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install django-cockroachdb",
			fmt.Sprintf(
				`source venv/bin/activate && pip3 install django-cockroachdb==%s`,
				djangoCockroachDBSupportedTag,
			),
		); err != nil {
			t.Fatal(err)
		}

		djangoLatestTag, err := repeatGetLatestTag(
			ctx, t, "django", "django", djangoReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest Django release is %s.", djangoLatestTag)
		t.L().Printf("Supported Django release is %s.", djangoSupportedTag)

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/timgraham/django/",
			"/mnt/data1/django",
			djangoSupportedTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "install django's dependencies", `
				if [ $(arch) == "s390x" ]; then
					# s390x doesn't have a prebuilt wheel for bcrypt, so we need
					# to build it from source. This requires rust.
					# Install rust and set the default toolchain to stable.
					curl https://sh.rustup.rs -sSf | sh -s -- -y
					source $HOME/.cargo/env

					# s390x doesn't have a prebuilt wheel for pillow, so we need
					# to build it from source. This requires libjpeg-dev.
					sudo apt-get install -y libjpeg-dev
				fi
				source venv/bin/activate &&
				cd /mnt/data1/django/tests &&
				pip3 install -e .. &&
				pip3 install -r requirements/py3.txt &&
				pip3 install -r requirements/postgres.txt`,
		); err != nil {
			t.Fatal(err)
		}

		// Write the cockroach config into the test suite to use.
		settings := cockroachDjangoSettings
		if c.Architecture() == "fips" {
			t.L().Printf("Setting up FIPS build settings for architecture %s", c.Architecture())
			settings = cockroachDjangoFIPSSettings
		}
		if err := repeatRunE(
			ctx, t, c, node, "configuring tests to use cockroach",
			fmt.Sprintf(
				"echo \"%s\" > /mnt/data1/django/tests/cockroach_settings.py",
				settings,
			),
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, ignoredlistName := "djangoBlocklist", "djangoIgnoreList"
		t.L().Printf("Running cockroach version %s, using blocklist %s, using ignoredlist %s",
			version, blocklistName, ignoredlistName)

		// TODO (rohany): move this to a file backed buffer if the output becomes
		//  too large.
		var fullTestResults []byte
		for _, testName := range enabledDjangoTests {
			t.Status("Running django test app ", testName)
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(node), fmt.Sprintf(djangoRunTestCmd, testName))

			// Fatal for a roachprod or transient error. A roachprod error is when result.Err==nil.
			// Proceed for any other (command) errors
			if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
				t.Fatal(err)
			}

			rawResults := []byte(result.Stdout + result.Stderr)

			fullTestResults = append(fullTestResults, rawResults...)
			t.L().Printf("Test results for app %s: %s", testName, rawResults)
			t.L().Printf("Test stdout for app %s:", testName)
			if err := c.RunE(
				ctx, option.WithNodes(node), fmt.Sprintf("cd /mnt/data1/django/tests && cat %s.stdout", testName),
			); err != nil {
				t.Fatal(err)
			}
		}
		t.Status("collating test results")

		results := newORMTestsResults()
		results.parsePythonUnitTestOutput(fullTestResults, djangoBlocklist, djangoIgnoreList)
		results.summarizeAll(
			t, "django" /* ormName */, blocklistName, djangoBlocklist, version, djangoSupportedTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:             "django",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1, spec.CPU(16)),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly, registry.ORM),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runDjango(ctx, t, c)
		},
	})
}

// Test results are only in stderr, so stdout is redirected and printed later.
const djangoRunTestCmd = `
source venv/bin/activate && cd /mnt/data1/django/tests &&
python3 runtests.py %[1]s --settings cockroach_settings -v 2 > %[1]s.stdout
`

var cockroachDjangoSettings = fmt.Sprintf(`
from django.test.runner import DiscoverRunner

DATABASES = {
    'default': {
        'ENGINE': 'django_cockroachdb',
        'NAME': 'django_tests',
        'USER': '%[1]s',
        'PASSWORD': '%[2]s',
        'HOST': 'localhost',
        'PORT': {pgport:1},
    },
    'other': {
        'ENGINE': 'django_cockroachdb',
        'NAME': 'django_tests2',
        'USER': '%[1]s',
        'PASSWORD': '%[2]s',
        'HOST': 'localhost',
        'PORT': {pgport:1},
    },
}
SECRET_KEY = 'django_tests_secret_key'
PASSWORD_HASHERS = [
    'django.contrib.auth.hashers.MD5PasswordHasher',
]
TEST_RUNNER = '.cockroach_settings.NonDescribingDiscoverRunner'
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'

class NonDescribingDiscoverRunner(DiscoverRunner):
    def get_test_runner_kwargs(self):
        return {
            'failfast': self.failfast,
            'resultclass': self.get_resultclass(),
            'verbosity': self.verbosity,
            'descriptions': False,
        }

USE_TZ = False
`, install.DefaultUser, install.DefaultPassword)

var cockroachDjangoFIPSSettings = fmt.Sprintf(`
# --- Allow MD5/SHA1 for *tests* under FIPS (non-security use), but be compatible with builds
#     where hashlib doesn't accept the 'usedforsecurity' kwarg. ---
import hashlib, hmac

def _wrap_nonsec(name):
    orig = getattr(hashlib, name, None)
    if orig is None:
        return
    def wrapper(*args, **kwargs):
        # Try with 'usedforsecurity=False'; if not supported, retry without it.
        kw = dict(kwargs)
        kw.setdefault('usedforsecurity', False)
        try:
            return orig(*args, **kw)
        except TypeError:
            return orig(*args, **kwargs)
    wrapper.__name__ = getattr(orig, '__name__', name)
    setattr(hashlib, name, wrapper)

for _n in ('md5', 'sha1'):
    _wrap_nonsec(_n)

_orig_new = hashlib.new
def _new(name, *args, **kwargs):
    kw = dict(kwargs)
    if str(name).lower() in ('md5', 'sha1'):
        kw.setdefault('usedforsecurity', False)
    try:
        return _orig_new(name, *args, **kw)
    except TypeError:
        return _orig_new(name, *args, **kwargs)
hashlib.new = _new

# --- Make hashlib.scrypt importable *and* falsey so scrypt tests skip cleanly ---
class _FalseyScrypt:
    def __bool__(self): return False
    def __call__(self, *a, **k):
        raise NotImplementedError('scrypt is unavailable under FIPS')
hashlib.scrypt = _FalseyScrypt()

# --- Pure-Python PBKDF2 that never calls digest() directly ---
import hashlib, hmac

def _resolve_digest_name(digest):
    # Accept 'sha1'/'sha256' strings, hashlib constructors, or wrapped callables.
    if digest is None:
        return 'sha256'
    if isinstance(digest, str):
        return digest.lower()
    # Try to infer from the function name (works even with wrapped funcs)
    name = getattr(digest, '__name__', '').lower()
    for cand in ('sha1', 'sha256', 'sha512', 'md5'):
        if cand in name:
            return cand
    # Last resort: introspect without args via hashlib.new on the name attribute if present
    # but avoid calling digest() directly (that caused the TypeError).
    return 'sha256'  # safe default for tests if unknown

def _py_pbkdf2(password, salt, iterations, dklen=None, digest=None):
    alg = _resolve_digest_name(digest)

    if isinstance(password, str):
        password = password.encode()
    if isinstance(salt, str):
        salt = salt.encode()

    # Build a constructor callable for HMAC that returns a fresh hash object.
    def digest_cons():
        # Our patched hashlib.new will add usedforsecurity=False for md5/sha1
        # and gracefully fall back if the kwarg isn't supported.
        return hashlib.new(alg)

    hlen = digest_cons().digest_size
    if dklen is None:
        dklen = hlen

    def PRF(msg):
        return hmac.new(password, msg, digest_cons).digest()

    out = bytearray()
    blocks = (dklen + hlen - 1) // hlen
    for i in range(1, blocks + 1):
        u = PRF(salt + i.to_bytes(4, 'big'))
        t = bytearray(u)
        for _ in range(1, iterations):
            u = PRF(u)
            for j in range(hlen):
                t[j] ^= u[j]
        out.extend(t)
    return bytes(out[:dklen])

# Install it
from django.utils import crypto as _crypto
_crypto.pbkdf2 = _py_pbkdf2


%s
`, cockroachDjangoSettings)

// var cockroachDjangoSettings = fmt.Sprintf(`
// import base64, hashlib, secrets
// from django.test.runner import DiscoverRunner
// from django.contrib.auth.hashers import PBKDF2PasswordHasher
// from django.utils.crypto import pbkdf2

// # FIPS 140-3 compliance: use PBKDF2 with SHA256 as the default hasher.
// class FastPBKDF2(PBKDF2PasswordHasher):
//     digest = hashlib.sha256
//     iterations = 6000
//     def salt(self) -> str:
//         # 16 random bytes => 32 hex chars (128-bit salt)
//         return secrets.token_hex(16)

//     def _normalize_pwd_bytes(self, password) -> bytes:
//         pwd = password.encode() if isinstance(password, str) else password
//         return hashlib.sha256(pwd).digest() if len(pwd) < 14 else pwd

//     def encode(self, password, salt, iterations=None):
//         iterations = iterations or self.iterations
//         dk = pbkdf2(self._normalize_pwd_bytes(password), salt, iterations, digest=self.digest)
//         return '$'.join((self.algorithm, str(iterations), salt, base64.b64encode(dk).decode('ascii').strip()))

// DATABASES = {
//     'default': {
//         'ENGINE': 'django_cockroachdb',
//         'NAME': 'django_tests',
//         'USER': '%[1]s',
//         'PASSWORD': '%[2]s',
//         'HOST': 'localhost',
//         'PORT': {pgport:1},
//     },
//     'other': {
//         'ENGINE': 'django_cockroachdb',
//         'NAME': 'django_tests2',
//         'USER': '%[1]s',
//         'PASSWORD': '%[2]s',
//         'HOST': 'localhost',
//         'PORT': {pgport:1},
//     },
// }
// SECRET_KEY = 'django_tests_secret_key'
// PASSWORD_HASHERS = [
//     '.cockroach_settings.FastPBKDF2',
// ]
// TEST_RUNNER = '.cockroach_settings.NonDescribingDiscoverRunner'
// DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'

// class NonDescribingDiscoverRunner(DiscoverRunner):
//     def get_test_runner_kwargs(self):
//         return {
//             'failfast': self.failfast,
//             'resultclass': self.get_resultclass(),
//             'verbosity': self.verbosity,
//             'descriptions': False,
//         }

// USE_TZ = False
// `, install.DefaultUser, install.DefaultPassword)
