// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

var djangoReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)(\.(?P<point>\d+))?$`)
var djangoCockroachDBReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)$`)

var djangoSupportedTag = "cockroach-3.2.x"
var djangoCockroachDBSupportedTag = "3.2.1"

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
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		c.Start(ctx, c.All())

		version, err := fetchCockroachVersion(ctx, c, node[0], nil)
		if err != nil {
			t.Fatal(err)
		}

		err = alterZoneConfigAndClusterSettings(
			ctx, version, c, node[0], nil,
		)
		if err != nil {
			t.Fatal(err)
		}

		t.Status("cloning django and installing prerequisites")

		if err := repeatRunE(
			ctx, t, c, node, "update apt-get",
			`
				sudo add-apt-repository ppa:deadsnakes/ppa &&
				sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install make python3.7 libpq-dev python3.7-dev gcc python3-setuptools python-setuptools build-essential`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "set python3.7 as default", `
    		sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.5 1
    		sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 2
    		sudo update-alternatives --config python3`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "install pip",
			`curl https://bootstrap.pypa.io/get-pip.py | sudo -H python3.7`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install pytest",
			`sudo pip3 install pytest pytest-xdist psycopg2`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old django", `rm -rf /mnt/data1/django`,
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

		djangoCockroachDBLatestTag, err := repeatGetLatestTag(
			ctx, t, "cockroachdb", "django-cockroachdb", djangoCockroachDBReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest django-cockroachdb release is %s.", djangoCockroachDBLatestTag)
		t.L().Printf("Supported django-cockroachdb release is %s.", djangoCockroachDBSupportedTag)

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/cockroachdb/django-cockroachdb",
			"/mnt/data1/django/tests/django-cockroachdb",
			djangoCockroachDBSupportedTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "install django's dependencies", `
				cd /mnt/data1/django/tests &&
				sudo pip3 install -e .. &&
				sudo pip3 install -r requirements/py3.txt &&
				sudo pip3 install -r requirements/postgres.txt`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "install django-cockroachdb", `
					cd /mnt/data1/django/tests/django-cockroachdb/ &&
					sudo pip3 install .`,
		); err != nil {
			t.Fatal(err)
		}

		// Write the cockroach config into the test suite to use.
		if err := repeatRunE(
			ctx, t, c, node, "configuring tests to use cockroach",
			fmt.Sprintf(
				"echo \"%s\" > /mnt/data1/django/tests/cockroach_settings.py",
				cockroachDjangoSettings,
			),
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailureList, ignoredlistName, ignoredlist := djangoBlocklists.getLists(version)
		if expectedFailureList == nil {
			t.Fatalf("No django blocklist defined for cockroach version %s", version)
		}
		if ignoredlist == nil {
			t.Fatalf("No django ignorelist defined for cockroach version %s", version)
		}
		t.L().Printf("Running cockroach version %s, using blocklist %s, using ignoredlist %s",
			version, blocklistName, ignoredlistName)

		// TODO (rohany): move this to a file backed buffer if the output becomes
		//  too large.
		var fullTestResults []byte
		for _, testName := range enabledDjangoTests {
			t.Status("Running django test app ", testName)
			// Running the test suite is expected to error out, so swallow the error.
			rawResults, _ := c.RunWithBuffer(
				ctx, t.L(), node, fmt.Sprintf(djangoRunTestCmd, testName))
			fullTestResults = append(fullTestResults, rawResults...)
			t.L().Printf("Test results for app %s: %s", testName, rawResults)
			t.L().Printf("Test stdout for app %s:", testName)
			if err := c.RunL(
				ctx, t.L(), node, fmt.Sprintf("cd /mnt/data1/django/tests && cat %s.stdout", testName),
			); err != nil {
				t.Fatal(err)
			}
		}
		t.Status("collating test results")

		results := newORMTestsResults()
		results.parsePythonUnitTestOutput(fullTestResults, expectedFailureList, ignoredlist)
		results.summarizeAll(
			t, "django" /* ormName */, blocklistName, expectedFailureList, version, djangoSupportedTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:    "django",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1, spec.CPU(16)),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runDjango(ctx, t, c)
		},
	})
}

// Test results are only in stderr, so stdout is redirected and printed later.
const djangoRunTestCmd = `
cd /mnt/data1/django/tests &&
RUNNING_COCKROACH_BACKEND_TESTS=1 python3 runtests.py %[1]s --settings cockroach_settings --parallel 1 -v 2 > %[1]s.stdout
`

const cockroachDjangoSettings = `
from django.test.runner import DiscoverRunner


DATABASES = {
    'default': {
        'ENGINE': 'django_cockroachdb',
        'NAME': 'django_tests',
        'USER': 'root',
        'PASSWORD': '',
        'HOST': 'localhost',
        'PORT': 26257,
    },
    'other': {
        'ENGINE': 'django_cockroachdb',
        'NAME': 'django_tests2',
        'USER': 'root',
        'PASSWORD': '',
        'HOST': 'localhost',
        'PORT': 26257,
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
`
