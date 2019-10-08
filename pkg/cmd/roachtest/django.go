// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"regexp"
)

var djangoReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

func registerDjango(r *testRegistry) {
	runDjango := func(
		ctx context.Context,
		t *test,
		c *cluster,
	) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Start(ctx, t, c.All())

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		err = alterZoneConfigAndClusterSettings(ctx, version, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		t.Status("cloning django and installing prerequisites")

		if err := repeatRunE(
			ctx, c, node, "update apt-get",
			`
				sudo add-apt-repository ppa:deadsnakes/ppa &&
				sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install python dependencies",
			`sudo apt-get -qq install python3.6 python3.6-dev build-essential`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "set python3.6 as default", `
    		sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.5 1
    		sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.6 2
    		sudo update-alternatives --config python3`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install pip",
			`curl https://bootstrap.pypa.io/get-pip.py | sudo -H python3.6`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install postgres",
			`sudo apt-get -qq install postgresql postgresql-contrib libpq-dev`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old django", `rm -rf /mnt/data1/django`,
		); err != nil {
			t.Fatal(err)
		}

		latestTag, err := repeatGetLatestTag(
			ctx, c, "django", "django", djangoReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest Django release is %s.", latestTag)

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/django/django/",
			"/mnt/data1/django",
			latestTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/cockroachdb/cockroach-django",
			"/mnt/data1/django/tests/cockroach-django",
			// TODO (rohany): change the arguments to take in a specific branch here.
			"master",
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install cockroach-django", `
					cd /mnt/data1/django/tests/cockroach-django/ && 
					pip3 install psycopg2-binary --user && pip3 install . --user`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install django's dependencies", `
				cd /mnt/data1/django/tests &&
				pip3 install -e .. --user &&
				pip3 install -r requirements/py3.txt --user && 
				pip3 install -r requirements/postgres.txt --user`,
		); err != nil {
			t.Fatal(err)
		}

		// Write the cockroach config into the test suite to use.
		if err := repeatRunE(
			ctx, c, node, "configuring tests to use cockroach",
			fmt.Sprintf(
				"echo \"%s\" > /mnt/data1/django/tests/cockroach_settings.py",
				cockroachDjangoSettings,
			),
		); err != nil {
			t.Fatal(err)
		}

		blacklistName, expectedFailureList, ignoredlistName, ignoredlist := djangoBlacklists.getLists(version)
		if expectedFailureList == nil {
			t.Fatalf("No django blacklist defined for cockroach version %s", version)
		}
		if ignoredlist == nil {
			t.Fatalf("No django ignorelist defined for cockroach version %s", version)
		}
		c.l.Printf("Running cockroach version %s, using blacklist %s, using ignoredlist %s",
			version, blacklistName, ignoredlistName)

		// TODO (rohany): move this to a file backed buffer if the output becomes
		//  too large.
		var fullTestResults []byte
		for _, testName := range enabledDjangoTests {
			c.l.Printf("Running django test app %s", testName)
			// Running the test suite is expected to error out, so swallow the error.
			rawResults, _ := c.RunWithBuffer(
				ctx, t.l, node, fmt.Sprintf(djangoRunTestCmd, testName))
			fullTestResults = append(fullTestResults, rawResults...)
			c.l.Printf("Test Results for app %s: %s", testName, rawResults)
		}
		t.Status("collating test results")

		results := newORMTestsResults()
		results.parsePythonUnitTestOutput(fullTestResults, expectedFailureList, ignoredlist)
		results.summarizeAll(
			t, "django" /* ormName */, blacklistName,
			expectedFailureList, version, latestTag,
		)
	}

	r.Add(testSpec{
		Skip:       "django tests are still too flaky to run",
		MinVersion: "v19.2.0",
		Name:       "django",
		Cluster:    makeClusterSpec(1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runDjango(ctx, t, c)
		},
	})
}

const djangoRunTestCmd = `
cd /mnt/data1/django/tests &&
python3 runtests.py %s --settings cockroach_settings --parallel 1 -v 2
`

const cockroachDjangoSettings = `
DATABASES = {
    'default': {
        'ENGINE': 'cockroach.django',
        'NAME' : 'django_tests',
        'USER' : 'root',
        'PASSWORD' : '',
        'HOST': 'localhost',
        'PORT' : 26257,
    },
}
SECRET_KEY = 'django_tests_secret_key'
PASSWORD_HASHERS = [
    'django.contrib.auth.hashers.MD5PasswordHasher',
]
`
