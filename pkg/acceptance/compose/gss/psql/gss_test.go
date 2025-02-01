// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// "make test" would normally test this file, but it should only be tested
// within docker compose. We also can't use just "gss" here because that
// tag is reserved for the toplevel Makefile's linux-gnu build.

//go:build gss_compose

package gss

import (
	"bytes"
	"crypto/tls"
	gosql "database/sql"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/lib/pq/auth/kerberos"
)

func init() {
	pq.RegisterGSSProvider(func() (pq.GSS, error) { return kerberos.NewGSS() })
}

func TestGSS(t *testing.T) {
	connector, err := pq.NewConnector("user=root password=rootpw sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	db := gosql.OpenDB(connector)
	defer db.Close()

	tests := []struct {
		// The hba.conf file/setting.
		conf string
		user string
		// Error message of hba conf
		hbaErr string
		// Error message of gss login.
		gssErr string
		// Optionally inject an HBA identity map.
		identMap string
	}{
		{
			conf:   `host all all all gss include_realm=0 nope=1`,
			hbaErr: `unsupported option`,
		},
		{
			conf:   `host all all all gss include_realm=1`,
			hbaErr: `include_realm must be set to 0`,
		},
		{
			conf:   `host all all all gss map=ignored include_realm=1`,
			hbaErr: `include_realm must be set to 0`,
		},
		{
			conf:   `host all all all gss`,
			hbaErr: `at least one of "include_realm=0" or "map" options required`,
		},
		{
			conf:   `host all all all gss include_realm=0`,
			user:   "tester",
			gssErr: "",
		},
		{
			conf:   `host all tester all gss include_realm=0`,
			user:   "tester",
			gssErr: "",
		},
		{
			conf:   `host all nope all gss include_realm=0`,
			user:   "tester",
			gssErr: "no server.host_based_authentication.configuration entry",
		},
		{
			conf:   `host all all all gss include_realm=0 krb_realm=MY.EX`,
			user:   "tester",
			gssErr: "",
		},
		{
			conf:   `host all all all gss include_realm=0 krb_realm=NOPE.EX`,
			user:   "tester",
			gssErr: `GSSAPI realm \(MY.EX\) didn't match any configured realm`,
		},
		{
			conf:   `host all all all gss include_realm=0 krb_realm=NOPE.EX krb_realm=MY.EX`,
			user:   "tester",
			gssErr: "",
		},
		// Validate that we can use the "map" option to strip the realm
		// data. Note that the system-identity value will have been
		// normalized into a lower-case value.
		{
			conf:     `host all all all gss map=demo`,
			identMap: `demo /^(.*)@my.ex$ \1`,
			user:     "tester",
			gssErr:   "",
		},
		// Verify case-sensitivity.
		{
			conf:     `host all all all gss map=demo`,
			identMap: `demo /^(.*)@MY.EX$ \1`,
			user:     "tester",
			gssErr:   `system identity "tester@my.ex" did not map to a database role`,
		},
		// Validating the use of "map" as a filter.
		{
			conf:     `host all all all gss map=demo`,
			identMap: `demo /^(.*)@NOPE.EX$ \1`,
			user:     "tester",
			gssErr:   `system identity "tester@my.ex" did not map to a database role`,
		},
		// Check map+include_realm=0 case.
		{
			conf:     `host all all all gss include_realm=0 map=demo`,
			identMap: `demo tester remapped`,
			user:     "remapped",
			gssErr:   "",
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if _, err := db.Exec(`SET CLUSTER SETTING server.host_based_authentication.configuration = $1`, tc.conf); !IsError(err, tc.hbaErr) {
				t.Fatalf("expected err %v, got %v", tc.hbaErr, err)
			}
			if tc.hbaErr != "" {
				return
			}
			if tc.identMap != "" {
				if _, err := db.Exec(`SET CLUSTER SETTING server.identity_map.configuration = $1`, tc.identMap); err != nil {
					t.Fatalf("bad identity_map: %v", err)
				}
			}
			if _, err := db.Exec(fmt.Sprintf(`CREATE USER IF NOT EXISTS %s`, tc.user)); err != nil {
				t.Fatal(err)
			}
			t.Run("libpq", func(t *testing.T) {
				userConnector, err := pq.NewConnector(fmt.Sprintf("user=%s sslmode=require krbspn=postgres/gss_cockroach_1.gss_default", tc.user))
				if err != nil {
					t.Fatal(err)
				}
				userDB := gosql.OpenDB(userConnector)
				defer userDB.Close()
				_, err = userDB.Exec("SELECT 1")
				if !IsError(err, tc.gssErr) {
					t.Errorf("expected err %v, got %v", tc.gssErr, err)
				}
			})
			t.Run("psql", func(t *testing.T) {
				out, err := exec.Command("psql", "-c", "SELECT 1", "-U", tc.user).CombinedOutput()
				err = errors.Wrap(err, strings.TrimSpace(string(out)))
				if !IsError(err, tc.gssErr) {
					t.Errorf("expected err %v, got %v", tc.gssErr, err)
				}
			})
			t.Run("cockroach", func(t *testing.T) {
				out, err := exec.Command("/cockroach/cockroach", "sql",
					"-e", "SELECT authentication_method FROM [SHOW SESSIONS]",
					"--certs-dir", "/certs",
					// TODO(mjibson): Teach the CLI to not ask for passwords during kerberos.
					// See #51588.
					"--url", fmt.Sprintf("postgresql://%s:nopassword@cockroach:26257/?sslmode=require&krbspn=postgres/gss_cockroach_1.gss_default", tc.user),
				).CombinedOutput()
				err = errors.Wrap(err, strings.TrimSpace(string(out)))
				if !IsError(err, tc.gssErr) {
					t.Errorf("expected err %v, got %v", tc.gssErr, err)
				}
				if tc.gssErr == "" {
					if !strings.Contains(string(out), "gss") {
						t.Errorf("expected authentication_method=gss, got %s", out)
					}
				}
			})
		})
	}
}

func TestGSSFileDescriptorCount(t *testing.T) {
	t.Skip("#110194")
	rootConnector, err := pq.NewConnector("user=root password=rootpw sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	rootDB := gosql.OpenDB(rootConnector)
	defer rootDB.Close()

	if _, err := rootDB.Exec(`SET CLUSTER SETTING server.host_based_authentication.configuration = $1`, "host all all all gss include_realm=0"); err != nil {
		t.Fatal(err)
	}
	const user = "tester"
	if _, err := rootDB.Exec(fmt.Sprintf(`CREATE USER IF NOT EXISTS %s`, user)); err != nil {
		t.Fatal(err)
	}

	cookie := rootAuthCookie(t)
	const adminURL = "https://localhost:8080"
	origFDCount := openFDCount(t, adminURL, cookie)

	start := now()
	for i := 0; i < 1000; i++ {
		fmt.Println(i, time.Since(start))
		out, err := exec.Command("psql", "-c", "SELECT 1", "-U", user).CombinedOutput()
		if err != nil {
			t.Log(string(out))
			t.Fatal(err)
		}
	}

	newFDCount := openFDCount(t, adminURL, cookie)
	if origFDCount != newFDCount {
		t.Fatalf("expected open file descriptor count to be the same: %d != %d", origFDCount, newFDCount)
	}
}

func IsError(err error, re string) bool {
	if err == nil && re == "" {
		return true
	}
	if err == nil || re == "" {
		return false
	}
	matched, merr := regexp.MatchString(re, err.Error())
	if merr != nil {
		return false
	}
	return matched
}

// rootAuthCookie returns a cookie for the root user that can be used to
// authentication a web session.
func rootAuthCookie(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("/cockroach/cockroach", "auth-session", "login", "root", "--only-cookie").CombinedOutput()
	if err != nil {
		t.Log(string(out))
		t.Fatal(errors.Wrap(err, "auth-session failed"))
	}
	return strings.Trim(string(out), "\n")
}

// openFDCount returns the number of open file descriptors for the node
// at the given URL.
func openFDCount(t *testing.T, adminURL, cookie string) int {
	t.Helper()

	const fdMetricName = "sys_fd_open"
	client := http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	req, err := http.NewRequest("GET", adminURL+"/_status/vars", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Cookie", cookie)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	metrics, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if http.StatusOK != resp.StatusCode {
		t.Fatalf("GET: expected %d, but got %d", http.StatusOK, resp.StatusCode)
	}

	for _, line := range bytes.Split(metrics, []byte("\n")) {
		if bytes.HasPrefix(line, []byte(fdMetricName)) {
			fields := bytes.Fields(line)
			count, err := strconv.Atoi(string(fields[len(fields)-1]))
			if err != nil {
				t.Fatal(err)
			}
			return count
		}
	}
	t.Fatalf("metric %s not found", fdMetricName)
	return 0
}

// This is copied from pkg/util/timeutil.
// "A little copying is better than a little dependency".
// See pkg/util/timeutil/time.go for an explanation of the hacky
// implementation here.
type timeLayout struct {
	wall uint64
	ext  int64
	loc  *time.Location
}

func now() time.Time {
	t := time.Now()
	x := (*timeLayout)(unsafe.Pointer(&t))
	x.loc = nil // nil means UTC
	return t
}
