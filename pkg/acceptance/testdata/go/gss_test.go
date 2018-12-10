package gss

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

func TestGSS(t *testing.T) {
	connector, err := pq.NewConnector("user=root sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	os.Setenv("KRB5_KTNAME", "/etc/keytab")

	tests := []struct {
		// The hba.conf file/setting.
		conf string
		user string
		// Error message of gss login.
		gssErr string
	}{
		{
			conf: `host all all all gss`,
			user: "tester@MY.EX",
		},
		{
			conf: `host all tester@MY.EX all gss`,
			user: "tester@MY.EX",
		},
		{
			conf:   `host all nope@MY.EX all gss`,
			user:   "tester@MY.EX",
			gssErr: "no server.host_based_authentication.configuration entry",
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if _, err := db.Exec(fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'`, tc.user)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`SET CLUSTER SETTING server.host_based_authentication.configuration = $1`, tc.conf); err != nil {
				t.Fatal(err)
			}

			out, err := exec.Command("psql", "-c", "SELECT 1", "-U", tc.user).CombinedOutput()
			err = errors.Wrap(err, strings.TrimSpace(string(out)))
			if !IsError(err, tc.gssErr) {
				t.Errorf("expected err %v, got %v", tc.gssErr, err)
			}
		})
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
