package docker

import (
	"fmt"
	"testing"
)

const tarFilePath = "upstream_artifacts/cockroach-docker-image.tar.gz"

type SqlQuery struct {
	query          string
	expectedResult string
}

// cockroachSqlArgs is passed to execute the sql query.
type dockerTest struct {
	testName           string
	buildContainerArgs []string
	containerName      string
	cockroachSqlArgs   []string
	sqlQueries         []SqlQuery
}

func TestSqlQueries(t *testing.T) {
	imageName, err := loadDockerImage(tarFilePath)
	if err != nil {
		t.Errorf(err.Error())
	}

	var dockerTests = []dockerTest{
		{
			// TODO (janexing): edit the testName to be more explicit.
			testName: "test1",
			buildContainerArgs: []string{
				"run",
				"-d",
				"--name=roach1",
				"--hostname=roach1",
				"-p",
				"26257:26257",
				"-p",
				"8080:8080",
				imageName,
				"start-single-node",
				"--insecure",
			},
			containerName:    "roach1",
			cockroachSqlArgs: []string{"--insecure"},
			sqlQueries: []SqlQuery{
				{"SELECT current_user", "current_user\nroot\n"},
			},
		},
	}

	for _, tt := range dockerTests {
		testcase := fmt.Sprintf("Running %s", tt.testName)
		t.Run(testcase, func(t *testing.T) {
			err = buildDockerContainer(tt.buildContainerArgs, tt.containerName)
			if err != nil {
				t.Errorf(err.Error())
			}
			for _, sqlQuery := range tt.sqlQueries {
				var ans string
				ans, err = executeSqlQuery(sqlQuery.query, tt.containerName, tt.cockroachSqlArgs)
				if err != nil {
					t.Errorf(err.Error())
				}
				if ans != sqlQuery.expectedResult {
					t.Errorf("got %s, want %s", ans, sqlQuery.expectedResult)
				}
			}
			if err := removeContainer(tt.containerName); err != nil {
				t.Errorf(err.Error())
			}
		})
	}
}
