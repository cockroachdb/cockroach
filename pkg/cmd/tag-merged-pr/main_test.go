package main

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"os"
	//"fmt"
	"testing"
)

func TestFindPullRequest(t *testing.T) {
	in := "Test #1212 pass test"
	out := "1212"
	valor := findPullRequest(in)

	assert.Equal(t, valor, out)
	assert.Equal(t, "", findPullRequest("FAIL"))
}

func TestReadToken(t *testing.T) {
	output, err := ioutil.TempFile("", "token_test_file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Remove(output.Name()); err != nil {
			t.Fatal(err)
		}
	}()

	// Writing to the test file
	text := []byte("hjk23434343j\n" +
		"SPACE")
	if _, err = output.Write(text); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}
	// Close the file
	if err := output.Close(); err != nil {
		log.Fatal(err)
	}

	token, err := readToken(output.Name())
	failToken, err := readToken("Fail")
	dat, err := ioutil.ReadFile(output.Name())

	assert.Equal(t, token, string(dat))
	assert.Equal(t, failToken, "")
}

func TestFilterPullRequests(t *testing.T) {
	input1 := `1111 Merge pull request #1111 
						 2222 Merge pull request #2222
						 3333 Merge pull request #3333
						 4444 Merge pull request #4444`

	input2 := `1111 Pull request #1111
						 2222 Merge request #2222
					   3333 Super pull request #3333
					   4444 Ultra pull request #4444`

	expectedOutput := []string{"1111","2222", "3333","4444"}

	prValue := filterPullRequests(input1)

	assert.Equal(t, prValue, expectedOutput)
	assert.Equal(t, filterPullRequests(input2), []string(nil))
}


func TestMatchVersion(t *testing.T) {
	// No other than versions tags should be accepted.
	inputf1 := "Version1.0.1"
	inputf2 := "Undefined"

	assert.Equal(t, matchVersion(inputf1), "")
	assert.Equal(t, matchVersion(inputf2), "")

	// Accepted version tags.
	input1 := "v1.0.1"
	input2 := "v2.2.2"

	assert.Equal(t, matchVersion(input1), input1)
	assert.Equal(t, matchVersion(input2), input2)

	// Skipping *-alpha.00000000 tag.
	input1 = "v1.0.1-alpha.00000000"
	input2 = "v2.2.2-alpha.00000000"

	assert.Equal(t, matchVersion(input1), "")
	assert.Equal(t, matchVersion(input2), "")

	// Checking for An alpha/beta/rc tag.
	input1 =  "v1.0.1-alpha.1"
	input2 =  "v1.0.1-beta.1"
	input3 := "v1.0.1-rc.1"

	assert.Equal(t, matchVersion(input1), input1)
	assert.Equal(t, matchVersion(input2), input2)
	assert.Equal(t, matchVersion(input3), input3)

	// Check is vX.Y.Z patch release >= .1 is first (ex: v20.1.1).
	input1 =  "v20.0.1"
	input2 =  "v22.0.1"

	assert.Equal(t, matchVersion(input1), input1)
	assert.Equal(t, matchVersion(input2), input2)

	// Checking for major releases.
	input1 =  "v1.1.0"
	input2 =  "v2.2.0"

	assert.Equal(t, matchVersion(input1), input1)
	assert.Equal(t, matchVersion(input2), input2)

}
