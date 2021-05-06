package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
)

func main() {
	var tokenPath, fromRef, toRef string
	flag.StringVar(&tokenPath, "token", "", "Path to GitHub token")
	flag.StringVar(&fromRef, "from", "", "From ref")
	flag.StringVar(&toRef, "to", "", "To ref")
	flag.Parse()
	// TODO: verify args
	fmt.Println(tokenPath, fromRef, toRef)
	token, err := readToken(tokenPath)
	if err != nil {
		log.Fatalln("Cannot read token from", tokenPath)
	}
	fmt.Println(token)
	refList, err := getRefs(token, fromRef, toRef)
	if err != nil {
		panic(err)
	}
	for _, ref := range refList {
		tag, err := getTag(ref)
		if err != nil {
			panic(err)
		}
		pr, _ := findPR(ref)
		fmt.Println(ref, "->", tag, "->", pr)
		labelPR(token, pr, tag)
	}
}

func readToken(path string) (string, error) {
	// TODO: io.ReadFile it
	return "token token token", nil
}

func getRefs(token, fromRef, toRef string) ([]string, error) {
	cmd := exec.Command("git", "log", "--merges", "--reverse", "--oneline",
		"--format=format:%h %s", "--ancestry-path", fmt.Sprintf("%s..%s", fromRef, toRef))
	out, err := cmd.Output()
	if err != nil {
		return []string{}, err
	}
	var shas []string
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.Contains(line, "Merge pull request") {
			continue
		}
		sha := strings.Fields(line)[0]
		shas = append(shas, sha)
	}
	return shas, nil
}

func getTag(ref string) (string, error) {
	cmd := exec.Command("git", "tag", "--contains", ref)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	// TODO: figure out which line to use
	// I'm going to use the first line
	for _, line := range strings.Fields(string(out)) {
		// TODO: use regexes or semver
		// https://github.com/cockroachdb/cockroach-operator/tree/master/hack/crdbversions
		// https://github.com/cockroachdb/cockroach-operator/tree/master/hack/versionbump
		if strings.HasPrefix(line, "v") {
			return line, nil
		}
	}
	return "", nil
}

func findPR(ref string) (int, error) {
	// TODO: implement PRNUMBER=$(git show --oneline --format="format:%h %s" $sha | head -n1 | sed -n "s/^.*Merge pull request #\\s*\\([0-9]*\\).*$/\\1/p")
	return 0, nil
}

func labelPR(token string, pr int, tag string) error {
	url := "https://api.github.com/repos/cockroachdb/cockroach/labels"
	label := fmt.Sprintf("earliest-release-%s", tag)
	payload := struct {
		Name  string `json:"name"`
		Color string `json:"color"`
	}{
		Name:  label,
		Color: "000000",
	}
	payloadJson, _ := json.Marshal(payload)
	fmt.Println(string(payloadJson))
	// TODO: convert the payload to Json io.Reader
	req, err := http.NewRequest("POST", url, bytes.NewReader(payloadJson))
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", fmt.Sprintf("token %s", token))
	res, err := http.DefaultClient.Do(req)

	if !(res.StatusCode == http.StatusCreated || res.StatusCode == http.StatusUnprocessableEntity) {
		return fmt.Errorf("cannot create label, %s", res.StatusCode)
	}
	// TODO: curl --retry 5 \
	//			 -s -f -X POST https://api.github.com/repos/cockroachdb/cockroach/issues/${PRNUMBER}/labels \
	//			 -H "Authorization: token $(cat ${GH_TOKEN_PATH})" \
	//			 -d '["'"${LABEL}"'"]'
	// lbl := []string{label}
	// lbl := "[\"label\"]"
	// TODO: marshal ^, pass as payload
	return nil
}
