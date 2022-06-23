package main

import (
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

var remotesRE = regexp.MustCompile(`^(\S+)\s+(\S+)`)

// A Head object encapsulates information about the HEAD revision of a git repo.
type Head struct {
	Id             string `json:"id"`
	AuthorName     string `json:"author_name,omitempty"`
	AuthorEmail    string `json:"author_email,omitempty"`
	CommitterName  string `json:"committer_name,omitempty"`
	CommitterEmail string `json:"committer_email,omitempty"`
	Message        string `json:"message"`
}

// A Remote object encapsulates information about a remote of a git repo.
type Remote struct {
	Name string `json:"name"`
	Url  string `json:"url"`
}

// A Git object encapsulates information about a git repo.
type Git struct {
	Head    Head      `json:"head"`
	Branch  string    `json:"branch"`
	Remotes []*Remote `json:"remotes,omitempty"`
}

// collectGitInfo runs several git commands to compose a Git object.
func collectGitInfo() *Git {
	gitCmds := map[string][]string{
		"id":      {"rev-parse", "HEAD"},
		"branch":  {"rev-parse", "--abbrev-ref", "HEAD"},
		"aname":   {"log", "-1", "--pretty=%aN"},
		"aemail":  {"log", "-1", "--pretty=%aE"},
		"cname":   {"log", "-1", "--pretty=%cN"},
		"cemail":  {"log", "-1", "--pretty=%cE"},
		"message": {"log", "-1", "--pretty=%s"},
		"remotes": {"remote", "-v"},
	}
	results := map[string]string{}
	remotes := map[string]Remote{}
	gitPath, err := exec.LookPath("git")
	if err != nil {
		log.Fatal(err)
	}
	for key, args := range gitCmds {
		if key == "branch" {
			if envBranch := loadBranchFromEnv(); envBranch != "" {
				results[key] = envBranch
				continue
			}
		}

		cmd := exec.Command(gitPath, args...)
		ret, err := cmd.CombinedOutput()
		if err != nil {
			if strings.Contains(string(ret), `Not a git repository`) {
				return nil
			}
			log.Fatalf("%v: %v", err, string(ret))
		}
		s := string(ret)
		s = strings.TrimRight(s, "\n")
		results[key] = s
	}
	for _, line := range strings.Split(results["remotes"], "\n") {
		matches := remotesRE.FindAllStringSubmatch(line, -1)
		if len(matches) != 1 {
			continue
		}
		if len(matches[0]) != 3 {
			continue
		}
		name := matches[0][1]
		url := matches[0][2]
		r := Remote{
			Name: name,
			Url:  url,
		}
		remotes[name] = r
	}
	h := Head{
		Id:             results["id"],
		AuthorName:     results["aname"],
		AuthorEmail:    results["aemail"],
		CommitterName:  results["cname"],
		CommitterEmail: results["cemail"],
		Message:        results["message"],
	}
	g := &Git{
		Head:   h,
		Branch: results["branch"],
	}
	for _, r := range remotes {
		g.Remotes = append(g.Remotes, &r)
	}
	return g
}

func loadBranchFromEnv() string {
	varNames := []string{"GIT_BRANCH", "CIRCLE_BRANCH", "TRAVIS_BRANCH", "CI_BRANCH", "APPVEYOR_REPO_BRANCH"}
	for _, varName := range varNames {
		if branch := os.Getenv(varName); branch != "" {
			return branch
		}
	}
	return ""
}
