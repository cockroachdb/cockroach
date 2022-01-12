/*
	Below is copy/pasted JIRA-relevant snippets from
	https://github.com/cockroachdb/cockroach/pull/74570.

	TODO(celia)
	- [x] use go-jira tool to post / get a test issue issue
	- [ ] use test code to fill in below snippets
	- [ ] create a JIRA bot shared account:
		- [ ] generate auth token
		- [ ] save in 1Password
	- Q: where do we store the JIRA auth info? and how it get passed through to this code?
 */
package release

import "context"

type metadata struct {
	Version   string `json:"version"`
	Tag       string `json:"tag"`
	Branch    string `json:"branch"`
	SHA       string `json:"sha"`
	Timestamp string `json:"timestamp"`
}

type jiraTicket struct {
	url    string
	ticket string
}

func postToJira(ctx context.Context, meta metadata) (jiraTicket, error) {
	// TODO: implement
	ticket := jiraTicket{
		url:    "https://....",
		ticket: "RE-555",
	}
	return ticket, nil
}
