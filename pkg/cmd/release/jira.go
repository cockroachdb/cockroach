package main

import "context"

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
