package main

import (
	"log"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/blathers"
)

// TODO(blathers): consider using gin instead.
func main() {
	http.HandleFunc("/github_webhook", blathers.Server().HandleGithubWebhook)

	log.Printf("starting server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
