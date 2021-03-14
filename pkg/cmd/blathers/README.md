# Blathers

## Developing Locally

__NOTE:__ blathers is currently not compatible with bazel.

* `cd ./pkg/cmd/blathers` (use this directory as the working base).
* `base64` encode the private key for the bot, then export it: `export BLATHERS_GITHUB_PRIVATE_KEY=<your token here>`.
* Now can run the following to start the server
```
go run ./serv
```
* Then CURL your webhook request to test. Sample payloads are available in the `example_webhooks` folder.
```
curl localhost:8080/github_webhook -H 'X-Github-Event: <event>' --header "Content-Type:application/json" --data '<data>'
```
* Example data is available in `example_webhooks/`. You can run curl with them:
```
curl localhost:8080/github_webhook -H 'X-Github-Event: pull_request'  --header "Content-Type:application/json" --data "@./example_webhooks/app_webhook_synchronize"
```
## Deployment
See [https://go.crdb.dev/blathers] (go.crdb.dev/blathers).

## Blathers Inspiration
> "Eeek! A bug...! Ah, I beg your pardon! I just don't like handling these things much!"

Blathers is an owl who hates bugs.
