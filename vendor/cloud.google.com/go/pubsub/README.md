## Cloud Pub/Sub [![Go Reference](https://pkg.go.dev/badge/cloud.google.com/go/pubsub.svg)](https://pkg.go.dev/cloud.google.com/go/pubsub)

- [About Cloud Pubsub](https://cloud.google.com/pubsub/)
- [API documentation](https://cloud.google.com/pubsub/docs)
- [Go client documentation](https://pkg.go.dev/cloud.google.com/go/pubsub)
- [Complete sample programs](https://github.com/GoogleCloudPlatform/golang-samples/tree/master/pubsub)

### Example Usage

First create a `pubsub.Client` to use throughout your application:

[snip]:# (pubsub-1)
```go
client, err := pubsub.NewClient(ctx, "project-id")
if err != nil {
	log.Fatal(err)
}
```

Then use the client to publish and subscribe:

[snip]:# (pubsub-2)
```go
// Publish "hello world" on topic1.
topic := client.Topic("topic1")
res := topic.Publish(ctx, &pubsub.Message{
	Data: []byte("hello world"),
})
// The publish happens asynchronously.
// Later, you can get the result from res:
...
msgID, err := res.Get(ctx)
if err != nil {
	log.Fatal(err)
}

// Use a callback to receive messages via subscription1.
sub := client.Subscription("subscription1")
err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
	fmt.Println(m.Data)
	m.Ack() // Acknowledge that we've consumed the message.
})
if err != nil {
	log.Println(err)
}
```
