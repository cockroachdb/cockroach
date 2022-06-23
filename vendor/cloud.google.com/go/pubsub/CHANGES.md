# Changes

## [1.16.0](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.15.0...pubsub/v1.16.0) (2021-08-24)


### Features

* **pubsub:** add topic message retention duration ([#4520](https://www.github.com/googleapis/google-cloud-go/issues/4520)) ([0440336](https://www.github.com/googleapis/google-cloud-go/commit/0440336c988a4401cbdb5d85a8cc7fca388831e5))

## [1.15.0](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.14.0...pubsub/v1.15.0) (2021-08-13)


### Features

* **pubsub:** Add topic retention options ([5996846](https://www.github.com/googleapis/google-cloud-go/commit/59968462a3870c6289166fa1161f9b6d9c10e093))


### Bug Fixes

* **pubsub:** always make config check to prevent race ([#4606](https://www.github.com/googleapis/google-cloud-go/issues/4606)) ([8cfcf53](https://www.github.com/googleapis/google-cloud-go/commit/8cfcf53d03b9b442e7f0bc1c1b20c791e31c07b0)), refs [#3626](https://www.github.com/googleapis/google-cloud-go/issues/3626)
* **pubsub:** mitigate race in checking ordering config ([#4602](https://www.github.com/googleapis/google-cloud-go/issues/4602)) ([112eea2](https://www.github.com/googleapis/google-cloud-go/commit/112eea20b46bbc34e5f8f65b9812fb3e60107409)), refs [#3626](https://www.github.com/googleapis/google-cloud-go/issues/3626)
* **pubsub:** replace IAMPolicy in API config ([5996846](https://www.github.com/googleapis/google-cloud-go/commit/59968462a3870c6289166fa1161f9b6d9c10e093))

## [1.14.0](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.13.0...pubsub/v1.14.0) (2021-08-09)


### Features

* **pubsub:** expose CallOptions for pub/sub retries and timeouts ([#4428](https://www.github.com/googleapis/google-cloud-go/issues/4428)) ([8b99dd3](https://www.github.com/googleapis/google-cloud-go/commit/8b99dd356475a750000c06a44fc7b8423d703967))

## [1.13.0](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.12.2...pubsub/v1.13.0) (2021-07-20)


### Features

* **pubsub/pstest:** add ability to create a pstest server listening on ([#4459](https://www.github.com/googleapis/google-cloud-go/issues/4459)) ([f1b7c8b](https://www.github.com/googleapis/google-cloud-go/commit/f1b7c8b33bc135c6cb8f21cdec586b25d81ea214))

### [1.12.2](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.12.1...pubsub/v1.12.2) (2021-07-08)


### Bug Fixes

* **pubsub:** retry all goaway errors ([#4384](https://www.github.com/googleapis/google-cloud-go/issues/4384)) ([1eae86f](https://www.github.com/googleapis/google-cloud-go/commit/1eae86f1882660d901b9fb0e8dab6f138a048dbb)), refs [#4257](https://www.github.com/googleapis/google-cloud-go/issues/4257)

### [1.12.1](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.12.0...pubsub/v1.12.1) (2021-07-01)


### Bug Fixes

* **pubsub:** retry GOAWAY errors ([#4313](https://www.github.com/googleapis/google-cloud-go/issues/4313)) ([7076fef](https://www.github.com/googleapis/google-cloud-go/commit/7076fef5fef81cce47dbfbab3d7257cc7d3776bc))

## [1.12.0](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.11.0...pubsub/v1.12.0) (2021-06-23)


### Features

* **pubsub/pstest:** add channel to support user-defined publish responses ([#4251](https://www.github.com/googleapis/google-cloud-go/issues/4251)) ([e1304f4](https://www.github.com/googleapis/google-cloud-go/commit/e1304f435fed4a767f4a652f32f1386979ff794f))


### Bug Fixes

* **pubsub:** fix memory leak issue in publish scheduler ([#4282](https://www.github.com/googleapis/google-cloud-go/issues/4282)) ([22ffc18](https://www.github.com/googleapis/google-cloud-go/commit/22ffc18e522c0f943db57f8c943e7356067bedfd))

## [1.11.0](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.10.3...pubsub/v1.11.0) (2021-05-27)


### Features

* **pubsub:** add flush method to topic ([#2863](https://www.github.com/googleapis/google-cloud-go/issues/2863)) ([825ddd6](https://www.github.com/googleapis/google-cloud-go/commit/825ddd692363eb2dd8cd253cc5976867e432f547))

### [1.10.3](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.10.2...pubsub/v1.10.3) (2021-04-23)


### Bug Fixes

* **pubsub:** fix failing message storage policy tests ([#4003](https://www.github.com/googleapis/google-cloud-go/issues/4003)) ([8946158](https://www.github.com/googleapis/google-cloud-go/commit/8946158561e1599c164021364e7fcb2a4c4d2f3d))
* **pubsub:** make config call permission error in Receive transparent ([#3985](https://www.github.com/googleapis/google-cloud-go/issues/3985)) ([a1614db](https://www.github.com/googleapis/google-cloud-go/commit/a1614db35a51d21c52bcba5e805071381d8f5133))

### [1.10.2](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.10.1...pubsub/v1.10.2) (2021-04-08)


### Bug Fixes

* **pubsub:** respect subscription message ordering field in scheduler ([#3886](https://www.github.com/googleapis/google-cloud-go/issues/3886)) ([1fcc78a](https://www.github.com/googleapis/google-cloud-go/commit/1fcc78ac6ecb461c3bbede9667436614c9df1535))
* **pubsub:** update quiescenceDur in failing e2e test ([#3780](https://www.github.com/googleapis/google-cloud-go/issues/3780)) ([97e6c69](https://www.github.com/googleapis/google-cloud-go/commit/97e6c696c39bf4cf49fa5ef51145cfcb2a1a5d71))

### [1.10.1](https://www.github.com/googleapis/google-cloud-go/compare/v1.10.0...v1.10.1) (2021-03-04)


### Bug Fixes

* **pubsub:** hide context.Cancelled error in sync pull ([#3752](https://www.github.com/googleapis/google-cloud-go/issues/3752)) ([f88bdc8](https://www.github.com/googleapis/google-cloud-go/commit/f88bdc85072e5ad511a907d98207ebf7d22e9df7))

## [1.10.0](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.9.1...v1.10.0) (2021-02-10)


### Features

* **pubsub:** add opencensus metrics for outstanding messages/bytes ([#3690](https://www.github.com/googleapis/google-cloud-go/issues/3690)) ([4039b82](https://www.github.com/googleapis/google-cloud-go/commit/4039b82e95b3a8ba2322d1f4fe9e2c21b087a907))

### [1.9.1](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.9.0...v1.9.1) (2020-12-10)


### Bug Fixes

* **pubsub:** fix default stream ack deadline seconds ([#3430](https://www.github.com/googleapis/google-cloud-go/issues/3430)) ([a10263a](https://www.github.com/googleapis/google-cloud-go/commit/a10263adc2ec9483ecedd0bf0b028863342ea760))
* **pubsub:** respect streamAckDeadlineSeconds with MaxExtensionPeriod ([#3367](https://www.github.com/googleapis/google-cloud-go/issues/3367)) ([45131b6](https://www.github.com/googleapis/google-cloud-go/commit/45131b6c526ded2964ffd067c4a5420d508f0b1a))

## [1.9.0](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.8.3...v1.9.0) (2020-12-03)

### Features

- **pubsub:** Enable server side flow control by default with the option to turn it off ([#3154](https://www.github.com/googleapis/google-cloud-go/issues/3154)) ([e392e61](https://www.github.com/googleapis/google-cloud-go/commit/e392e6157ee02a344528de63ab16baba61470b24))

### Refactor

**NOTE**: Several changes were proposed for allowing `Message` and `PublishResult` to be used outside the library. However, the decision was made to only allow packages in `google-cloud-go` to access `NewMessage` and `NewPublishResult` (see #3351).

- **pubsub:** Allow Message and PublishResult to be used outside the package ([#3200](https://www.github.com/googleapis/google-cloud-go/issues/3200)) ([581bf92](https://www.github.com/googleapis/google-cloud-go/commit/581bf92878dcb52ae8ea3633d4b3fcbb7054ff0f))
- **pubsub:** Remove NewMessage and NewPublishResult ([#3232](https://www.github.com/googleapis/google-cloud-go/issues/3232)) ([a781a3a](https://www.github.com/googleapis/google-cloud-go/commit/a781a3ad0c626fc0a7aff0ce33b1ef0830ee2259))

### [1.8.3](https://www.github.com/googleapis/google-cloud-go/compare/pubsub/v1.8.2...v1.8.3) (2020-11-10)

### Bug Fixes

- **pubsub:** retry deadline exceeded errors in Acknowledge ([#3157](https://www.github.com/googleapis/google-cloud-go/issues/3157)) ([ae75b46](https://www.github.com/googleapis/google-cloud-go/commit/ae75b46033d9f14f41c1bde4b9646c93f8e2bbad))

## v1.8.2

- Fixes:
  - fix(pubsub): track errors in published messages opencensus metric (#2970)
  - fix(pubsub): do not propagate context deadline exceeded error (#3055)

## v1.8.1

- Suppress connection is closing on error on subscriber close. (#2951)

## v1.8.0

- Add status code to error injection in pstest. This is a BREAKING CHANGE.

## v1.7.0

- Add reactor options to pstest server. (#2916)

## v1.6.2

- Make message.Modacks thread safe in pstest. (#2755)
- Fix issue with closing publisher and subscriber client errors. (#2867)
- Fix updating subscription filtering/retry policy in pstest. (#2901)

## v1.6.1

- Fix issue where EnableMessageOrdering wasn't being parsed properly to `SubscriptionConfig`.

## v1.6.0

- Fix issue where subscriber streams were limited because it was using a single grpc conn.
  - As a side effect, publisher and subscriber grpc conns are no longer shared.
- Add fake time function in pstest.
- Add support for server side flow control.

## v1.5.0

- Add support for subscription detachment.
- Add support for message filtering in subscriptions.
- Add support for RetryPolicy (server-side feature).
- Fix publish error path when ordering key is disabled.
- Fix panic on Topic.ResumePublish method.

## v1.4.0

- Add support for upcoming ordering keys feature.

## v1.3.1

- Fix bug with removing dead letter policy from a subscription
- Set default value of MaxExtensionPeriod to 0, which is functionally equivalent

## v1.3.0

- Update cloud.google.com/go to v0.54.0

## v1.2.0

- Add support for upcoming dead letter topics feature
- Expose Subscription.ReceiveSettings.MaxExtensionPeriod setting
- Standardize default settings with other client libraries
  - Increase publish delay threshold from 1ms to 10ms
  - Increase subscription MaxExtension from 10m to 60m
- Always send keepalive/heartbeat ping on StreamingPull streams to minimize
  stream reopen requests

## v1.1.0

- Limit default grpc connections to 4.
- Fix issues with OpenCensus metric for pull count not including synchronous pull messages.
- Fix issue with publish bundle size calculations.
- Add ClearMessages method to pstest server.

## v1.0.1

Small fix to a package name.

## v1.0.0

This is the first tag to carve out pubsub as its own module. See:
https://github.com/golang/go/wiki/Modules#is-it-possible-to-add-a-module-to-a-multi-module-repository.
