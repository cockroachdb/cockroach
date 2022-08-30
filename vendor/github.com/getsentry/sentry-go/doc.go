/*
Package sentry is the official Sentry SDK for Go.

Use it to report errors and track application performance through distributed
tracing.

For more information about Sentry and SDK features please have a look at the
documentation site https://docs.sentry.io/platforms/go/.

Basic Usage

The first step is to initialize the SDK, providing at a minimum the DSN of your
Sentry project. This step is accomplished through a call to sentry.Init.

	func main() {
		err := sentry.Init(...)
		...
	}

A more detailed yet simple example is available at
https://github.com/getsentry/sentry-go/blob/master/example/basic/main.go.

Error Reporting

The Capture* functions report messages and errors to Sentry.

	sentry.CaptureMessage(...)
	sentry.CaptureException(...)
	sentry.CaptureEvent(...)

Use similarly named functions in the Hub for concurrent programs like web
servers.

Performance Monitoring

You can use Sentry to monitor your application's performance. More information
on the product page https://docs.sentry.io/product/performance/.

The StartSpan function creates new spans.

	span := sentry.StartSpan(ctx, "operation")
	...
	span.Finish()

Integrations

The SDK has support for several Go frameworks, available as subpackages.

Getting Support

For paid Sentry.io accounts, head out to https://sentry.io/support.

For all users, support channels include:
	Forum: https://forum.sentry.io
	Discord: https://discord.gg/Ww9hbqr (#go channel)

If you found an issue with the SDK, please report through
https://github.com/getsentry/sentry-go/issues/new/choose.

For responsibly disclosing a security issue, please follow the steps in
https://sentry.io/security/#vulnerability-disclosure.
*/
package sentry
