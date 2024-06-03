# Async Buffered Logging in CRDB
Last Update: June 2024

Original author: abarganier

"Buffered logging" in CRDB could mean one of two things as of today. It could refer to the buffering 
 capabilities of the `fileSink`, or it could refer to the asynchronous buffering capabilities of the
`bufferedSink`. This document provides an overview of the `bufferedSink`, which controls asynchronous
buffered logging within CRDB. It aims to explain the core concepts behind the `bufferedSink` implementation,
and provide context of recent events that influenced its development.

## Overview & History

### What's a "log sink"?
When we refer to a "log sink" within CRDB, we are generally referring to any implementation of the
`logSink` interface in `pkg/util/log`. Most of these implementations, such as the `fileSink`, `fluentSink`,
`httpSink`, and `stderrSink` have a  user-facing counterpart in [the CRDB log config](
https://www.cockroachlabs.com/docs/stable/configure-logs). When a customer creates a log configuration,
these `logSink` implementations are initialized at startup & make the config a reality. When a log message
is generated in CRDB, it is routed to the proper `logSink` based on the log configuration provided by the
customer.


### History of buffering
Originally, "buffering" was only available to the `fileSink` type log sink, which is controlled by the
`buffered-writes` [config option](https://www.cockroachlabs.com/docs/stable/configure-logs#output-to-files)
(specific to the `fileSink` only). When enabled (which it is, by default), writes to the `logSink` are added
to a buffer, until the buffer is full. When the buffer is full, the next write triggers a "flush" to the 
underlying disk. Note however that this "flush" is not asynchronous. The logger/caller that triggers the 
flush of the `fileSink` will do so synchronously. If the disk is unavailable, the caller will hang. 
Generally, this has been acceptable since an unavailable disk usually means CRDB has much bigger problems
than just logging. However, recent desire to make `fileSink` logging more tolerant of disk 
unavailability has prompted us to enable use of async buffering with the `fileSink` via the `bufferedSink`
(more on `bufferedSink` later).

While this initial buffering capability existed for the `fileSink`, the logging sinks that involved the
network (`httpSink` and `fluentSink`) had no such capability. This quickly became a problem [[#67945](https://github.com/cockroachdb/cockroach/issues/67945)],
especially for serverless deployments where SQL pods rely heavily on network logging due to their ephemeral nature
(commonly, the SQL pod log files are discarded when the pod is deactivated). If the network target became
unavailable, `fluentSink` and `httpSink` would cause the caller to hang, which eventually could make the
process effectively unavailable as all server goroutines get blocked trying to make a logging call.

This prompted the creation of the `bufferedSink`, which is a `logSink` implementation that "wraps" an
underlying `logSink` and provides *asynchronous* buffering. This `bufferedSink` is perhaps one of the
more complex pieces of code within the logging subsystem, and has been a source of bugs in the past [e.g. [#104413](https://github.com/cockroachdb/cockroach/issues/104413)],
which is what prompted this tech note.

## Core concepts

The `bufferedSink` is a `logSink` implementation that "wraps" another underlying "child" `logSink` implementation.
The `bufferedSink` accepts & buffers writes intended for the child sink, and a separate goroutine is responsible for 
"flushing" the buffered writes once triggered. This is a fairly classic example of the asynchronous producer & consumer 
pattern,  which decouples the process of writing records from the process of consuming records. It shields the writer(s) 
from any problems that may occur on the consumption side of things, which is the exact goal of the `bufferedSink` - if a
`logSink` becomes unavailable for whatever reason, the log *writer* goroutine should not be blocked or hang. The 
`bufferedSink` should provide fast log write performance to writers, no matter what.

### Flush goroutine & signaling

As noted previously, the `bufferedSink` has two goroutines in play - the "writer", and the "flusher". Note that the 
writer is simply whichever goroutine making the logging call, which eventually gets routed to the `bufferedSink`. 
The `bufferedSink` does not explicitly create a writer goroutine. However, a flusher goroutine is explicitly created by
the `bufferedSink` once started.

The writer and flusher communicate with each other using a buffered channel, which is used to signal the flusher to 
perform the flush operation. The flusher receives the signal, atomically "dumps" the current buffer of logs to its local
memory, resets the buffer used by the writer, and then pushes the logs to the child `logSink`. 

If log volume is abnormally high, or the child `logSink` becomes unavailable, it's possible that the flusher goroutine
is not listening on the channel when signaled by the writer. Even in this instance, we want to protect the writer from
blocking when signaling the channel. To achieve this, we take a combination of approaches. The first is that we buffer
the channel with a size of 1. This allows writers to queue up one additional flush if one is currently ongoing. The
second is that when signaling the channel from the writer, we provide a `default` case to the `select` statement. If the
flusher goroutine is working on a flush, and the buffered channel already has another one scheduled, the writer will
simply take the `default` case and exit without scheduling an additional flush. In this case, another flush is already
scheduled, so the log message added by the writer will be picked up in the following flush. It's rare that this state
is reached, but it's important for us to protect writers from becoming blocked in this scenario. The general expected 
scenario where this *would* occur is when the underlying child sink becomes available and the flusher goroutine is stuck
waiting. 

### Buffering

The `bufferedSink` uses a bytes buffer that's size limited, which is configured in the logging configuration by the
`buffering.max-buffer-size`. However, under normal operation, the `bufferedSink` is likely to flush and empty the buffer
long before this max size is reached. This is because the `bufferedSink` is configured to flush before the buffer is
truly fully via the `buffering.flush-trigger-size` setting. The `bufferedSink` enforces that 
`max-buffer-size` >= `flush-trigger-size`. 

If the buffer ever becomes full, the `bufferedSink` will start dropping the oldest events to make way for the new. The
count of messages dropped can be observed via the `log.buffered.messages.dropped` metric.

### Output Formatting

When buffered messages are flushed from the underlying `bufferedSink`, they are concatenated together by a delimiter.
By default, a newline is used, but the `bufferedSink` can also be configured to concatenate the individual payloads
together as a JSON array. These are configured by the `buffering.format` log config option, but is not explicitly 
advertised in CRDB documentation as this was implemented for a special customer use case.

### The `tryForceSync` and `extraFlush` options

As part of the `logSink` interface, each implementation is expected to support a set of `sinkOutputOptions`. The 
`bufferedSink` supports the `tryForceSink` and `extraFlush` options.

`tryForceSync` attempts to force a synchronous flush. That is, it will block until the flush has been handled, so
long as the underlying sink can support the operation at that moment. This isn't an ironclad guarantee, but in the
vast majority of scenarios, this option is expected to be honored.

In the `bufferedSink`, this `tryForceSync` option is achieved by triggering a flush immediately, and providing a 
channel for the flusher goroutine to signal on once the flush has completed. In this case, the writer goroutine 
signals an immediate flush, and waits for the flusher goroutine to signal on a channel once the flush has completed.

This is a best effort attempt to honor `tryForceSync`. If one is already in progress, we simply buffer the message and
return, since this indicates that a flush has already been scheduled and the message will be flushed imminently anyway.
If you want to read an overly in-depth analysis of this option that was written back when we were fixing a bug with
this option, [see this document here](https://docs.google.com/document/d/11ey9EMCU5I73aVvJY2Vo2jbjFHwr_tVoynaKiY-sjfk/edit#heading=h.kw8y3kc4nv3r).

The other option supported by the `bufferedSink` is the `extraFlush` option. This simply tells the `bufferedSink` to 
flush immediately, but does not cause the writer to wait for the flush to complete. 

### Draining on Server Shutdown

Logging is the very last thing that's shutdown during the node shutdown sequence within CRDB. We want logging facilities
available to the process until the very last moment, to ensure that various systems still have logging capabilities 
during their own shutdown processes. For this reason, how we handle server shutdowns in the logging package needs a bit
extra care. 

The `bufferedSinkCloser` is a mechanism designed to signal all initialized `bufferedSink` instances to drain their 
buffers by performing a final flush, and exit the flusher goroutine. It is triggered within the logging shutdown
function, which is the last operation performed prior to the process exiting (see the `Main()` function in `pkg/cli`).
Normally, we'd use a `stop.Stopper` for something like this, but even `Stopper`s are stopped before logging facilities
are stopped before logging is stopped, so we're unable to use them for logging.

When a `bufferedSink` is created, it's registered with a process-global `bufferedSinkCloser`. When it's finally time to
shutdown logging, the `bufferedSinkCloser` will close a "stop channel" that's shared by all `bufferedSink` instances.
The `bufferedSinkCloser` then waits with a timeout on a `sync.WaitGroup` for all the `bufferedSink` instances to finish
their flush, and then returns. If a flusher goroutine is stuck due to an unavailable child sink, then the timeout will
eventually trigger (default 90 seconds) and a warning will be logged to stderr.
