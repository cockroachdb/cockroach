// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
This package is used to collect SQL Activity Statistics from the SQL subsystem.

# The recording of SQL Activity is managed through the following components:

 1. Conn Executor: SQL Execution is managed here and stats are collected and sent
    to the Stats Collector.
 2. (sslocal) Stats Collector: Receives stats from the Conn Executor and buffers them into
    the `ssmemstorage.Container`
 3. (sslocal) SQL Stats Ingester: Receives stats from the Stats Collector and
    flushes them to the registry.
 4. Container: A per-application container that holds stats for a given
    application. The Ingester flushes

# Sequence of Operations

The diagram below illustrates how stats flow through the StatsCollector and
StatsIngester as a transaction executes.

	+---------------+           +-----------------+                                   +---------------+                                        +-----------+
	| ConnExecutor  |           | StatsCollector  |                                   | StatsIngester |                                        | Container |
	+---------------+           +-----------------+                                   +---------------+                                        +-----------+
			|                            |                                                    |                                                      |
			| RecordStatement            |                                                    |                                                      |
			|--------------------------->|                                                    |                                                      |
			|                            | -------------------------------------------\       |                                                      |
			|                            |-| *RecordedStmtStats accumulates in buffer |       |                                                      |
			|                            | |------------------------------------------|       |                                                      |
			|                            |                                                    |                                                      |
			| EndTransaction             |                                                    |                                                      |
			|--------------------------->|                                                    |                                                      |
			|                            | ------------------------------------------\        |                                                      |
			|                            |-| set TransactionID on *RecordedStmtStats |        |                                                      |
			|                            | |-----------------------------------------|        |                                                      |
			|                            |                                                    |                                                      |
			|                            | IngestStatement                                    |                                                      |
			|                            |--------------------------------------------------->|                                                      |
			|                            |                                                    | -------------------------------------------\         |
			|                            |                                                    |-| *RecordedStmtStats accumulates in buffer |         |
			|                            |                                                    | |------------------------------------------|         |
			|                            |                                                    |                                                      |
			|                            | RecordStatement                                    |                                                      |
			|                            |---------------------------------------------------------------------------------------------------------->|
			|                            |                                                    |                                                      |
			| RecordTransaction          |                                                    |                                                      |
			|--------------------------->|                                                    |                                                      |
			|                            |                                                    |                                                      |
			|                            | IngestTransaction                                  |                                                      |
			|                            |--------------------------------------------------->|                                                      |
			|                            |                                                    |                                                      |
			|                            | RecordTransaction                                  |                                                      |
			|                            |---------------------------------------------------------------------------------------------------------->|
			|                            |                                                    | -----------------------------------------------\     |
			|                            |                                                    |-| all *RecordedStmtStats in buffer are flushed |     |
			|                            |                                                    | |----------------------------------------------|     |
			|                            |                                                    |                                                      | ----------------------------------------\
			|                            |                                                    |                                                      |-| eventually flushed to persisted stats |
			|                            |                                                    |                                                      | |---------------------------------------|
			|                            |                                                    |                                                      |
*/
package sslocal

// Input to sequence diagram generator:
/*
object ConnExecutor StatsCollector StatsIngester Container
ConnExecutor -> StatsCollector: RecordStatement
note right of StatsCollector: *RecordedStmtStats accumulates in buffer
ConnExecutor -> StatsCollector: EndTransaction
note right of StatsCollector: set TransactionID on *RecordedStmtStats
StatsCollector -> StatsIngester: IngestStatement
note right of StatsIngester: *RecordedStmtStats accumulates in buffer
StatsCollector -> Container: RecordStatement
ConnExecutor -> StatsCollector: RecordTransaction
StatsCollector -> StatsIngester: IngestTransaction
StatsCollector -> Container: RecordTransaction
note right of StatsIngester: all *RecordedStmtStats in buffer are flushed
note right of Container: eventually flushed to persisted stats
*/
