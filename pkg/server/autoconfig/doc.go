// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package autoconfig defines a mechanism through which the SQL service
// for a tenant (including, possibly, the system tenant) can run
// SQL tasks defined externally only once, using jobs.
//
// The main purposes of this logic are:
//
//   - To support SQL-based initialization of a cluster's parameters
//     (including cluster settings, initial user creation, initial
//     db creation) prior to accepting application clients.
//   - To support logical cluster maintainance by CockroachCloud
//     SREs for Serverless customers without incurring RU billing.
//   - To support the automation of maintainance operations across
//     a fleet of secondary tenants (such as the creation of
//     multi-region system tables when initializing a multi-region
//     CC serverless cluster).
//
// In a nutshell, the mechanism works as follows:
//
//   - There is a permanent job called the 'auto config runner'.
//   - The runner job accepts task definitions from "the environment"
//     (see below for details), and organizes their execution
//     by creating individual 'auto config task' jobs.
//   - Each auto config task job runs one task; it is cancellable
//     by the end user.
//   - The runner job only creates the job for the next task
//     after the job for the previous task has fully completed
//     (either successfully, with an error, or has been cancelled).
//
// # Task provenance
//
// The auto config runner job takes its input from a "task provider"
// (Go interface `Provider`, from the package `acprovider`). The task
// provider has a notification channel and an accessor that lists
// pending tasks.
//
// The provider also has a method through which the task runner can
// advise of task completion. This is an optimization and can be used
// by the provider to truncate the list of incoming tasks.
//
// In practice, there are 2 main task providers envisioned at the time
// of this writing:
//
//   - A provider of static configuration tasks for the system tenant,
//     via hardcoded "configuration profiles" selected when a cluster is
//     initially created.
//   - A provider of dynamic configuration asks for secondary tenants,
//     provisioned through a RPC in the "tenant connector".
//
// # Task definition and execution guarantees
//
// The runner logic provides at-least-once execution semantics, and
// tries hard to provide exactly-once semantics. (As the theory tells
// us that exactly-once is not possible, see below for a discussion of
// exceptions.) It also guarantees that auto config tasks are not
// executed concurrently with each other, with at most one node
// executing a task at a time.
//
// In a nutshell, the mechanism works as follows:
//
//   - Tasks are numbered sequentially (task ID); it is the
//     responsibility of "the environment" (the task provider) to
//     guarantee that later tasks have greater IDs than earlier tasks.
//   - The runner uses "started" and "completed" markers in the
//     system.job_info table to ensure that no more than one job is
//     created for a given task.
//
// More specifically, when the runner is ready to execute a task, it
// transactionally:
//
//  1. checks that there is no in-flight task already.
//     (e.g. one started by the runner on another node)
//  2. retrieves the latest completion marker by task ID from job_info,
//     (indicates the latest task completed by ID)
//  3. it ratchets the queue of incoming tasks forward
//     to skip over all tasks with an ID lower or equal to the
//     last task completed.
//  4. it creates a "auto config task" job entry for that task
//     and places a start marker for it in job_info.
//
// Then, the "auto config task" executes the SQL in the task
// transactionally with the removal of its start marker and the
// creation of its completion marker.
//
// # Possible exceptions to the "exactly-once" semantics
//
// The tasks are executed as jobs:
//
//   - if a task's job is cancelled by the user before it runs, its
//     side-effects will never be observed.
//   - if a node crashes while a task job is executing, it
//     will be restarted later (either after this node restarts,
//     or by being adopted by another node). So any
//     side effects not otherwise aborted by the transaction
//     supporting the task's execution may be observed more than once.
//     This includes e.g. changes to cluster settings.
package autoconfig
