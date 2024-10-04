// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
//   - The runner job accepts task definitions from zero or more
//     "external environments" (see below for details), and organizes
//     their execution by creating individual 'auto config task' jobs.
//   - Each auto config task job runs one task; it is cancellable
//     by the end user (see below for a note about cancellability).
//   - The runner job only creates the job for the next task after the
//     job for the previous task by a given environment has fully
//     completed (either successfully, with an error, or has been
//     cancelled).
//
// # Task provenance - data model
//
// We plan to use tasks for various purposes (troubleshooting,
// migrations, etc.) and so we need sequential semantics: a guarantee
// that a task doesn't start before the previous one has completed.
//
// However, if we did this for all tasks we would have a problem
// when a task gets stuck - then the entire system wouldn't make
// progress any more.
//
// To reconcile our desire for sequences and preserve our ability
// to "repair" broken situations, we also introduce a notion of
// *environment* where tasks are defined:
//
//   - an environment identifies a potential source of tasks.
//   - tasks are grouped by environment; or rather, each environment
//     provisions its own tasks (potentially at different rates).
//   - the execution of tasks within one environment is sequential.
//   - the execution of tasks of separate environments is independent,
//     and so different environments can execute tasks concurrently
//     with each other.
//   - the system supports the dynamic addition of new environments
//     (by using a yet-unused environment name for new tasks).
//
// This makes it possible to "abandon" an environment whose tasks
// got stuck and start tasks using a new one.
//
// # Task provenance - mechanism
//
// The auto config runner job takes its input from a "task provider"
// (Go interface `Provider`, from the package `acprovider`). The task
// provider has a notification channel and an accessor that lists
// environments with pending tasks ("active").
//
// For each active environment, the top-level runner creates a runner
// job specific to that environment. Each environment-specific runner
// then retrieves tasks from the provider to run them.
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
// Under normal conditions, and barring explicit contrarian action by
// the user, the runner logic provides at-least-once execution
// semantics, and tries hard to provide exactly-once semantics. (As
// the theory tells us that exactly-once is not possible, see below
// for a discussion of exceptions.) It also guarantees that auto
// config tasks from the same environment are not executed
// concurrently with each other, with at most one node executing a
// task at a time.
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
//
// # Task errors vs progress across tasks
//
// Note that if a task fail, its job is marked as permanently failed
// and it will not be retried. This also goes for tasks that have been
// canceled by the user.
//
// Of note, a task's failure does not prevent the next task from
// starting.
//
// This begs the question: what if the environment wishes to mark
// a task as "required", such that no progress is allowed in subsequent
// tasks until the required task completes successfully?
//
// While this is not yet supported in this package, a solution
// would probably contain the following:
//
//   - the task's job would be marked as non-cancellable by the
//     end-user.
//   - the task's job would not be marked as permanently failed
//     upon encountering execution failures, so it gets retried.
//   - we would need another mechanism to cancel execution
//     of that task and its environment in case the operator
//     wants to abandon the whole sequence.
//
// This enhancement is left to a later iteration.
package autoconfig
