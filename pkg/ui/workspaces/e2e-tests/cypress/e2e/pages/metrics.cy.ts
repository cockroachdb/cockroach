// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";

describe("metrics page", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    // make sure we deterministic start on a fixed set of configurations
    cy.visit("#/metrics/overview/cluster?preset=past-hour");
  });

  it("displays cluster ID", () => {
    cy.findByText(
      /^Cluster id: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
      { timeout: 30_000 },
    );
  });

  it("displays metrics controls", () => {
    // Check for node selector
    cy.get('[class*="dropdown__title"]').contains("Graph:");
    cy.get(".Select-value-label").contains("Cluster");

    // Check for Dashboard selector
    cy.get('[class*="dropdown__title"]').contains("Dashboard:");
    cy.get(".Select-value-label").contains("Overview");

    // Check for time range selector
    cy.get('[class*="range__range-title"]').contains("1h");
    cy.get('[class*="Select-value-label"]').contains("Past Hour");

    // Check for time navigation controls
    cy.get('button[aria-label="previous time interval"]').should("exist");
    cy.get('button[aria-label="next time interval"]').should("exist");
    cy.get("button").contains("Now").should("exist");
  });

  it("displays overview graphs", () => {
    cy.get('[class*="visualization-header"]').contains(
      "SQL Queries Per Second",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Service Latency: SQL Statements, 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "SQL Statement Contention",
    );
    cy.get('[class*="visualization-header"]').contains("Replicas per Node");
    cy.get('[class*="visualization-header"]').contains("Capacity");
  });

  it("can switch graph view to specific node", () => {
    cy.get('[class*="dropdown__title"]').contains("Graph:").click();
    cy.get(".Select-menu-outer .Select-option").contains("n1").click();
    cy.get(".Select-value-label").contains("n1").should("exist");
    cy.get(".Select-value-label").contains("Cluster").should("not.exist");
  });

  it("can switch dashboard to Hardware", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option").contains("Hardware").click();
    cy.get(".Select-value-label").contains("Hardware").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("CPU Percent");
    cy.get('[class*="visualization-header"]').contains("Host CPU Percent");
    cy.get('[class*="visualization-header"]').contains("Memory Usage");
    cy.get('[class*="visualization-header"]').contains("Disk Read Bytes/s");
    cy.get('[class*="visualization-header"]').contains("Disk Write Bytes/s");
    cy.get('[class*="visualization-header"]').contains("Disk Read IOPS");
    cy.get('[class*="visualization-header"]').contains("Disk Write IOPS");
    cy.get('[class*="visualization-header"]').contains("Disk Ops In Progress");
    cy.get('[class*="visualization-header"]').contains(
      "Available Disk Capacity",
    );
  });

  it("can switch dashboard to Runtime", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option").contains("Runtime").click();
    cy.get(".Select-value-label").contains("Runtime").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("Live Node Count");
    cy.get('[class*="visualization-header"]').contains("Memory Usage");
    cy.get('[class*="visualization-header"]').contains("Goroutine Count");
    cy.get('[class*="visualization-header"]').contains(
      "Goroutine Scheduling Latency: 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Runnable Goroutines per CPU",
    );
    cy.get('[class*="visualization-header"]').contains("GC Runs");
    cy.get('[class*="visualization-header"]').contains("GC Pause Time");
    cy.get('[class*="visualization-header"]').contains("GC Stopping Time");
    cy.get('[class*="visualization-header"]').contains("GC Assist Time");
    cy.get('[class*="visualization-header"]').contains("Non-GC Pause Time");
    cy.get('[class*="visualization-header"]').contains("Non-GC Stopping Time");
    cy.get('[class*="visualization-header"]').contains("CPU Time");
    cy.get('[class*="visualization-header"]').contains("Clock Offset");
  });

  it("can switch dashboard to Networking", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option").contains("Networking").click();
    cy.get(".Select-value-label").contains("Networking").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("Network Bytes Sent");
    cy.get('[class*="visualization-header"]').contains(
      "Network Bytes Received",
    );
    cy.get('[class*="visualization-header"]').contains(
      "RPC Heartbeat Latency: 50th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "RPC Heartbeat Latency: 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Unhealthy RPC Connections",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Network Packet Errors and Drops",
    );
    cy.get('[class*="visualization-header"]').contains("TCP Retransmits");
    cy.get('[class*="visualization-header"]').contains("Proxy requests");
    cy.get('[class*="visualization-header"]').contains("Proxy request errors");
    cy.get('[class*="visualization-header"]').contains("Proxy forwards");
    cy.get('[class*="visualization-header"]').contains("Proxy forward errors");
  });

  it("can switch dashboard to SQL", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option").contains("SQL").click();
    cy.get(".Select-value-label").contains("SQL").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("Open SQL Sessions");
    cy.get('[class*="visualization-header"]').contains("SQL Connection Rate");
    cy.get('[class*="visualization-header"]').contains(
      "Upgrades of SQL Transaction Isolation Level",
    );
    cy.get('[class*="visualization-header"]').contains("Open SQL Transactions");
    cy.get('[class*="visualization-header"]').contains("Active SQL Statements");
    cy.get('[class*="visualization-header"]').contains("SQL Byte Traffic");
    cy.get('[class*="visualization-header"]').contains(
      "SQL Queries Per Second",
    );
    cy.get('[class*="visualization-header"]').contains(
      "SQL Queries Within Routines Per Second",
    );
    cy.get('[class*="visualization-header"]').contains("SQL Statement Errors");
    cy.get('[class*="visualization-header"]').contains(
      "SQL Statement Contention",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Full Table/Index Scans",
    );
    cy.get('[class*="visualization-header"]').contains("Transaction Deadlocks");
    cy.get('[class*="visualization-header"]').contains(
      "Active Flows for Distributed SQL Statements",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Failed SQL Connections",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Connection Latency: 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Connection Latency: 90th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Service Latency: SQL Statements, 99.99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Service Latency: SQL Statements, 99.9th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Service Latency: SQL Statements, 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Service Latency: SQL Statements, 90th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "KV Execution Latency: 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "KV Execution Latency: 90th percentile",
    );
    cy.get('[class*="visualization-header"]').contains("Transactions");
    cy.get('[class*="visualization-header"]').contains("Transaction Restarts");
    cy.get('[class*="visualization-header"]').contains(
      "Transaction Latency: 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Transaction Latency: 90th percentile",
    );
    cy.get('[class*="visualization-header"]').contains("SQL Memory");
    cy.get('[class*="visualization-header"]').contains("Schema Changes");
    cy.get('[class*="visualization-header"]').contains(
      "Table Statistics Collections",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Statement Denials: Cluster Settings",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Distributed Query Error Reruns",
    );
  });

  it("can switch dashboard to Storage", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option").contains("Storage").click();
    cy.get(".Select-value-label").contains("Storage").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("Capacity");
    cy.get('[class*="visualization-header"]').contains("Live Bytes");
    cy.get('[class*="visualization-header"]').contains("WAL Fsync Latency");
    cy.get('[class*="visualization-header"]').contains("Log Commit Latency");
    cy.get('[class*="visualization-header"]').contains(
      "Command Commit Latency",
    );
    cy.get('[class*="visualization-header"]').contains("Compaction Duration");
    cy.get('[class*="visualization-header"]').contains(
      "Level Compaction Scores",
    );
    cy.get('[class*="visualization-header"]').contains("Read Amplification");
    cy.get('[class*="visualization-header"]').contains("File Counts");
    cy.get('[class*="visualization-header"]').contains("L0 SSTable Count");
    cy.get('[class*="visualization-header"]').contains("L0 SSTable Size");
    cy.get('[class*="visualization-header"]').contains("Flushes");
    cy.get('[class*="visualization-header"]').contains("WAL Bytes Written");
    cy.get('[class*="visualization-header"]').contains("Compactions");
    cy.get('[class*="visualization-header"]').contains("Ingestions");
    cy.get('[class*="visualization-header"]').contains("Write Stalls");
    cy.get('[class*="visualization-header"]').contains("Disk Write Breakdown");
    cy.get('[class*="visualization-header"]').contains(
      "Store Disk Write Bytes/s",
    );
    cy.get('[class*="visualization-header"]').contains("Iterator Block Bytes");
    cy.get('[class*="visualization-header"]').contains(
      "Store Disk Read Bytes/s",
    );
    cy.get('[class*="visualization-header"]').contains("Value Separated Bytes");
    cy.get('[class*="visualization-header"]').contains(
      "Unreclaimed Disk Space",
    );
    cy.get('[class*="visualization-header"]').contains("Blob File Sizes");
    cy.get('[class*="visualization-header"]').contains("File Descriptors");
    cy.get('[class*="visualization-header"]').contains("Time Series Writes");
    cy.get('[class*="visualization-header"]').contains(
      "Time Series Bytes Written",
    );
  });

  it("can switch dashboard to Replication", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option").contains("Replication").click();
    cy.get(".Select-value-label").contains("Replication").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("Ranges");
    cy.get('[class*="visualization-header"]').contains("Replicas per Node");
    cy.get('[class*="visualization-header"]').contains("Leaseholders per Node");
    cy.get('[class*="visualization-header"]').contains("Lease Types");
    cy.get('[class*="visualization-header"]').contains("Lease Preferences");
    cy.get('[class*="visualization-header"]').contains(
      "Average Replica Queries per Node",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Average Replica CPU per Node",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Logical Bytes per Node",
    );
    cy.get('[class*="visualization-header"]').contains("Replica Quiescence");
    cy.get('[class*="visualization-header"]').contains("Range Operations");
    cy.get('[class*="visualization-header"]').contains("Snapshots");
    cy.get('[class*="visualization-header"]').contains(
      "Snapshot Data Received",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Receiver Snapshots Queued",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Circuit Breaker Tripped Replicas",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Replicate Queue Actions: Successes",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Replicate Queue Actions: Failures",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Decommissioning Errors",
    );
  });

  it("can switch dashboard to Distributed", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option").contains("Distributed").click();
    cy.get(".Select-value-label").contains("Distributed").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("Batches");
    cy.get('[class*="visualization-header"]').contains("RPCs");
    cy.get('[class*="visualization-header"]').contains("RPC Errors");
    cy.get('[class*="visualization-header"]').contains("KV Transactions");
    cy.get('[class*="visualization-header"]').contains(
      "KV Transaction Durations: 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "KV Transaction Durations: 90th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Node Heartbeat Latency: 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Node Heartbeat Latency: 90th percentile",
    );
  });

  it("can switch dashboard to Queues", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option").contains("Queues").click();
    cy.get(".Select-value-label").contains("Queues").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains(
      "Queue Processing Failures",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Queue Processing Times",
    );
    cy.get('[class*="visualization-header"]').contains("Replica GC Queue");
    cy.get('[class*="visualization-header"]').contains("Replication Queue");
    cy.get('[class*="visualization-header"]').contains("Lease Queue");
    cy.get('[class*="visualization-header"]').contains("Split Queue");
    cy.get('[class*="visualization-header"]').contains("Merge Queue");
    cy.get('[class*="visualization-header"]').contains("Raft Log Queue");
    cy.get('[class*="visualization-header"]').contains("Raft Snapshot Queue");
    cy.get('[class*="visualization-header"]').contains(
      "Consistency Checker Queue",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Time Series Maintenance Queue",
    );
    cy.get('[class*="visualization-header"]').contains("MVCC GC Queue");
    cy.get('[class*="visualization-header"]').contains(
      "Protected Timestamp Records",
    );
  });

  it("can switch dashboard to Slow Requests", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option")
      .contains("Slow Requests")
      .click();
    cy.get(".Select-value-label").contains("Slow Requests").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("Slow Raft Proposals");
    cy.get('[class*="visualization-header"]').contains("Slow DistSender RPCs");
    cy.get('[class*="visualization-header"]').contains(
      "Slow Lease Acquisitions",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Slow Latch Acquisitions",
    );
  });

  it("can switch dashboard to Changefeeds", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option").contains("Changefeeds").click();
    cy.get(".Select-value-label").contains("Changefeeds").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("Changefeed Status");
    cy.get('[class*="visualization-header"]').contains("Commit Latency");
    cy.get('[class*="visualization-header"]').contains("Emitted Bytes");
    cy.get('[class*="visualization-header"]').contains("Sink Counts");
    cy.get('[class*="visualization-header"]').contains("Max Checkpoint Lag");
    cy.get('[class*="visualization-header"]').contains("Changefeed Restarts");
    cy.get('[class*="visualization-header"]').contains(
      "Oldest Protected Timestamp",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Backfill Pending Ranges",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Schema Registry Registrations",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Ranges in catchup mode",
    );
    cy.get('[class*="visualization-header"]').contains(
      "RangeFeed catchup scans duration",
    );
  });

  it("can switch dashboard to Overload", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option").contains("Overload").click();
    cy.get(".Select-value-label").contains("Overload").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("CPU Utilization");
    cy.get('[class*="visualization-header"]').contains(
      "KV Admission CPU Slots Exhausted Duration Per Second",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Admission Foreground IO Tokens Exhausted Duration Per Second",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Admission Background IO Tokens Exhausted Duration Per Second",
    );
    cy.get('[class*="visualization-header"]').contains("IO Overload");
    cy.get('[class*="visualization-header"]').contains(
      "Elastic CPU Tokens Exhausted Duration Per Second",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Admission Queueing Delay p99 – Foreground (Regular) CPU",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Admission Queueing Delay p99 – Foreground (Regular) Store",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Admission Queueing Delay p99 – Background (Elastic) Store",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Admission Queueing Delay p99 – Background (Elastic) CPU",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Admission Queueing Delay p99 – Replication Admission Control",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Blocked Replication Streams",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Replication Stream Send Queue Size",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Elastic CPU Utilization",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Goroutine Scheduling Latency: 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Goroutine Scheduling Latency: 99.9th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Runnable Goroutines per CPU",
    );
    cy.get('[class*="visualization-header"]').contains("LSM L0 Sublevels");
  });

  it("can switch dashboard to TTL", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option").contains("TTL").click();
    cy.get(".Select-value-label").contains("TTL").should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("Processing Rate");
    cy.get('[class*="visualization-header"]').contains("Estimated Rows");
    cy.get('[class*="visualization-header"]').contains("Job Latency");
    cy.get('[class*="visualization-header"]').contains("Spans in Progress");
  });

  it("can switch dashboard to Physical Cluster Replication", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option")
      .contains("Physical Cluster Replication")
      .click();
    cy.get(".Select-value-label")
      .contains("Physical Cluster Replication")
      .should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("Replication Lag");
    cy.get('[class*="visualization-header"]').contains("Logical Bytes");
  });

  it("can switch dashboard to Logical Data Replication", () => {
    cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
    cy.get(".Select-menu-outer .Select-option")
      .contains("Logical Data Replication")
      .click();
    cy.get(".Select-value-label")
      .contains("Logical Data Replication")
      .should("exist");
    cy.get(".Select-value-label").contains("Overview").should("not.exist");

    // verify graphs
    cy.get('[class*="visualization-header"]').contains("Replication Latency");
    cy.get('[class*="visualization-header"]').contains("Replication Lag");
    cy.get('[class*="visualization-header"]').contains("Row Updates Applied");
    cy.get('[class*="visualization-header"]').contains(
      "Logical Bytes Received",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Row Application Processing Time: 50th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Row Application Processing Time: 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains("DLQ Causes");
    cy.get('[class*="visualization-header"]').contains("Retry Queue Size");
  });

  it("can switch time range view", () => {
    cy.get('[class*="range__range-title"]').contains("1h").click();
    cy.get('[class*="range-selector"]').find("button").contains("30m").click();
    cy.get('[class*="Select-value-label"]').contains("Past 30 Minutes");
  });

  it("displays summary side bar", () => {
    cy.get(".summary-section").within(() => {
      cy.get(".summary-label").contains("Summary");

      // Total Nodes
      cy.contains(".summary-stat__title", "Total Nodes")
        .closest(".summary-stat")
        .within(() => {
          cy.get(".summary-stat__value").should("have.text", "1");
          cy.get("a")
            .contains("View nodes list")
            .should("have.attr", "href", "#/overview/list");
        });

      // Capacity Usage
      cy.contains(".summary-stat__title", "Capacity Usage")
        .closest(".summary-stat")
        .within(() => {
          cy.get(".summary-stat__value")
            .invoke("text")
            .should("match", /\d+\.\d+%/);
        });

      // Unavailable ranges
      cy.contains(".summary-stat__title", "Unavailable ranges")
        .closest(".summary-stat")
        .within(() => {
          cy.get(".summary-stat__value")
            .invoke("text")
            .should("match", /^\d+$/);
        });

      // Queries per second
      cy.contains(".summary-stat__title", "Queries per second")
        .closest(".summary-stat")
        .within(() => {
          cy.get(".summary-stat__value")
            .invoke("text")
            .should("match", /^\d+\.\d+$/);
        });

      // P99 latency
      cy.contains(".summary-stat__title", "P99 latency")
        .closest(".summary-stat")
        .within(() => {
          cy.get(".summary-stat__value")
            .invoke("text")
            .should("match", /^\d+\.\d+ ms$/);
        });
    });
  });

  it("displays navigation sidebar", () => {
    cy.findByRole("navigation").within(() => {
      cy.findByRole("link", { name: "Overview" });
      cy.findByRole("link", { name: "Metrics" });
      cy.findByRole("link", { name: "Databases" });
      cy.findByRole("link", { name: "SQL Activity" });
      cy.findByRole("link", { name: "Insights" });
      cy.findByRole("link", { name: "Top Ranges" });
      cy.findByRole("link", { name: "Jobs" });
      cy.findByRole("link", { name: "Schedules" });
      cy.findByRole("link", { name: "Advanced Debug" });
    });
  });
});
