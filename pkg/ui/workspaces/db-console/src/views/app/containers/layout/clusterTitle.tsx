// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as React from "react";

import "./clusterTitle.scss";

interface ClusterTitleProps {
  // Human-readable cluster name, when one is configured.
  clusterName?: string;
  // Fallback cluster UUID, shown when no name is available.
  clusterId?: string;
  // Optional leading marker (e.g. "(Obs Service) ") rendered before the title.
  prefix?: string;
}

// ClusterTitle renders the cluster identity in the navigation bar. REBRAND
// (2026): matches the design reference's page-header treatment — a light Geist
// label with the UUID fallback set in Geist Mono, rather than the previous
// faux-bold Heading2 (Geist-Medium synthesized to 600). When a cluster name is
// present it renders in place of the "Cluster id: <uuid>" fallback.
export function ClusterTitle({
  clusterName,
  clusterId,
  prefix,
}: ClusterTitleProps): React.ReactElement {
  return (
    <span className="cluster-title">
      {prefix}
      {clusterName ? (
        clusterName
      ) : (
        <>
          {"Cluster id: "}
          <span className="cluster-title__id">{clusterId || ""}</span>
        </>
      )}
    </span>
  );
}
