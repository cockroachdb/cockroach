// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

export interface ClusterInfo {
  name: string;
  nodes: number;
  cloud: string;
  lifetimeRemaining: string;
  created: string;
  owner: string;
  machineType?: string;
  region?: string;
}

export interface CreateClusterRequest {
  name: string;
  nodes: number;
  cloud: string;
  machineType?: string;
  lifetime: string;
  localSSD: boolean;
  // Advanced options
  arch?: string;
  geo?: boolean;
  zones?: string[];
  filesystem?: string;
  labels?: Record<string, string>;
}

export interface CreateClusterResponse {
  success: boolean;
  message: string;
  cluster?: string;
}

export interface ExtendClusterRequest {
  lifetime: string;
}

export interface DeleteClusterRequest {
  clusters: string[];
}

export interface ErrorResponse {
  error: string;
  details?: string;
}

export interface ListClustersResponse {
  clusters: ClusterInfo[];
  total: number;
}
