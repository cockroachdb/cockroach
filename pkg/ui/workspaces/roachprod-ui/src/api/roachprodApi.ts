// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  ClusterInfo,
  CreateClusterRequest,
  CreateClusterResponse,
  DeleteClusterRequest,
  ExtendClusterRequest,
  ListClustersResponse,
} from '../types/cluster';

const API_BASE = '/api';

async function fetchJSON<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: response.statusText }));
    throw new Error(error.error || `HTTP ${response.status}: ${response.statusText}`);
  }

  return response.json();
}

export async function listClusters(userFilter?: string): Promise<ClusterInfo[]> {
  const url = userFilter
    ? `${API_BASE}/clusters?user=${encodeURIComponent(userFilter)}`
    : `${API_BASE}/clusters`;
  const response = await fetchJSON<ListClustersResponse>(url);
  return response.clusters || [];
}

export async function createCluster(request: CreateClusterRequest): Promise<CreateClusterResponse> {
  return fetchJSON<CreateClusterResponse>(`${API_BASE}/clusters/create`, {
    method: 'POST',
    body: JSON.stringify(request),
  });
}

export async function deleteClusters(clusterNames: string[]): Promise<{ success: boolean; message: string }> {
  const request: DeleteClusterRequest = { clusters: clusterNames };
  return fetchJSON(`${API_BASE}/clusters/delete`, {
    method: 'DELETE',
    body: JSON.stringify(request),
  });
}

export async function extendCluster(
  clusterName: string,
  lifetime: string,
): Promise<{ success: boolean; message: string }> {
  const request: ExtendClusterRequest = { lifetime };
  return fetchJSON(`${API_BASE}/clusters/${clusterName}/extend`, {
    method: 'PUT',
    body: JSON.stringify(request),
  });
}

export async function refreshAWSSSO(): Promise<{ success: boolean; message: string; output?: string }> {
  return fetchJSON(`${API_BASE}/aws-sso-login`, {
    method: 'POST',
  });
}

export async function getSSOStatus(): Promise<{ tokenExpired: boolean }> {
  return fetchJSON(`${API_BASE}/aws-sso-status`, {
    method: 'GET',
  });
}

export async function stageCluster(clusterName: string): Promise<{ success: boolean; message: string }> {
  return fetchJSON(`${API_BASE}/clusters/${clusterName}/stage`, {
    method: 'POST',
  });
}

export async function stageLocalCluster(clusterName: string): Promise<{ success: boolean; message: string }> {
  return fetchJSON(`${API_BASE}/clusters/${clusterName}/stage-local`, {
    method: 'POST',
  });
}

export async function startCluster(clusterName: string): Promise<{ success: boolean; message: string }> {
  return fetchJSON(`${API_BASE}/clusters/${clusterName}/start`, {
    method: 'POST',
  });
}

export async function stopCluster(clusterName: string, node?: string): Promise<{ success: boolean; message: string }> {
  const body = node ? JSON.stringify({ node }) : undefined;
  return fetchJSON(`${API_BASE}/clusters/${clusterName}/stop`, {
    method: 'POST',
    body,
  });
}

export async function wipeCluster(clusterName: string): Promise<{ success: boolean; message: string }> {
  return fetchJSON(`${API_BASE}/clusters/${clusterName}/wipe`, {
    method: 'POST',
  });
}

export async function getClusterInfo(clusterName: string): Promise<{
  name: string;
  nodes: string[];
  pgUrls: string[];
  adminUrls: string[];
}> {
  return fetchJSON(`${API_BASE}/clusters/${clusterName}/info`, {
    method: 'GET',
  });
}

export async function getAdminURL(clusterName: string): Promise<{ url: string }> {
  return fetchJSON(`${API_BASE}/clusters/${clusterName}/adminurl`, {
    method: 'GET',
  });
}

export interface NodeStatus {
  nodeId: number;
  running: boolean;
  version: string;
  pid: string;
  err?: string;
}

export async function getClusterStatus(clusterName: string): Promise<{ statuses: NodeStatus[] }> {
  return fetchJSON(`${API_BASE}/clusters/${clusterName}/status`, {
    method: 'GET',
  });
}

export function getSSHWebSocketURL(clusterName: string, nodeNum: number): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const host = window.location.host;
  return `${protocol}//${host}/api/ssh/${encodeURIComponent(clusterName)}/${nodeNum}`;
}

export function getSQLWebSocketURL(clusterName: string, nodeNum: number): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const host = window.location.host;
  return `${protocol}//${host}/api/ssh/${encodeURIComponent(clusterName)}/${nodeNum}`;
}

export interface MachineType {
  name: string;
  description: string;
}

export interface CloudProviderOptions {
  zones: string[];
  machineTypes: MachineType[];
}

export async function getCloudProviderOptions(provider: string): Promise<CloudProviderOptions> {
  return fetchJSON(`${API_BASE}/cloud-providers/${encodeURIComponent(provider)}/options`, {
    method: 'GET',
  });
}

export async function getCurrentUser(): Promise<string> {
  const response = await fetchJSON<{ username: string }>(`${API_BASE}/current-user`);
  return response.username;
}

export async function stageWorkload(clusterName: string): Promise<{ success: boolean; message: string }> {
  return fetchJSON(`${API_BASE}/clusters/${encodeURIComponent(clusterName)}/stage-workload`, {
    method: 'POST',
  });
}

export async function startWorkload(clusterName: string): Promise<{ success: boolean; message: string }> {
  return fetchJSON(`${API_BASE}/clusters/${encodeURIComponent(clusterName)}/start-workload`, {
    method: 'POST',
  });
}

export async function stopWorkload(clusterName: string): Promise<{ success: boolean; message: string }> {
  return fetchJSON(`${API_BASE}/clusters/${encodeURIComponent(clusterName)}/stop-workload`, {
    method: 'POST',
  });
}
