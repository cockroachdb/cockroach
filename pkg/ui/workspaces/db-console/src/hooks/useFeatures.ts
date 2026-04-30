// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";

export interface FeatureInfo {
  name: string;
  title: string;
  description: string;
  route_path: string;
  enabled: boolean;
}

interface FeaturesResponse {
  features: FeatureInfo[];
}

const FEATURES_API_PATH = "api/v2/dbconsole/features";

async function fetchFeatures(): Promise<FeatureInfo[]> {
  const response = await fetch(FEATURES_API_PATH, {
    headers: {
      Accept: "application/json",
      "X-Cockroach-API-Session": "cookie",
    },
    credentials: "same-origin",
  });
  if (!response.ok) {
    throw new Error(
      `Failed to fetch features: ${response.status} ${response.statusText}`,
    );
  }
  const data: FeaturesResponse = await response.json();
  return data.features ?? [];
}

async function toggleFeature(
  name: string,
  action: "enable" | "disable",
): Promise<void> {
  const response = await fetch(
    `${FEATURES_API_PATH}/${name}/${action}`,
    {
      method: "POST",
      headers: { "X-Cockroach-API-Session": "cookie" },
      credentials: "same-origin",
    },
  );
  if (!response.ok) {
    throw new Error(
      `Failed to ${action} feature ${name}: ${response.status}`,
    );
  }
}

export function useFeatures() {
  const { data, error, isLoading, mutate } =
    util.useSwrWithClusterId<FeatureInfo[]>(
      "dbconsole-features",
      fetchFeatures,
      {
        revalidateOnFocus: false,
        revalidateOnReconnect: false,
      },
    );

  const enableFeature = async (name: string) => {
    await toggleFeature(name, "enable");
    await mutate();
  };

  const disableFeature = async (name: string) => {
    await toggleFeature(name, "disable");
    await mutate();
  };

  return {
    features: data ?? [],
    error,
    isLoading,
    enableFeature,
    disableFeature,
    refresh: mutate,
  };
}
