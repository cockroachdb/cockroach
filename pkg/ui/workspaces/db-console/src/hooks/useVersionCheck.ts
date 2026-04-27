// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import useSWRImmutable from "swr/immutable";

import { VersionList } from "src/interfaces/cockroachlabs";
import { versionCheck } from "src/util/cockroachlabsAPI";

/**
 * useVersionCheck fetches the list of newer CockroachDB versions available
 * from cockroachlabs.com. The check is performed once per session when both
 * clusterID and buildtag are available.
 */
export const useVersionCheck = (
  clusterID: string | undefined,
  buildtag: string | undefined,
) => {
  const key =
    clusterID && buildtag ? ["versionCheck", clusterID, buildtag] : null;

  const { data, error, isLoading } = useSWRImmutable<VersionList>(key, () =>
    versionCheck({ clusterID: clusterID!, buildtag: buildtag! }),
  );

  return { newerVersions: data ?? null, error, isLoading };
};
