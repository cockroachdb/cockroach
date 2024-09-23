// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { useMemo } from "react";
import useSWR from "swr";
import useSWRImmutable from "swr/immutable";

import { executeInternalSql } from "./sqlApi";

// This call is issued if we don't have the current db id in the cache.
const getDatabases = `SELECT id, name FROM crdb_internal.databases`;

type DatabaseNameRow = {
  id: number;
  name: string;
};

const fetchDbIdsToNames = async () =>
  executeInternalSql<DatabaseNameRow>({
    statements: [{ sql: getDatabases }],
    execute: true,
  });

export const useDbIdToName = (id: number) => {
  const { data, isLoading, error } = useSWRImmutable(
    `dbIdsToNames`,
    fetchDbIdsToNames,
  );

  const idsToName = useMemo(() => {
    const idsToName = new Map<number, string>();
    if (!data?.execution?.txn_results?.length) {
      return idsToName;
    }

    data.execution.txn_results[0].rows.forEach(row => {
      idsToName.set(row.id, row.name);
    });
    return idsToName;
  }, [data]);

  const name = idsToName.get(id);

  return {
    name,
    isLoading: isLoading && !name,
    error: !name ? error : null,
  };
};
