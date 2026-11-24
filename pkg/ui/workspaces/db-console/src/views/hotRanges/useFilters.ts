// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { NodeID } from "@cockroachlabs/cluster-ui";
import isEmpty from "lodash/isEmpty";
import { useState } from "react";
import { useHistory, useLocation } from "react-router-dom";

import { cockroach } from "src/js/protos";

import type { History } from "history";

type HotRange = cockroach.server.serverpb.HotRangesResponseV2.IHotRange;

export interface Filters {
  nodeIds?: NodeID[];
  store_id?: string;
  databases?: string[];
  table?: string;
  index?: string;
}

// useFilters is a custom hook for managing the filter state of the
// hot ranges page. It is separate from the state management
// for the filters component. It both manages the state for the filters,
// as well as loads, persists it to the URL so that deep linking
// is possible.
export default function useFilters() {
  // On initial mount, we attempt to load the filter state
  // from the query parameters.
  const { search } = useLocation(); // Add this
  const getInitialFilters = () => {
    return loadSearchParams(new URLSearchParams(search)) || {};
  };
  const [filters, setFilters] = useState<Filters>(getInitialFilters());
  const history = useHistory();

  function applyFilters(newFilters: Filters) {
    setSearchParams(history, newFilters);
    setFilters(newFilters);
  }

  return { filters, applyFilters };
}

// loadSearchParams is a helper function that loads the filter state
// from the URL query parameters. It is used to initialize the filter state
// on initial mount.
function loadSearchParams(query: URLSearchParams): Filters {
  const loadArray = (key: string) => {
    const raw = query.get(key);
    if (raw) {
      return raw.split(",");
    }
    return [];
  };

  return {
    nodeIds: loadArray("node_ids").map(n => parseInt(n, 10) as NodeID),
    databases: loadArray("databases"),
    store_id: query.get("store_id"),
    table: query.get("table"),
    index: query.get("index"),
  };
}

// setSearchParams is a helper function that sets the filter state
// in the URL query parameters. It is used to persist the filter state
// to the URL anytime the user applies filters.
function setSearchParams(history: History, filters: Filters) {
  const params = new URLSearchParams();
  filters.nodeIds?.length && params.set("node_ids", filters.nodeIds?.join(","));
  filters.databases?.length &&
    params.set("databases", filters.databases?.join(","));
  filters.store_id && params.set("store_id", filters.store_id);
  filters.table && params.set("table", filters.table);
  filters.index && params.set("index", filters.index);
  history.location.search = params.toString();
  history.replace(history.location);
}

// filterRanges is a helper function that filters the hot ranges
// based on the filter state. It is used to filter the hot ranges
// when the user applies filters.
export function filterRanges(ranges: HotRange[], filters: Filters): any[] {
  const { store_id, databases, table, index } = filters;
  let filtered = ranges;

  if (!isEmpty(store_id)) {
    filtered = filtered.filter(r => r.store_id.toString().includes(store_id));
  }

  if (!isEmpty(databases)) {
    filtered = filtered.filter(r =>
      databases.some((db: string) => r.databases?.includes(db)),
    );
  }

  if (!isEmpty(table)) {
    filtered = filtered.filter(r => r.tables?.includes(table));
  }

  if (!isEmpty(index)) {
    filtered = filtered.filter(r => r.indexes?.includes(index));
  }

  return filtered;
}
