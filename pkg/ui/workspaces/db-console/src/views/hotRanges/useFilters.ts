import { useEffect, useState } from "react";
import type { History } from "history";
import { useHistory, useLocation } from "react-router-dom";
import { isEmpty } from "lodash";

import { cockroach } from "src/js/protos";

type HotRange = cockroach.server.serverpb.HotRangesResponseV2.IHotRange;

export interface Filters {
  node_ids?: string[];
  store_id?: string;
  databases?: string[];
  table?: string;
  index?: string;
  localities?: string[];
}

// useFilters is a custom hook for managing the filter state of the
// hot ranges page. It is separate from the state management
// for the filters component. It both manages the state for the filters,
// as well as loads, persists it to the URL so that deep linking
// is possible.
export default function useFilters() {
  // On initial mount, we attempt to load the filter state
  // from session storage or the query parameters.
  const { search } = useLocation(); // Add this
  const getInitialFilters = (() => {
    return loadSearchParams(new URLSearchParams(search)) || {};
  });
  const [filters, setFilters] = useState<Filters>(getInitialFilters());
  const history = useHistory();

  function applyFilters(newFilters: Filters) {
    setSearchParams(history, newFilters);
    setFilters(newFilters);
  }

  return { filters, applyFilters };
}

function loadSearchParams(query: URLSearchParams): Filters {
  const loadArray = (key: string) => {
    const raw = query.get(key);
    if (raw) {
      return raw.split(",");
    }
    return [];
  };

  return {
    node_ids: loadArray("node_ids"),
    databases: loadArray("databases"),
    localities: loadArray("localities"),
    store_id: query.get("store_id"),
    table: query.get("table"),
    index: query.get("index"),
  };
}

function setSearchParams(history: History, filters: Filters) {
  const params = new URLSearchParams();
  filters.node_ids?.length &&
    params.set("node_ids", filters.node_ids?.join(","));
  filters.localities?.length && params.set("localities", filters.localities?.join(","));
  filters.databases?.length && params.set("databases", filters.databases?.join(","));
  filters.store_id && params.set("store_id", filters.store_id);
  filters.table && params.set("table", filters.table);
  filters.index && params.set("index", filters.index);
  history.location.search = params.toString();
  history.replace(history.location);
}

export function filterRanges(
  ranges: HotRange[],
  filters: Filters,
  nodeIdToLocalityMap: Map<number, string>,
): any[] {
  const { store_id, databases, table, index, localities } = filters;
  var filtered = ranges;

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

  if (!isEmpty(localities)) {
    filtered = filtered.filter(r => {
      const locality = nodeIdToLocalityMap.get(r.node_id);
      return localities.some((l: string) => l === locality);
    });
  }

  return filtered;
}
