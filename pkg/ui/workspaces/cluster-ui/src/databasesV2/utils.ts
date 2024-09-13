// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { DatabaseListFilters, DatabaseRow } from "./databaseTypes";

export const filterDatabases = (
  databases: DatabaseRow[],
  filter: DatabaseListFilters,
): DatabaseRow[] => {
  return databases.filter(db => {
    if (filter.regionValues?.length) {
      // Empty regionValues means no filtering.
      const regions = Object.keys(db.nodesByRegion);
      if (!regions.some(region => filter.regionValues?.includes(region))) {
        return false;
      }
    }
    if (filter.search) {
      if (!db.name.toLowerCase().includes(filter.search.toLowerCase())) {
        return false;
      }
    }
    return true;
  });
};
