// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";

import { cockroach } from "src/js/protos";
import { getHotRanges } from "src/util/api";

type HotRange = cockroach.server.serverpb.HotRangesResponseV2.IHotRange;
const HotRangesRequest = cockroach.server.serverpb.HotRangesRequest;

const PAGE_SIZE = 1000;
const REQUEST_TIMEOUT = moment.duration(30, "minutes");

interface HotRangesResult {
  ranges: HotRange[];
  fetchedAt: moment.Moment;
}

/**
 * fetchAllHotRanges fetches all pages of hot ranges for the given node IDs,
 * accumulating results into a single array. Ranges with qps <= 0 are filtered
 * out.
 */
export async function fetchAllHotRanges(
  nodeIds: string[],
): Promise<HotRangesResult> {
  const allRanges: HotRange[] = [];
  let pageToken = "";

  // TODO(jasonlmfong): Move sorting and filtering to the server so we can fetch a single
  // page instead of accumulating all pages client-side.
  let hasMoreData = true;
  while (hasMoreData) {
    const req = new HotRangesRequest({
      nodes: nodeIds,
      page_token: pageToken,
      page_size: PAGE_SIZE,
    });
    const resp = await getHotRanges(req, REQUEST_TIMEOUT);
    allRanges.push(...resp.ranges);

    if (pageToken === resp.next_page_token) {
      hasMoreData = false;
    } else {
      pageToken = resp.next_page_token;
    }
  }

  return {
    ranges: allRanges.filter(r => r?.qps > 0),
    fetchedAt: moment(),
  };
}

/**
 * useHotRanges fetches hot ranges for the specified node IDs using SWR.
 * When nodeIds is empty, no fetch is performed.
 */
export function useHotRanges(nodeIds: number[]) {
  const sortedIds =
    nodeIds.length > 0 ? [...nodeIds].sort((a, b) => a - b) : null;
  const key = sortedIds ? ["hotRanges", ...sortedIds.map(String)] : null;

  const { data, error, isLoading, mutate } =
    util.useSwrWithClusterId<HotRangesResult>(
      key,
      () => fetchAllHotRanges(sortedIds.map(String)),
      {
        revalidateOnFocus: false,
        keepPreviousData: false,
      },
    );

  return {
    hotRanges: data?.ranges ?? [],
    error,
    isLoading,
    lastSetAt: data?.fetchedAt,
    refresh: mutate,
  };
}
