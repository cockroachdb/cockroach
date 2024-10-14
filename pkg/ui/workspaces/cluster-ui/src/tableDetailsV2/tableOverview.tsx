// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Icon } from "@cockroachlabs/ui-components";
import { Col, Row, Skeleton } from "antd";
import moment from "moment-timezone";
import React from "react";

import { useNodeStatuses } from "src/api";
import { TableDetails } from "src/api/databases/getTableMetadataApi";
import { Tooltip } from "src/components/tooltip";
import { PageSection } from "src/layouts";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import { Timestamp } from "src/timestamp";
import { StoreID } from "src/types/clusterTypes";
import { Bytes, DATE_WITH_SECONDS_FORMAT_24_TZ } from "src/util";

import { TABLE_METADATA_LAST_UPDATED_HELP } from "../constants/tooltipMessages";

type TableOverviewProps = {
  tableDetails: TableDetails;
};

export const TableOverview: React.FC<TableOverviewProps> = ({
  tableDetails,
}) => {
  const { metadata } = tableDetails;
  const {
    nodeIDToRegion,
    storeIDToNodeID,
    isLoading: nodesLoading,
  } = useNodeStatuses();

  // getNodesByRegionDisplayStr returns a string that displays
  // the regions and nodes that the table is replicated across.
  const getNodesByRegionDisplayStr = (): string => {
    if (nodesLoading || !tableDetails?.metadata) {
      return "";
    }
    const nodesByRegion: Record<string, number[]> = {};
    metadata.storeIds.forEach(storeID => {
      const nodeID = storeIDToNodeID[storeID as StoreID];
      const region = nodeIDToRegion[nodeID];
      if (!nodesByRegion[region]) {
        nodesByRegion[region] = [];
      }
      nodesByRegion[region].push(nodeID);
    });
    return Object.entries(nodesByRegion)
      .map(
        ([region, nodes]) =>
          `${region} (${nodes.map(nid => "n" + nid).join(",")})`,
      )
      .join(", ");
  };

  const percentLiveDataWithPrecision = (metadata.percentLiveData * 100).toFixed(
    2,
  );

  const formattedErrorText = metadata.lastUpdateError
    ? "Update error: " + metadata.lastUpdateError
    : null;

  return (
    <>
      <PageSection>
        <SqlBox value={tableDetails.createStatement} size={SqlBoxSize.CUSTOM} />
      </PageSection>
      <PageSection>
        <Row justify={"end"}>
          <Col>
            <Tooltip
              title={formattedErrorText ?? TABLE_METADATA_LAST_UPDATED_HELP}
            >
              <Row gutter={8} align={"middle"} justify={"center"}>
                {metadata.lastUpdateError ? (
                  <Icon fill={"warning"} iconName={"Caution"} />
                ) : (
                  <Icon fill="info" iconName={"InfoCircle"} />
                )}
                <Col>
                  {" "}
                  Last updated:{" "}
                  <Timestamp
                    format={DATE_WITH_SECONDS_FORMAT_24_TZ}
                    time={moment.utc(metadata.lastUpdated)}
                    fallback={"Never"}
                  />
                </Col>
              </Row>
            </Tooltip>
          </Col>
        </Row>
        <Row gutter={8}>
          <Col span={12}>
            <SummaryCard>
              <SummaryCardItem
                label="Size"
                value={Bytes(metadata.replicationSizeBytes)}
              />
              <SummaryCardItem label="Ranges" value={metadata.rangeCount} />
              <SummaryCardItem label="Replicas" value={metadata.replicaCount} />
              <SummaryCardItem
                label="Regions / Nodes"
                value={
                  <Skeleton loading={nodesLoading}>
                    {getNodesByRegionDisplayStr()}
                  </Skeleton>
                }
              />
            </SummaryCard>
          </Col>
          <Col span={12}>
            <SummaryCard>
              <SummaryCardItem
                label="% of Live data"
                value={
                  <div>
                    <div>{percentLiveDataWithPrecision}% </div>
                    <div>
                      {Bytes(metadata.totalLiveDataBytes)} /{" "}
                      {Bytes(metadata.totalLiveDataBytes)}
                    </div>
                  </div>
                }
              />
              <SummaryCardItem
                label="Auto stats collections"
                value={metadata.autoStatsEnabled ? "Enabled" : "Disabled"}
              />
              <SummaryCardItem
                label="Stats last updated"
                value={
                  <Timestamp
                    time={metadata.statsLastUpdated}
                    format={DATE_WITH_SECONDS_FORMAT_24_TZ}
                    fallback={"Never"}
                  />
                }
              />
            </SummaryCard>
          </Col>
        </Row>
      </PageSection>
    </>
  );
};
