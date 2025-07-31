// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Col, Row } from "antd";
import React, { useContext } from "react";

import { useNodeStatuses } from "src/api";
import { TableDetails } from "src/api/databases/getTableMetadataApi";
import { LiveDataPercent } from "src/components/liveDataPercent/liveDataPercent";
import { TableMetadataLastUpdatedTooltip } from "src/components/tooltipMessages/tableMetadataLastUpdatedTooltip";
import { ClusterDetailsContext } from "src/contexts";
import { PageSection } from "src/layouts";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import { Timestamp } from "src/timestamp";
import { Bytes, DATE_WITH_SECONDS_FORMAT_24_TZ } from "src/util";
import { mapStoreIDsToNodeRegions } from "src/util/nodeUtils";

import { RegionNodesLabel } from "../components/regionNodesLabel";

type TableOverviewProps = {
  tableDetails: TableDetails;
};

export const TableOverview: React.FC<TableOverviewProps> = ({
  tableDetails,
}) => {
  const clusterDetails = useContext(ClusterDetailsContext);
  const isTenant = clusterDetails.isTenant;
  const metadata = tableDetails.metadata;
  const {
    nodeStatusByID,
    storeIDToNodeID,
    isLoading: nodesLoading,
  } = useNodeStatuses();

  const regionsToNodes = mapStoreIDsToNodeRegions(
    tableDetails.metadata.storeIds,
    nodeStatusByID,
    storeIDToNodeID,
  );

  return (
    <>
      <PageSection>
        <SqlBox value={tableDetails.createStatement} size={SqlBoxSize.CUSTOM} />
      </PageSection>
      <PageSection>
        <Row justify={"end"}>
          <Col>
            <TableMetadataLastUpdatedTooltip
              hasError={!!metadata.lastUpdateError}
              timestamp={metadata.lastUpdated}
            >
              {(durationText, icon) => (
                <>
                  {icon}
                  <span> Last updated: {durationText} </span>
                </>
              )}
            </TableMetadataLastUpdatedTooltip>
          </Col>
        </Row>
      </PageSection>
      <PageSection>
        <Row gutter={8}>
          <Col span={12}>
            <SummaryCard>
              <SummaryCardItem
                label="Size"
                value={Bytes(metadata.replicationSizeBytes)}
              />
              <SummaryCardItem label="Ranges" value={metadata.rangeCount} />
              <SummaryCardItem label="Replicas" value={metadata.replicaCount} />
              {!isTenant && (
                <SummaryCardItem
                  label="Regions / Nodes"
                  value={
                    <RegionNodesLabel
                      nodesByRegion={regionsToNodes}
                      loading={nodesLoading}
                    />
                  }
                />
              )}
            </SummaryCard>
          </Col>
          <Col span={12}>
            <SummaryCard>
              <SummaryCardItem
                label="% of Live data"
                value={
                  <LiveDataPercent
                    liveBytes={metadata.totalLiveDataBytes}
                    totalBytes={metadata.totalLiveDataBytes}
                  />
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
