import { Pagination, ResultsPerPageLabel } from "cluster-ui/src/pagination";
import React, { useState } from "react";
import { HotRange } from "../../redux/hotRanges/hotRangesReducer";

const pageSize = 50;

interface HotRangesTableProps {
  hotRangesList: HotRange[];
}

const HotRangesTable = ({ hotRangesList }: HotRangesTableProps) => {
  const [currentPage, setCurrentPage] = useState(1);
  if (hotRangesList.length === 0) {
    return <div>No hot ranges</div>;
  }
  console.log(pageSize, currentPage, hotRangesList.length);
  return (
    <div>
      <section>
        <ResultsPerPageLabel
          pagination={{
            pageSize,
            current: currentPage,
            total: hotRangesList.length,
          }}
          pageName={"hot ranges"}
        />
      </section>
      <Pagination
        pageSize={pageSize}
        current={currentPage}
        total={hotRangesList.length}
        onChange={(val) => setCurrentPage(val)}
      />
    </div>
  );
};

export default HotRangesTable;
