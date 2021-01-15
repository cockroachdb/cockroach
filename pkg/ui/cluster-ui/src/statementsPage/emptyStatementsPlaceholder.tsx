import React from "react";
import { EmptyTable, EmptyTableProps } from "src/empty";
import { Anchor } from "src/anchor";
import { statementsTable } from "src/util";
import magnifyingGlassImg from "../assets/emptyState/magnifying-glass.svg";
import emptyTableResultsImg from "../assets/emptyState/empty-table-results.svg";

export const EmptyStatementsPlaceholder: React.FC<{
  isEmptySearchResults: boolean;
}> = ({ isEmptySearchResults }) => {
  const footer = (
    <Anchor href={statementsTable} target="_blank">
      Learn more about statements
    </Anchor>
  );

  const emptyPlaceholderProps: EmptyTableProps = isEmptySearchResults
    ? {
        title:
          "No SQL statements match your search since this page was last cleared",
        icon: magnifyingGlassImg,
        footer,
      }
    : {
        title: "No SQL statements since this page was last cleared",
        icon: emptyTableResultsImg,
        message:
          "Statements are cleared every hour by default, or according to your configuration.",
        footer,
      };
  return <EmptyTable {...emptyPlaceholderProps} />;
};
