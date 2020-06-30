import { RouteProps } from "react-router";
import { appAttr, implicitTxnAttr, statementAttr } from "src/core/util/constants";
import { StatementsPage } from "./components/statementsPage";
import { StatementDetails } from "./components/statementDetails";

// TODO (koorosh): refactor to lazy imports.
export const routes: RouteProps[] = [
  {
    path: "/statements",
    component: StatementsPage,
    exact: true,
  },
  {
    path: `/statements/:${appAttr}`,
    component: StatementsPage,
    exact: true,
  },
  {
    path: `/statements/:${appAttr}/:${statementAttr}`,
    component: StatementDetails,
    exact: true,
  },
  {
    path: `/statements/:${appAttr}/:${implicitTxnAttr}/:${statementAttr}`,
    component: StatementDetails,
    exact: true,
  }
]
