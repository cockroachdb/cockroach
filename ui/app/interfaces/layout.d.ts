import * as React from "react";
import { IRouterProps } from "react-router";

export interface TitledComponent {
  title(routeProps: IRouterProps): React.ReactElement<any>;
}
