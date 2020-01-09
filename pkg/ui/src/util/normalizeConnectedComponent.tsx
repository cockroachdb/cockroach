import React, { ExoticComponent } from "react";

/*
* normalizeConnectedComponent function returns react element created by wrapping Connected component (which in fact is
* not a 'valid' react component (see: ) and provided properties.
* It is required for passing correct components to Route component.
* For more details see: @types/react/index.d.ts:314
* > "However, we have no way of telling the JSX parser that it's a JSX element type or its props other than
* > by pretending to be a normal component."
* */
// tslint:disable-next-line:variable-name
export const normalizeConnectedComponent = (ConnectedComponent: ExoticComponent) =>
  (props: React.ComponentProps<any>) => <ConnectedComponent {...props} />;
