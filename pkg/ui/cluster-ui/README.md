# Cluster UI Components

This library contains components used by the CockroachDB Console and CockroachCloud to display cluster-level information. 

```
  npm install --save-dev @cockroachlabs/cluster-ui
```

Components are exported individually from the package,

```javascript
import { Drawer } from "@cockroachlabs/cluster-ui";

export default props => (
  <div>
    <Drawer />
  </div>
);
```
