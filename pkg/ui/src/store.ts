import { createHashHistory } from "history";
import {createAdminUIStore} from "ui-modules/src/core/redux/state";

const history = createHashHistory();
const store = createAdminUIStore(history);

export { history, store };
