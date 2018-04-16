import Enzyme from "enzyme";
import Adapter from "enzyme-adapter-react-16";

// As of v3, Enzyme requires an "adapter" to be initialized.
// See https://github.com/airbnb/enzyme/blob/master/docs/guides/migration-from-2-to-3.md
Enzyme.configure({ adapter: new Adapter() });
