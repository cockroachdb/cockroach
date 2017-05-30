import "src/js/object-assign";
import "whatwg-fetch";
import fetchMock from "fetch-mock";

fetchMock.configure({
  sendAsJson: false,
});

export default fetchMock;
