import "../js/object-assign";
import fetchMock from "fetch-mock";

fetchMock.configure({
  sendAsJson: false,
});

export default fetchMock;
