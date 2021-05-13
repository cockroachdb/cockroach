import { assert } from "chai";
import { aggregateStatementStats } from "./appStats";
import { statementsWithSameIdButDifferentNodeId } from "./appStats.fixture";

describe("aggregateStatementStats", () => {
  it("groups duplicate statements by node id", () => {
    const aggregated = aggregateStatementStats(
      statementsWithSameIdButDifferentNodeId,
    );
    assert.equal(aggregated.length, 1);
  });
});
