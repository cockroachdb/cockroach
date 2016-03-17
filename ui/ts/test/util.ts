/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../util/property.ts" />
/// <reference path="../util/querycache.ts" />

suite("Properties", () => {
  let assert = chai.assert;
  suite("Util.Prop", () => {
    let testProp: Utils.Property<string>;
    setup(() => {
      testProp = Utils.Prop("initial");
    });

    test("initial value is correct.", () => {
      assert.equal(testProp(), "initial");
      assert.equal(testProp.Epoch(), 0);
      assert.equal(testProp(), "initial");
      assert.equal(testProp.Epoch(), 0);
    });

    test("setting value works correctly.", () => {
      for (let i = 1; i < 3; i++) {
        let newVal = "new_" + i.toString();
        assert.equal(testProp(newVal), newVal);
        assert.equal(testProp(), newVal);
        assert.equal(testProp.Epoch(), i);
      }
    });

    test("Update() increases epoch.", () => {
      assert.equal(testProp(), "initial");
      assert.equal(testProp.Epoch(), 0);
      for (let i = 1; i < 3; i++) {
        testProp.Update();
        assert.equal(testProp(), "initial");
        assert.equal(testProp.Epoch(), i);
      }
    });
  });

  suite("Util.Computed", () => {
    suite("Single Parent", () => {
      let parent: Utils.Property<string>;
      let recomputeCount: number;
      let computed: Utils.ReadOnlyProperty<string>;
      let computedString = (s: string, n: number) => s + "+computed" + n.toString();

      setup(() => {
        parent = Utils.Prop("initial");
        recomputeCount = 0;
        computed = Utils.Computed(parent, (s: string) => {
          recomputeCount++;
          return computedString(s, recomputeCount);
        });
      });

      test("computed value is lazily computed.", () => {
        assert.equal(computed(), computedString("initial", 1));
        for (let i = 1; i < 3; i++) {
          let newVal = "new_" + i.toString();
          parent(newVal);
          assert.equal(computed(), computedString(newVal, i + 1));
          assert.equal(computed(), computedString(newVal, i + 1));
        }
      });

      test("computed value recomputes if parent epoch changes.", () => {
        assert.equal(computed(), computedString("initial", 1));
        for (let i = 1; i < 3; i++) {
          parent.Update();
          assert.equal(computed(), computedString("initial", i + 1));
          assert.equal(computed(), computedString("initial", i + 1));
        }
      });

      test("computed epoch is always parent epoch.", () => {
        for (let i = 1; i < 3; i++) {
          let newVal = "new_" + i.toString();
          parent(newVal);
          assert.equal(parent.Epoch(), i);
          assert.equal(computed.Epoch(), parent.Epoch());
        }
      });

      test("computed property can depend on another computed property", () => {
        let computed2 = Utils.Computed(computed, (s: string) => s + "+recomputed");
        assert.equal(computed2(), "initial+computed1+recomputed");
        parent("newvalue");
        assert.equal(computed2(), "newvalue+computed2+recomputed");
      });
    });

    suite("Multi-Parent", () => {
      let parents: Utils.Property<string>[];
      let recomputeCount: number;
      let computed: Utils.ReadOnlyProperty<string>;
      let computedString = (s1: string, s2: string, s3: string, s4: string, n: number) => {
        return [s1, s2, s3].join(", ") + " and " + s4 + "(" + n + ")";
      };

      setup(() => {
        parents = [
          Utils.Prop("Leonardo"),
          Utils.Prop("Donatello"),
          Utils.Prop("Raphael"),
          Utils.Prop("Michaelangelo"),
        ];
        recomputeCount = 0;
        computed = Utils.Computed(parents[0], parents[1], parents[2], parents[3],
                                  (s1: string, s2: string, s3: string, s4: string) => {
          recomputeCount++;
          return computedString(s1, s2, s3, s4, recomputeCount);
        });
      });

      test("computed value is lazily recomputed.", () => {
        for (let i = 1; i < 3; i++) {
          parents.forEach((p: Utils.Property<string>) => {
            p(p() + "_" + i.toString());
            assert.equal(p.Epoch(), i);
          });
          assert.equal(computed(), computedString(parents[0](), parents[1](), parents[2](), parents[3](), i));
          assert.equal(computed(), computedString(parents[0](), parents[1](), parents[2](), parents[3](), i));
        }
        assert.equal(computed(), "Leonardo_1_2, Donatello_1_2, Raphael_1_2 and Michaelangelo_1_2(2)");
      });

      test("computed value is recomputed if only one parent changes.", () => {
        for (let i = 1; i < 3; i++) {
          parents[0](parents[0]() + "_" + i.toString());
          assert.equal(computed(), computedString(parents[0](), parents[1](), parents[2](), parents[3](), i));
          assert.equal(computed(), computedString(parents[0](), parents[1](), parents[2](), parents[3](), i));
        }
        assert.equal(computed(), "Leonardo_1_2, Donatello, Raphael and Michaelangelo(2)");
      });

      test("computed epoch is always sum of parent epochs.", () => {
        for (let i = 1; i < 3; i++) {
          parents.forEach((p: Utils.Property<string>) => {
            p(p() + "_" + i.toString());
            assert.equal(p.Epoch(), i);
          });
          assert.equal(computed.Epoch(), parents[0].Epoch() * parents.length);
        }
      });

      test("computed property can depend on multiple computed properties.", () => {
        let parentA = Utils.Prop("A_1");
        let parentB = Utils.Prop("B_1");
        let computedA = Utils.Computed(parentA, (s: string) => "(" + s + ")");
        let computedB = Utils.Computed(parentB, (s: string) => "(" + s + ")");
        let computedFinal = Utils.Computed(computedA, computedB, (a: string, b: string) => [a, b].join(":"));

        // Validate initial value and epochs.
        assert.equal(computedFinal(), "(A_1):(B_1)");
        assert.equal(computedA.Epoch(), 0);
        assert.equal(computedB.Epoch(), 0);
        assert.equal(computedFinal.Epoch(), 0);

        parentA("A_2");
        assert.equal(computedFinal(), "(A_2):(B_1)");
        assert.equal(computedA.Epoch(), 1);
        assert.equal(computedB.Epoch(), 0);
        assert.equal(computedFinal.Epoch(), 1);

        parentB("B_2");
        assert.equal(computedFinal(), "(A_2):(B_2)");
        assert.equal(computedA.Epoch(), 1);
        assert.equal(computedB.Epoch(), 1);
        assert.equal(computedFinal.Epoch(), 2);
      });
    });
  });
});

suite("QueryCache", () => {
  let assert = chai.assert;
  let activePromise: Utils.Property<_mithril.MithrilDeferred<string>>;
  let promiseFn: () => _mithril.MithrilPromise<string> = () => {
    activePromise(m.deferred<string>());
    return activePromise().promise;
  };
  let cache: Utils.QueryCache<string>;

  setup(() => {
    activePromise = Utils.Prop(null);
    cache = new Utils.QueryCache(promiseFn);
  });

  test("Is Initially empty.", () => {
    assert.isFalse(cache.hasData(), "cache should initially be empty");
    assert.isNull(cache.result(), "result was not empty");
    assert.isNull(cache.error(), "error was not initially empty");
  });

  test("Caches one value or error at a time.", () => {
    cache.refresh();
    activePromise().resolve("resolved");
    assert.isTrue(cache.hasData(), "cache should have value");
    assert.equal(cache.result(), "resolved");
    assert.isNull(cache.error(), "error should be empty after result.");

    cache.refresh();
    activePromise().reject(new Error("error"));
    assert.isTrue(cache.hasData(), "cache should have value");
    assert.isNull(cache.result(), "result should be null after error");
    assert.deepEqual(cache.error(), new Error("error"));

    cache.refresh();
    activePromise().resolve("resolved again");
    assert.isTrue(cache.hasData(), "cache should have value");
    assert.equal(cache.result(), "resolved again");
    assert.isNull(cache.error(), "error should be empty after result.");
  });

  test("LastResult remains cached after error.", () => {
    cache.refresh();
    activePromise().resolve("resolved");
    assert.isTrue(cache.hasData(), "cache should have value");
    assert.equal(cache.result(), "resolved");
    assert.equal(cache.lastResult(), "resolved");
    assert.isNull(cache.error(), "error should be empty after result.");

    cache.refresh();
    activePromise().reject(new Error("error"));
    assert.isTrue(cache.hasData(), "cache should have value");
    assert.isNull(cache.result(), "result should be null after error");
    assert.equal(cache.lastResult(), "resolved");
    assert.deepEqual(cache.error(), new Error("error"));

    cache.refresh();
    activePromise().resolve("resolved again");
    assert.isTrue(cache.hasData(), "cache should have value");
    assert.equal(cache.result(), "resolved again");
    assert.equal(cache.lastResult(), "resolved again");
    assert.isNull(cache.error(), "error should be empty after result.");
  });

  test("Cached value not overwritten until next promise resolved.", () => {
    cache.refresh();
    activePromise().resolve("resolved");
    assert.equal(cache.result(), "resolved");
    cache.refresh();
    assert.equal(cache.result(), "resolved");
    activePromise().resolve("resolved again");
    assert.equal(cache.result(), "resolved again");
  });

  test("Call to refresh is no-op if request is already in flight.", () => {
    cache.refresh();
    activePromise().resolve("resolved");
    assert.equal(cache.result(), "resolved");
    cache.refresh();
    cache.refresh();
    cache.refresh();
    activePromise().resolve("resolved again");
    assert.equal(cache.result(), "resolved again");
    assert.equal(activePromise.Epoch(), 2, "redundant refreshes should not have resulted in additional promises");
  });
});
