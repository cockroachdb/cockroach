// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const assert = require('assert');
const client = require('./client');
const rejectsWithPGError = require('./rejects-with-pg-error');

['JSON', 'JSONB'].forEach(t => {
  describe(t, () => {
    describe('round-tripping a value', () => {
      const cases = [
        `123`,
        `"hello"`,
        `{}`,
        `[]`,
        `0`,
        `0.0000`,
        `""`,
        '"\uD83D\uDE80"',
        '{"\uD83D\uDE80": "hello"}',
        `[1, 2, 3]`,
        `{"foo": 123}`,
      ];

      before(() => {
        return client.query(`CREATE TABLE x (j ${t})`);
      });

      after(() => {
        return client.query(`DROP TABLE x`);
      });

      cases.forEach(json => {
        describe(json, () => {
          beforeEach(() => {
            return client.query(`DELETE FROM x`);
          });
          it(`can be selected directly`, () => {
            return client.query(`SELECT $1::${t} j`, [json]).then(results => {
              assert.deepStrictEqual(results.rows[0].j, JSON.parse(json));
            });
          });

          it(`can be inserted into a table and then retrieved`, () => {
            return client.query(`INSERT INTO x VALUES ($1)`, [json])
              .then(() => client.query(`SELECT j FROM x`))
              .then(results => {
                assert.deepStrictEqual(results.rows[0].j, JSON.parse(json));
              })
          });
        });
      });
    });

    it('gives the right error code on invalid JSON', () => {
      return rejectsWithPGError(
        client.query({text: `SELECT '{"foo": 123'::JSONB`}),
        {msg: 'unexpected EOF', code: '22P02'}
      );
    });
  });
});
