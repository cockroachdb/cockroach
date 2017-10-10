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

      cases.forEach(json => {
        it(`${json}`, () => {
          return client.query(`SELECT '${json}'::${t} j`).then(results => {
            assert.deepStrictEqual(results.rows[0].j, JSON.parse(json));
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
