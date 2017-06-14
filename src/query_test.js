import test from 'ava';
import Query from './query';

class MockDB {
   constructor(handler) {
      this.handler = handler;
   }
   query(sql, params, callback) {
      this.handler.query(sql, params, callback);
   }
}

class MockClass {
   constructor(params) {
      this.params = params;
   }
}

test('empty', async t => {
   const db = new MockDB({
      query(sql, params, callback) {
         callback(null, []);
      }
   });
   t.deepEqual(await Query.exec(db, 'select * from foo', [], MockClass), []);
});

test('simple exec', async t => {
   const db = new MockDB({
      query(sql, params, callback) {
         callback(null, [{
               foo: 'bar'
         }]);
      }
   });
   t.deepEqual(await Query.exec(db, 'select * from foo', [], MockClass), [new MockClass({foo:'bar'})]);
});

test('error', async t => {
   const db = new MockDB({
      query(sql, params, callback) {
         callback(new Error('error'));
      }
   });
   const error = await t.throws(Query.exec(db, 'select * from foo', MockClass, ['foo']));
   t.is(error.message, 'error');
});

test('escape', t => {
   t.is(Query.escape('abc'), `'abc'`);
   t.is(Query.escapeId('abc'), '`abc`');
});

test('filter rows', async t => {
   const db = new MockDB({
      query(sql, params, callback) {
         callback(null, [{
               foo: 'bar'
         }]);
      }
   });
   const fn = (row) => {row.params.count = 1; row};
   const result = await Query.exec(db, 'select * from foo', [], MockClass, fn);
   t.deepEqual(result, [new MockClass({count:1, foo:'bar'})]);
});

test('filter rows with index', async t => {
   const db = new MockDB({
      query(sql, params, callback) {
         callback(null, [{
               foo: 'bar'
         }]);
      }
   });
   const fn = (row, rs, i) => { row.params.i = i; row };
   const result = await Query.exec(db, 'select * from foo', [], MockClass, fn);
   t.deepEqual(result, [new MockClass({i:0, foo:'bar'})]);
});

test('query always returns empty array', async t => {
   const db = new MockDB({
      query(sql, params, callback) {
         callback();
      }
   });
   const fn = (row, rs, i) => { row.params.i = i; row };
   const result = await Query.exec(db, 'select * from foo', [], MockClass, fn);
   t.deepEqual(result, []);
});
