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
   t.falsy(await Query.exec(db, 'select * from foo', [], MockClass, ['foo']));
});

test('simple exec', async t => {
   const db = new MockDB({
      query(sql, params, callback) {
         callback(null, [{
               foo: 'bar'
         }]);
      }
   });
   t.deepEqual(await Query.exec(db, 'select * from foo', [], MockClass, ['foo']), [new MockClass({foo:'bar'})]);
});

test('error', async t => {
   const db = new MockDB({
      query(sql, params, callback) {
         callback(new Error('error'));
      }
   });
   const error = await t.throws(Query.exec(db, 'select * from foo', [], MockClass, ['foo']));
   t.is(error.message, 'error');
});

test('escape', t => {
   t.is(Query.escape('abc'), `'abc'`);
   t.is(Query.escapeId('abc'), '`abc`');
});
