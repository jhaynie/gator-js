import test from 'ava';
import Filter from './filter';
import {SQL} from './query';

import {
   QueryConditionOperator_EQ,
   QueryConditionOperator_EQUAL,
   QueryConditionOperator_NOT_EQ,
   QueryConditionOperator_NOT_EQUAL,
   QueryConditionOperator_NULL,
   QueryConditionOperator_NOT_NULL,
   QueryConditionOperator_GREATER,
   QueryConditionOperator_GREATER_EQ,
   QueryConditionOperator_LESS,
   QueryConditionOperator_LESS_EQ,
   QueryConditionOperator_IN,
   QueryConditionOperator_NOT_IN,
   QueryConditionOperator_LIKE,
   QueryConditionOperator_NOT_LIKE,
   QueryConditionOperator_BETWEEN,
   QueryConditionOperator_NOT_BETWEEN,
   QueryConditionOperator_JOIN,
   QueryConditionGroupOperator_AND,
   QueryConditionGroupOperator_OR,
   QueryDirection_ASCENDING,
   QueryDirection_DESCENDING
} from './filter';

test('empty filter', t => {
   t.deepEqual(Filter.toWhere(), {query:'', params:[]})
});

test('simple equal condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_EQUAL,
               value: 'bar'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: ['bar'],
      query: 'WHERE `foo` = ?'
   });
});

test('simple not equal condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_NOT_EQUAL,
               value: 'bar'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: ['bar'],
      query: 'WHERE `foo` != ?'
   });
});

test('simple is null condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_NULL
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: 'WHERE `foo` IS NULL'
   });
});

test('simple is not null condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_NOT_NULL
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: 'WHERE `foo` IS NOT NULL'
   });
});

test('simple is greater condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_GREATER,
               value: 1
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [1],
      query: 'WHERE `foo` > ?'
   });
});

test('simple is greater than equal condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_GREATER_EQ,
               value: 1
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [1],
      query: 'WHERE `foo` >= ?'
   });
});

test('simple is less than condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_LESS,
               value: 1
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [1],
      query: 'WHERE `foo` < ?'
   });
});

test('simple is less than equal condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_LESS_EQ,
               value: 1
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [1],
      query: 'WHERE `foo` <= ?'
   });
});

test('simple is IN condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_IN,
               value: [1]
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [[1]],
      query: 'WHERE `foo` IN (?)'
   });
});

test('simple is NOT IN condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_NOT_IN,
               value: [1]
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [[1]],
      query: 'WHERE `foo` NOT IN (?)'
   });
});

test('simple is LIKE condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_LIKE,
               value: '%bar%'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: ['%bar%'],
      query: 'WHERE `foo` LIKE ?'
   });
});

test('simple is NOT LIKE condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_NOT_LIKE,
               value: '%bar%'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: ['%bar%'],
      query: 'WHERE `foo` NOT LIKE ?'
   });
});

test('simple BETWEEN condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_BETWEEN,
               value: '1 AND 2'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: "WHERE `foo` BETWEEN '1' AND '2'"
   });
});

test('simple NOT BETWEEN condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_NOT_BETWEEN,
               value: '1 AND 2'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: "WHERE `foo` NOT BETWEEN '1' AND '2'"
   });
});

test('simple multi condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_EQUAL,
               value: 'bar'
            },{
               field: 'foo',
               operator: QueryConditionOperator_NOT_EQUAL,
               value: 'foo'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: ['bar', 'foo'],
      query: 'WHERE `foo` = ? AND `foo` != ?'
   });
});

test('complex multi condition default', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_EQUAL,
               value: 'bar'
            },{
               field: 'foo',
               operator: QueryConditionOperator_NOT_EQUAL,
               value: 'foo'
            }]
         },
         {
            conditions: [{
               field: 'bar',
               operator: QueryConditionOperator_EQUAL,
               value: 'bar'
            },{
               field: 'bar',
               operator: QueryConditionOperator_NOT_EQUAL,
               value: 'foo'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: ['bar', 'foo', 'bar', 'foo'],
      query: 'WHERE (`foo` = ? AND `foo` != ?) AND (`bar` = ? AND `bar` != ?)'
   });
});

test('complex multi condition with OR', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_EQUAL,
               value: 'bar'
            },{
               field: 'foo',
               operator: QueryConditionOperator_NOT_EQUAL,
               value: 'foo'
            }],
            operator: QueryConditionGroupOperator_OR
         },
         {
            conditions: [{
               field: 'bar',
               operator: QueryConditionOperator_EQUAL,
               value: 'bar'
            },{
               field: 'bar',
               operator: QueryConditionOperator_NOT_EQUAL,
               value: 'foo'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: ['bar', 'foo', 'bar', 'foo'],
      query: 'WHERE (`foo` = ? OR `foo` != ?) AND (`bar` = ? AND `bar` != ?)'
   });
});

test('complex multi condition with AND', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_EQUAL,
               value: 'bar'
            },{
               field: 'foo',
               operator: QueryConditionOperator_NOT_EQUAL,
               value: 'foo'
            }],
            operator: QueryConditionGroupOperator_AND
         },
         {
            conditions: [{
               field: 'bar',
               operator: QueryConditionOperator_EQUAL,
               value: 'bar'
            },{
               field: 'bar',
               operator: QueryConditionOperator_NOT_EQUAL,
               value: 'foo'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: ['bar', 'foo', 'bar', 'foo'],
      query: 'WHERE (`foo` = ? AND `foo` != ?) AND (`bar` = ? AND `bar` != ?)'
   });
});

test('limit', t => {
   const o = {
      limit: 1
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: 'LIMIT 1'
   });
});

test('range', t => {
   const o = {
      range: {offset: 0, limit:10}
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: 'LIMIT 0,10'
   });
});

test('order default', t => {
   const o = {
      order: [{field:'foo'}]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: 'ORDER BY `foo` ASC'
   });
});

test('order asc', t => {
   const o = {
      order: [{field:'foo', direction: QueryDirection_ASCENDING}]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: 'ORDER BY `foo` ASC'
   });
});

test('order desc', t => {
   const o = {
      order: [{field:'foo', direction: QueryDirection_DESCENDING}]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: 'ORDER BY `foo` DESC'
   });
});

test('order multiple', t => {
   const o = {
      order: [
         {field:'foo', direction: QueryDirection_DESCENDING},
         {field:'bar', direction: QueryDirection_ASCENDING}
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: 'ORDER BY `foo` DESC, `bar` ASC'
   });
});

test('order with table', t => {
   const o = {
      order: [{table:'foo', field:'bar'}]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: 'ORDER BY `foo`.`bar` ASC'
   });
});


test('prepend empty', t => {
   t.deepEqual(Filter.toWherePrepend(null, "a", "b"), {
      params: ['b'],
      query: 'WHERE `a` = ?'
   });
});

test('prepend no condition', t => {
   const o = {
      order: [
         {field:'foo', direction: QueryDirection_DESCENDING},
         {field:'bar', direction: QueryDirection_ASCENDING}
      ]
   };
   t.deepEqual(Filter.toWherePrepend(o, "a", "b"), {
      params: ['b'],
      query: 'WHERE `a` = ? ORDER BY `foo` DESC, `bar` ASC'
   });
});

test('prepend condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_EQUAL,
               value: 'bar'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWherePrepend(o, "a", "b"), {
      params: ['b', 'bar'],
      query: 'WHERE (`a` = ?) AND (`foo` = ?)'
   });
});

test('where join', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo.a=bar.b',
               operator: QueryConditionOperator_JOIN
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: 'WHERE foo.a=bar.b'
   });
});

test('where join conditions', t => {
   t.deepEqual(Filter.toWhereConditions(null, ['foo.a=bar.b']), {
      params: [],
      query: 'WHERE foo.a=bar.b'
   });
});

test('where join conditions with multiple', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo.a=bar.b',
               operator: QueryConditionOperator_JOIN
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhereConditions(o, ['a.b=b.c']), {
      params: [],
      query: 'WHERE (foo.a=bar.b) AND (a.b=b.c)'
   });
});

test('where groupby', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo.a=bar.b',
               operator: QueryConditionOperator_JOIN
            }]
         }
      ],
      groupby: 'foo'
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: 'WHERE foo.a=bar.b GROUP BY foo'
   });
});

test('where groupby with condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo.a=bar.b',
               operator: QueryConditionOperator_JOIN
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhereConditions(o, [], 'foo'), {
      params: [],
      query: 'WHERE foo.a=bar.b GROUP BY foo'
   });
});

test('where order and groupby', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo.a=bar.b',
               operator: QueryConditionOperator_JOIN
            }]
         }
      ],
      order: [{
         field: 'foo'
      }]
   };
   t.deepEqual(Filter.toWhereConditions(o, [], 'foo'), {
      params: [],
      query: 'WHERE foo.a=bar.b GROUP BY foo ORDER BY `foo` ASC'
   });
});

test('where table', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_EQUAL,
               value: 'a'
            }]
         }
      ],
      table: 'bar'
   };
   t.deepEqual(Filter.toWhere(o), {
      params: ['a'],
      query: 'WHERE `bar`.`foo` = ?'
   });
});

test('where table condition', t => {
   const o = {
      condition: [
         {
            conditions: [{
               table: 'bar',
               field: 'foo',
               operator: QueryConditionOperator_EQUAL,
               value: 'a'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: ['a'],
      query: 'WHERE `bar`.`foo` = ?'
   });
});

test('where conditions array is empty', t => {
   const o = {
      condition: []
   };
   t.deepEqual(Filter.toWhere(o), {
      params: [],
      query: ''
   });
});

test('where join filter', t => {
   t.deepEqual(Filter.toJoin(null, 'a', 'b'), {
      params: [],
      query: 'WHERE `a`=`b`'
   });
});

test('where join filter with multiple', t => {
   t.deepEqual(Filter.toJoin(null, 'a,b', 'c,d'), {
      params: [],
      query: 'WHERE `a`=`c` AND `b`=`d`'
   });
   t.deepEqual(Filter.toJoin(null, 'a, b', 'c, d'), {
      params: [],
      query: 'WHERE `a`=`c` AND `b`=`d`'
   });
   t.deepEqual(Filter.toJoin({table:'foo'}, 'a, b', 'c, d'), {
      params: [],
      query: 'WHERE `foo`.`a`=`foo`.`c` AND `foo`.`b`=`foo`.`d`'
   });
   t.deepEqual(Filter.toJoin({table:'foo'}, 'a, b', 'c, d', 'x', 'y'), {
      params: [],
      query: 'WHERE `x`.`a`=`y`.`c` AND `x`.`b`=`y`.`d`'
   });
});

test('where join filter with params', t => {
   t.deepEqual(Filter.toJoinWithParams({xtable:'foo'}, 'a, b', 'c, d', 'x', 'y', {c:1, d:2}), {
      params: [1, 2],
      query: 'WHERE `x`.`a`=`y`.`c` AND `y`.`c` = ? AND `x`.`b`=`y`.`d` AND `y`.`d` = ?'
   });
});

test('where eq and not eq', t => {
   const o = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_EQ,
               value: 'bar'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o), {
      params: ['bar'],
      query: 'WHERE `foo` = ?'
   });

   const o2 = {
      condition: [
         {
            conditions: [{
               field: 'foo',
               operator: QueryConditionOperator_NOT_EQ,
               value: 'bar'
            }]
         }
      ]
   };
   t.deepEqual(Filter.toWhere(o2), {
      params: ['bar'],
      query: 'WHERE `foo` != ?'
   });
});

test('filter instance equals', t => {
   const filter = new Filter().eq('a', 'b').toSQL();
   t.deepEqual(filter, {
      params: ['b'],
      query: 'WHERE `a` = ?'
   });
});

test('filter instance not equals', t => {
   const filter = new Filter().neq('a', 'b').toSQL();
   t.deepEqual(filter, {
      params: ['b'],
      query: 'WHERE `a` != ?'
   });
});

test('filter instance with join', t => {
   const filter = new Filter().join('a', 'afield', 'b', 'bfield').toSQL();
   t.deepEqual(filter, {
      params: [],
      query: 'WHERE `a`.`afield` = `b`.`bfield`'
   });
});

test('filter duplicates', t => {
   const filter = new Filter()
      .join('a', 'afield', 'b', 'bfield')
      .join('a', 'afield', 'b', 'bfield')
      .toSQL();
   t.deepEqual(filter, {
      params: [],
      query: 'WHERE `a`.`afield` = `b`.`bfield`'
   });
   const filter2 = new Filter()
      .join('a', 'afield', 'b', 'bfield')
      .join('a', 'xfield', 'b', 'yfield')
      .toSQL();
   t.deepEqual(filter2, {
      params: [],
      query: 'WHERE (`a`.`afield` = `b`.`bfield`) AND (`a`.`xfield` = `b`.`yfield`)'
   });
   const filter3 = new Filter()
      .join('a', 'afield', 'b', 'bfield')
      .join('a', 'xfield', 'b', 'yfield')
      .join('a', 'afield', 'b', 'bfield')
      .join('a', 'xfield', 'b', 'yfield')
      .toSQL();
   t.deepEqual(filter3, {
      params: [],
      query: 'WHERE (`a`.`afield` = `b`.`bfield`) AND (`a`.`xfield` = `b`.`yfield`)'
   });
   const filter4 = new Filter()
      .eq('a', 'afield', 'f')
      .eq('a', 'afield', 'f')
      .toSQL();
   t.deepEqual(filter4, {
      params: ['afield'],
      query: 'WHERE `f`.`a` = ?'
   });
   const filter5 = new Filter()
      .eq('a', 'afield', 'f')
      .eq('a', 'bfield', 'f')
      .toSQL();
   t.deepEqual(filter5, {
      params: ['afield', 'bfield'],
      query: 'WHERE (`f`.`a` = ?) AND (`f`.`a` = ?)'
   });
});

test('filter or', t => {
   const filter = new Filter()
      .eq('a', 'b')
      .eq('b', 'c')
      .or();
   const {sql, params} = new SQL().table('foo').filter(filter).toSQL();
   t.is(sql, 'SELECT * FROM `foo` WHERE `a` = ? OR `b` = ?');
   t.deepEqual(params, ['b', 'c']);
});