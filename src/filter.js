import SqlString from 'sqlstring';

export const QueryDirection_ASCENDING = 'ASCENDING';
export const QueryDirection_DESCENDING = 'DESCENDING';

export const QueryConditionOperator_EQ = 'EQ';
export const QueryConditionOperator_NOT_EQ = 'NOT_EQ';
export const QueryConditionOperator_EQUAL = 'EQUAL';
export const QueryConditionOperator_NOT_EQUAL = 'NOT_EQUAL';
export const QueryConditionOperator_NULL = 'NULL';
export const QueryConditionOperator_NOT_NULL = 'NOT_NULL';
export const QueryConditionOperator_GREATER = 'GREATER';
export const QueryConditionOperator_GREATER_EQ = 'GREATER_EQ';
export const QueryConditionOperator_LESS = 'LESS';
export const QueryConditionOperator_LESS_EQ = 'LESS_EQ';
export const QueryConditionOperator_IN = 'IN';
export const QueryConditionOperator_NOT_IN = 'NOT_IN';
export const QueryConditionOperator_BETWEEN = 'BETWEEN';
export const QueryConditionOperator_NOT_BETWEEN = 'NOT_BETWEEN';
export const QueryConditionOperator_LIKE = 'LIKE';
export const QueryConditionOperator_NOT_LIKE = 'NOT_LIKE';
export const QueryConditionOperator_JOIN = 'JOIN';

export const QueryConditionGroupOperator_AND = 'AND';
export const QueryConditionGroupOperator_OR = 'OR';

// support table coming from either a function, a static getter or just a value itself
export const tablename = (o) => {
   const t = typeof(o) === 'object';
   if (t) {
      const tt = typeof(o.table);
      switch(tt) {
         case 'function': {
            return o.table();
         }
         case 'string': {
            return o.table;
         }
      }
      const tn = typeof(o.name);
      if (typeof(tn) === 'string') {
         return o.name;
      }
   }
   return o;
};

/**
 * SQL Filter helper class
 * 
 * @class
 */
export default class Filter {
   constructor() {
      this.filter = {condition: []};
   }
   cond(key, value, operator = QueryConditionOperator_EQ, table) {
      table = tablename(table);
      this.filter.condition.push({
         conditions:[{
            field: key,
            value: value,
            operator: operator,
            table: table
         }]
      });
      return this;
   }
   eq(key, value, table) {
      return this.cond(key, value, QueryConditionOperator_EQ, table);
   }
   neq(key, value, table) {
      return this.cond(key, value, QueryConditionOperator_NOT_EQ, table);
   }
   gt(key, value, table) {
      return this.cond(key, value, QueryConditionOperator_GREATER, table);
   }
   gte(key, value, table) {
      return this.cond(key, value, QueryConditionOperator_GREATER_EQ, table);
   }
   lt(key, value, table) {
      return this.cond(key, value, QueryConditionOperator_LESS, table)
   }
   lte(key, value, table) {
      return this.cond(key, value, QueryConditionOperator_LESS_EQ, table);
   }
   in(key, values, table) {
      return this.cond(key, values, QueryConditionOperator_IN, table);
   }
   nin(key, values, table) {
      return this.cond(key, values, QueryConditionOperator_NOT_IN, table);
   }
   like(key, values, table) {
      return this.cond(key, values, QueryConditionOperator_LIKE, table);
   }
   nlike(key, values, table) {
      return this.cond(key, values, QueryConditionOperator_NOT_LIKE, table);
   }
   between(key, a, b, table) {
      return this.cond(key, a + ' AND ' + b, QueryConditionOperator_BETWEEN, table);
   }
   nbetween(key, a, b, table) {
      return this.cond(key, a + ' AND ' + b, QueryConditionOperator_NOT_BETWEEN, table);
   }
   null(key, table) {
      return this.cond(key, null, QueryConditionOperator_NULL, table);
   }
   notnull(key, table) {
      return this.cond(key, null, QueryConditionOperator_NOT_NULL, table);
   }
   join(aTable, aColumn, bTable, bColumn) {
      let left = SqlString.escapeId(aColumn);
      if (aTable || this.table) {
         left = SqlString.escapeId(tablename(aTable) || this.table) + '.' + left;
      }
      let right = SqlString.escapeId(bColumn);
      if (bTable || this.table) {
         right = SqlString.escapeId(tablename(bTable) || this.table) + '.' + right;
      }
      this.filter.condition.push({
         conditions:[{
            field: left + ' = ' + right,
            operator: QueryConditionOperator_JOIN,
         }]
      });
      return this;
   }
   group(...groups) {
      this.filter.groupby = groups.join(', ');
      return this;
   }
   orderby(column, direction = QueryDirection_DESCENDING, table = this.table) {
      this.filter.order = this.filter.order || [];
      this.filter.order.push({
         field: column,
         table: tablename(table),
         direction: direction
      });
      return this;
   }
   limit(offset, total) {
      if (arguments.length === 2) {
         this.filter.range = {offset: offset, limit: total};
      } else {
         this.filter.limit = offset;
      }
      return this;
   }
   toSQL() {
      // console.log(JSON.stringify(this.filter, null, 2));
      return Filter.toWhere(this.filter);
   }
   /**
    * create a simple join filter which includes params from the right table
    */
   static toJoinWithParams(filter, leftStr, rightStr, leftTable, rightTable, params) {
      filter = filter || {};
      if (!filter.condition) {
         filter.condition = [];
      }
      const leftTok = leftStr.split(',');
      const rightTok = rightStr.split(',');

      if (leftTok.length !== rightTok.length) {
         throw new Error('left join should equal right join number of fields');
      }

      const conds = [];
      filter.condition.push({conditions:conds});

      leftTok.forEach((item, i) => {
         let left = SqlString.escapeId(item.trim());
         if (leftTable || filter.table) {
            left = SqlString.escapeId(tablename(leftTable) || filter.table) + '.' + left;
         }
         const r = rightTok[i].trim();
         let right = SqlString.escapeId(r);
         if (rightTable || filter.table) {
            right = SqlString.escapeId(tablename(rightTable) || filter.table) + '.' + right;
         }
         conds.push({
            field: left + '=' + right,
            operator: QueryConditionOperator_JOIN
         });
         if (params) {
            conds.push({
               table: tablename(rightTable) || filter.table,
               field: r,
               operator: QueryConditionOperator_EQUAL,
               value: params[r]
            });
         }
      });
      return this.toWhere(filter);
   }
   /**
    * create a simple join condition filter
    */
   static toJoin(filter, left, right, leftTable, rightTable) {
      return this.toJoinWithParams(filter, left, right, leftTable, rightTable);
   }
   /**
    * create a filter prepending the key = value to the query
    */
   static toWherePrepend(filter, key, value) {
      filter = filter || {};
      
      if (!filter.condition) {
         filter.condition = [];
      }
      filter.condition.unshift({
         conditions:[{
            field: key,
            value: value,
            operator: QueryConditionOperator_EQUAL
         }]
      });
      return this.toWhere(filter);
   }
   /**
    * construct a where filter using an array of join conditions
    */
   static toWhereConditions(filter, conds, groupby) {
      filter = filter || {};
      if (groupby) {
         filter.groupby = groupby;
      }
      if (!filter.condition) {
         filter.condition = [];
      }
      conds && conds.map(cond => {
         filter.condition.push({
            conditions:[{
               field: cond,
               operator: QueryConditionOperator_JOIN
            }]
         });
      });
      return this.toWhere(filter);
   }
   /**
    * construct a where filter expression as SQL
    */
   static toWhere(filter) {
      if (filter) {
         let sql = '';
         const params = [];
         const statements = {}; // keep hash of statements to elimitate dups
         if (filter.condition && filter.condition.length) {
            sql += 'WHERE ';
            const groups = [];
            for (let c = 0; c < filter.condition.length; c++) {
               const cond = filter.condition[c];
               const stmt = cond.conditions.map(cd => {
                  let sql = '';
                  if (cd.table || filter.table) {
                     sql += '`' + (tablename(cd.table) || filter.table) + '`.';
                  }
                  sql += '`' + cd.field + '` ';
                  switch (cd.operator) {
                     case QueryConditionOperator_EQ:
                     case QueryConditionOperator_EQUAL: {
                        sql += '= ?';
                        params.push(cd.value);
                        break;
                     }
                     case QueryConditionOperator_NOT_EQ:
                     case QueryConditionOperator_NOT_EQUAL: {
                        sql += '!= ?';
                        params.push(cd.value);
                        break;
                     }
                     case QueryConditionOperator_NULL: {
                        sql += 'IS NULL';
                        break;
                     }
                     case QueryConditionOperator_NOT_NULL: {
                        sql += 'IS NOT NULL';
                        break;
                     }
                     case QueryConditionOperator_GREATER: {
                        sql += '> ?';
                        params.push(cd.value);
                        break;
                     }
                     case QueryConditionOperator_GREATER_EQ: {
                        sql += '>= ?';
                        params.push(cd.value);
                        break;
                     }
                     case QueryConditionOperator_LESS: {
                        sql += '< ?';
                        params.push(cd.value);
                        break;
                     }
                     case QueryConditionOperator_LESS_EQ: {
                        sql += '<= ?';
                        params.push(cd.value);
                        break;
                     }
                     case QueryConditionOperator_IN: {
                        sql += 'IN (?)';
                        params.push(cd.value);
                        break;
                     }
                     case QueryConditionOperator_NOT_IN: {
                        sql += 'NOT IN (?)';
                        params.push(cd.value);
                        break;
                     }
                     case QueryConditionOperator_BETWEEN: {
                        const tok = cd.value.split(' AND ');
                        if (tok.length !== 2) {
                           throw new Error('missing `AND` in `BETWEEN` value');
                        }
                        sql += 'BETWEEN ' + SqlString.escape(tok[0].trim()) + ' AND ' + SqlString.escape(tok[1].trim());
                        break;
                     }
                     case QueryConditionOperator_NOT_BETWEEN : {
                        const tok = cd.value.split(' AND ');
                        if (tok.length !== 2) {
                           throw new Error('missing `AND` in `BETWEEN` value');
                        }
                        sql += 'NOT BETWEEN ' + SqlString.escape(tok[0].trim()) + ' AND ' + SqlString.escape(tok[1].trim());
                        break;
                     }
                     case QueryConditionOperator_LIKE : {
                        sql += 'LIKE ?';
                        params.push(cd.value);
                        break;
                     }
                     case QueryConditionOperator_NOT_LIKE : {
                        sql += 'NOT LIKE ?';
                        params.push(cd.value);
                        break;
                     }
                     case QueryConditionOperator_JOIN: {
                        sql = cd.field;
                        break;
                     }
                  }
                  if (!statements[sql]) {
                     statements[sql] = true;
                     return sql;
                  }
               }).filter(x => x);
               if (stmt.length) {
                  if (cond.operator === QueryConditionGroupOperator_OR) {
                     groups.push(stmt.join(' OR '))
                  } else {
                     groups.push(stmt.join(' AND '))
                  }
               }
            }
            if (groups.length > 1) {
               sql += groups.map(g => '(' + g + ')').join(' AND ');
            } else {
               sql += groups[0];
            }
         }
         if (filter.groupby) {
            sql += ' GROUP BY ' + filter.groupby;
         }
         if (filter.order) {
            const order = o => (o.table ? ('`' + o.table + '`.`' + o.field + '` ') : ('`' + o.field + '` ')) + (o.direction === QueryDirection_DESCENDING ? 'DESC' : 'ASC');
            sql += ' ORDER BY ' + filter.order.map(order).join(', ');
         }
         if (filter.limit) {
            sql += ' LIMIT ' + filter.limit;
         }
         if (filter.range) {
            sql += ' LIMIT ' + filter.range.offset + ',' + filter.range.limit;
         }
         return {query:sql.trim(), params:params};
      }
      return {query:'', params:[]};
   }   
}