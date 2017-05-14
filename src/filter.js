import SqlString from 'sqlstring';

export const QueryDirection_ASCENDING = 'ASCENDING';
export const QueryDirection_DESCENDING = 'DESCENDING';

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

/**
 * SQL Filter helper class
 * 
 * @class
 */
export default class Filter {
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
      if (filter.groupby) {
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
         if (filter.condition) {
            sql += 'WHERE ';
            const groups = [];
            for (let c = 0; c < filter.condition.length; c++) {
               const cond = filter.condition[c];
               const stmt = cond.conditions.map(cd => {
                  let sql = '`' + cd.field + '` ';
                  switch (cd.operator) {
                     case QueryConditionOperator_EQUAL: {
                        sql += '= ?';
                        params.push(cd.value);
                        break;
                     }
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
                  return sql;
               });
               if (cond.operator === QueryConditionGroupOperator_OR) {
                  groups.push(stmt.join(' OR '))
               } else {
                  groups.push(stmt.join(' AND '))
               }
            }
            if (groups.length > 1) {
               sql += groups.map(g => '(' + g + ')').join(' AND ');
            } else {
               sql += groups[0];
            }
         }
         if (filter.order) {
            sql += ' ORDER BY ' + filter.order.map(o => '`' + o.field + '` ' + (o.direction === QueryDirection_DESCENDING ? 'DESC' : 'ASC')).join(', ');
         }
         if (filter.groupby) {
            sql += ' GROUP BY ' + filter.groupby;
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