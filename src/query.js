import SqlString from 'sqlstring';
import Filter from './filter';
import {
   tablename,
   expression,
   QueryDirection_ASCENDING,
   QueryDirection_DESCENDING
} from './filter';

// marker interfaces
class SQLDefinition {
   constructor() {
   }
}
class ColumnDefinition extends SQLDefinition {
   constructor() {
      super();
   }
   noescape() {
      this.dontescape = true;
      return this;
   }
   noinvoke() {
      this.dontinvoke = true;
      return this;
   }
}
class TableDefinition extends SQLDefinition {
   constructor() {
      super();
   }
}
/**
 * Column class
 */
export class Column extends ColumnDefinition {
   constructor(name, alias) {
      super();
      this.name = name;
      this.alias = alias;
   }
   toSQL() {
      if (this.alias) {
         return SqlString.escapeId(this.name) + ' as ' + SqlString.escapeId(this.alias);
      }
      if (this.dontescape || this.name && this.name[0] === '`') {
         return this.name;
      }
      return SqlString.escapeId(this.name);
   }
}

/**
 * Expression column such as SUM(column)
 */
export class ColumnExpression extends ColumnDefinition {
   constructor(expr, column, alias) {
      super();
      this.expr = expr;
      this.column = column;
      this.alias = alias;
   }
   toSQL() {
      let sql;
      if (this.dontinvoke) {
         sql = this.expr;
      } else {
         if (this.column) {
            if (Array.isArray(this.column)) {
               const col = this.column.map(c => {
                  if (c instanceof ColumnDefinition) {
                     return c.toSQL();
                  }
                  return SqlString.escapeId(c);
               });
               sql = this.expr + '(' + col.join(', ') + ')';
            } else {
               if (this.column instanceof ColumnDefinition) {
                  sql = this.expr + '(' + this.column.toSQL() + ')';
               } else {
                  let col = this.column === '*' ? '*' : SqlString.escapeId(this.column);
                  sql = this.expr + '(' + col + ')';
               }
            }
         } else {
            if (this.expr && this.expr.toSQL) {
               sql = this.expr.toSQL();
            } else {
               sql = this.expr + '()';
            }
         }
      }
      if (this.alias) {
         sql += ' as ' + SqlString.escapeId(this.alias);
      }
      return sql;
   }
}

/**
 * ScopedColumn column such table.b
 */
export class ScopedColumn extends ColumnDefinition {
   constructor(table, column, alias) {
      super();
      this.table = table;
      this.column = column;
      this.alias = alias;
      this.table = tablename(table);
   }
   toSQL() {
      let sql = SqlString.escapeId(this.table) + '.' + (this.dontescape ? this.column : SqlString.escapeId(this.column));
      if (this.alias) {
         sql += ' as ' + SqlString.escapeId(this.alias);
      }
      return sql;
   }
}

/**
 * ScopedColumnExpression column such as SUM(table.column)
 */
export class ScopedColumnExpression extends ColumnDefinition {
   constructor(expr, table, column, alias) {
      super();
      this.expr = expr;
      this.table = tablename(table);
      this.column = column;
      this.alias = alias;
   }
   toSQL() {
      let sql;
      if (this.dontinvoke) {
         sql = this.expr;
      } else {
         const prefix = SqlString.escapeId(this.table) + '.';
         if (this.column) {
            if (Array.isArray(this.column)) {
               const col = this.column.map(c => {
                  if (c instanceof ColumnDefinition) {
                     return c.toSQL();
                  }
                  return prefix + SqlString.escapeId(c);
               });
               sql = this.expr + '(' + col.join(', ') + ')';
            } else {
               if (this.column instanceof ColumnDefinition) {
                  sql = this.expr + '(' + this.column.toSQL() + ')';
               } else {
                  let col = this.column === '*' ? '*' : SqlString.escapeId(this.column);
                  sql = this.expr + '(' + prefix + col + ')';
               }
            }
         } else {
            sql = this.expr + '()';
         }
      }
      if (this.alias) {
         sql += ' as ' + SqlString.escapeId(this.alias);
      }
      return sql;
   }
}

/**
 * Expression that represents all columns for one or more tables
 */
export class AllColumns extends ColumnDefinition {
   constructor(...tables) {
      super();
      this.tables = tables;
   }
   toSQL() {
      if (this.tables && this.tables.length > 1) {
         return this.tables.map(table => SqlString.escapeId(tablename(table)) + '.*').join(', ');
      }
      return '*';
   }
}

/**
 * Table definition
 */
export class Table extends TableDefinition {
   constructor(name, alias) {
      super();
      this.name = name;
      this.alias = alias;
      this.name = tablename(name);
   }
   toSQL() {
      let sql = SqlString.escapeId(this.name);
      if (this.alias) {
         sql += ' ' + SqlString.escapeId(this.alias);
      }
      return sql;
   }
}

/**
 * SQL builder
 */
export class SQL {
   constructor(context, cls) {
      this.parts = [];
      this.tables = {};
      this.cls = cls;
      if (context && cls && context.filterAugmentation) {
         this.filter(context.filterAugmentation(null, context, cls));
      }
   }
   static now(alias) {
      return new ColumnExpression('now', null, alias).noescape();
   }
   static epochSeconds(alias) {
      return new ColumnExpression('UNIX_TIMESTAMP()', null, alias).noinvoke();
   }
   static count(column, alias, table) {
      if (table) {
         return new ScopedColumnExpression('count', table, column || '*', alias);
      } else {
         return new ColumnExpression('count', column || '*', alias);
      }
   }
   static avg(column, alias, table) {
      if (table) {
         return new ScopedColumnExpression('avg', table, column, alias);
      } else {
         return new ColumnExpression('avg', column, alias);
      }
   }
   static sum(column, alias, table) {
      if (table) {
         return new ScopedColumnExpression('sum', table, column, alias);
      } else {
         return new ColumnExpression('sum', column, alias);
      }
   }
   static min(column, alias, table) {
      if (table) {
         return new ScopedColumnExpression('min', table, column, alias);
      } else {
         return new ColumnExpression('min', column, alias);
      }
   }
   static max(column, alias, table) {
      if (table) {
        return new ScopedColumnExpression('max', table, column, alias);
      } else {
         return new ColumnExpression('max', column, alias);
      }
   }
   static datediff(from, to, alias, table) {
      return new ColumnExpression('datediff', [from, to], alias);
   }
   static datediffEpoch(from, to, table, alias) {
      from = expression(from);
      to = expression(to);
      if (table) {
         const f = SqlString.escapeId(tablename(table)) + '.' + from;
         const t = SqlString.escapeId(tablename(table)) + '.' + to;
         return new ScopedColumnExpression('datediff(from_unixtime('+f+'/1000), from_unixtime('+t+'/1000))', table, '', alias).noinvoke();
      } else {
         return new ColumnExpression('datediff(from_unixtime('+from+'/1000), from_unixtime('+to+'/1000))', table, '', alias).noinvoke();
      }
   }
   static from_unixtime(column, alias) {
      return new ColumnExpression('from_unixtime', column, alias);
   }
   static date_format(column, fmt, table, alias) {
      column = expression(column);
      if (table) {
         const f = SqlString.escapeId(tablename(table)) + '.' + column;
         const expr = 'date_format(' + f + ', "' + fmt + '")';
         return new ScopedColumnExpression(expr, table, column, alias).noinvoke();
      } else {
         const expr = 'date_format(' + column + ', "' + fmt + '")';
         return new ColumnExpression(expr, null, alias).noinvoke();
      }
   }
   static expr(op, col, value, table, alias) {
      col = expression(col);
      if (table) {
         return new ScopedColumn(table, col + op + value, alias).noescape();
      }
      return new Column(col + op + value).noescape();
   }
   static div(col, value, table, alias) {
      return this.expr('/', col, value, table, alias);
   }
   static mul(col, value, table, alias) {
      return this.expr('*', col, value, table, alias);
   }
   static add(col, value, table, alias) {
      return this.expr('+', col, value, table, alias);
   }
   static sub(col, value, table, alias) {
      return this.expr('-', col, value, table, alias);
   }
   static mod(col, value, table, alias) {
      return this.expr('%', col, value, table, alias);
   }
   static column(name, alias) {
      return new Column(name, alias);
   }
   static columnExpr(expr, column, alias) {
      return new ColumnExpression(expr, column, alias);
   }
   static scopedColumn(table, column, alias) {
      return new ScopedColumn(table, column, alias);
   }
   static scopedColumnExpr(expr, table, column, alias) {
      return new ScopedColumnExpression(expr, table, column, alias);
   }
   static distinct(expr, alias) {
      expr = expression(expr);
      expr = 'distinct(' + expr + ')';
      return new ColumnExpression(expr, null, alias).noinvoke();
   }
   static gt(col, value, table) {
      return new Filter().gt(col, value, table);
   }
   static lt(col, value, table) {
      return new Filter().lt(col, value, table);
   }
   static gte(col, value, table) {
      return new Filter().gte(col, value, table);
   }
   static lte(col, value, table) {
      return new Filter().lte(col, value, table);
   }
   static eq(col, value, table) {
      return new Filter().eq(col, value, table);
   }
   static neq(col, value, table) {
      return new Filter().neq(col, value, table);
   }
   static in(col, value, table) {
      return new Filter().in(col, value, table);
   }
   static nin(col, value, table) {
      return new Filter().nin(col, value, table);
   }
   static like(col, value, table) {
      return new Filter().like(col, value, table);
   }
   static nlike(col, value, table) {
      return new Filter().nlike(col, value, table);
   }
   static null(col, table) {
      return new Filter().null(col, table);
   }
   static notnull(col, table) {
      return new Filter().notnull(col, table);
   }
   distinct(expr, alias) {
      this.parts.push(SQL.distinct(expr, alias));
      return this;
   }
   count(column, alias, table) {
      this._addtable(table);
      this.parts.push(SQL.count(column, alias, table));
      return this;
   }
   avg(column, alias, table) {
      this._addtable(table);
      this.parts.push(SQL.avg(column, alias, table));
      return this;
   }
   sum(column, alias, table) {
      this._addtable(table);
      this.parts.push(SQL.sum(column, alias, table));
      return this;
   }
   min(column, alias, table) {
      this._addtable(table);
      this.parts.push(SQL.min(column, alias, table));
      return this;
   }
   max(column, alias, table) {
      this._addtable(table);
      this.parts.push(SQL.max(column, alias, table));
      return this;
   }
   div(col, value, table, alias) {
      this._addtable(table);
      this.parts.push(SQL.div(col, value, table, alias));
      return this;
   }
   mod(col, value, table, alias) {
      this._addtable(table);
      this.parts.push(SQL.mod(col, value, table, alias));
      return this;
   }
   add(col, value, table, alias) {
      this._addtable(table);
      this.parts.push(SQL.add(col, value, table, alias));
      return this;
   }
   sub(col, value, table, alias) {
      this._addtable(table);
      this.parts.push(SQL.sub(col, value, table, alias));
      return this;
   }
   mul(col, value, table, alias) {
      this._addtable(table);
      this.parts.push(SQL.mul(col, value, table, alias));
      return this;
   }
   now(alias) {
      this.parts.push(SQL.now(alias));
      return this;
   }
   datediffEpoch(from, to, table, alias) {
      this._addtable(table);
      this.parts.push(SQL.datediffEpoch(from, to, table, alias));
      return this;
   }
   datediff(from, to, alias) {
      this.parts.push(SQL.datediff(from, to, alias));
      return this;
   }
   from_unixtime(column, alias) {
      this.parts.push(SQL.from_unixtime(column, alias));
      return this;
   }
   column(name, alias) {
      this.parts.push(SQL.column(name, alias));
      return this;
   }
   columnExpr(expr, column, alias) {
      this.parts.push(SQL.columnExpr(expr, column, alias));
      return this;
   }
   scopedColumn(table, column, alias) {
      this.parts.push(SQL.scopedColumn(table, column, alias));
      return this;
   }
   scopedColumnExpr(expr, table, column, alias) {
      this.parts.push(SQL.scopedColumnExpr(expr, table, column, alias));
      return this;
   }
   epochSeconds(alias) {
      this.parts.push(SQL.epochSeconds(alias));
      return this;
   }
   date_format(col, fmt, alias, table) {
      this.parts.push(SQL.date_format(col, fmt, table, alias));
      return this;
   }
   _table(t, alias) {
      return {name: tablename(t), alias: alias};
   }
   _addtable(t, alias) {
      if (!t) {
         return false;
      }
      const {name, alias:_alias} = this._table(t, alias);
      const n = SqlString.escapeId(name);
      const k = alias || _alias ? n + ' ' + SqlString.escapeId(alias || _alias) : n;
      if (k in this.tables) {
         return false;
      }
      this.tables[k] = {table:name, alias:alias || _alias};
      return true;
   }
   all(...tables) {
      tables.forEach(t => this._addtable(t));
      this.parts.push(new AllColumns(...tables))
      return this;
   }
   table(name, alias) {
      this._addtable(name, alias);
      this.parts.push(new Table(name, alias));
      return this;
   }
   eq(col, value, table) {
      this._addtable(table);
      return this.filter(new Filter().eq(col, value, table));
   }
   neq(col, value, table) {
      this._addtable(table);
      return this.filter(new Filter().neq(col, value, table));
   }
   in(col, value, table) {
      this._addtable(table);
      return this.filter(new Filter().in(col, value, table));
   }
   nin(col, value, table) {
      this._addtable(table);
      return this.filter(new Filter().nin(col, value, table));
   }
   gt(col, value, table) {
      this._addtable(table);
      return this.filter(new Filter().gt(col, value, table));
   }
   lt(col, value, table) {
      this._addtable(table);
      return this.filter(new Filter().lt(col, value, table));
   }
   gte(col, value, table) {
      this._addtable(table);
      return this.filter(new Filter().gte(col, value, table));
   }
   lte(col, value, table) {
      this._addtable(table);
      return this.filter(new Filter().lte(col, value, table));
   }
   between(col, a, b, table) {
      this._addtable(table);
      return this.filter(new Filter().between(col, a, b, table));
   }
   nbetween(col, a, b, table) {
      this._addtable(table);
      return this.filter(new Filter().nbetween(col, a, b, table));
   }
   null(col, table) {
      this._addtable(table);
      return this.filter(new Filter().null(col, table));
   }
   notnull(col, table) {
      this._addtable(table);
      return this.filter(new Filter().notnull(col, table));
   }
   like(col, values, table) {
      this._addtable(table);
      return this.filter(new Filter().like(col, values, table));
   }
   nlike(col, values, table) {
      this._addtable(table);
      return this.filter(new Filter().nlike(col, values, table));
   }
   groupby(...cols) {
      return this.filter(new Filter().group(...cols));
   }
   scopedGroupby(table, column) {
      const g = SqlString.escapeId(tablename(table)) + '.' + SqlString.escapeId(column);
      return this.filter(new Filter().group(g));
   }
   join(tableA, colA, tableB, colB) {
      this._addtable(tableA);
      this._addtable(tableB);
      return this.filter(new Filter().join(tableA, colA, tableB, colB));
   }
   order(...orders) {
      this.filters = this.filters || {};
      this.filters.order = orders.map(o => {
         if (typeof(o) === 'string') {
            return {field: o, direction: QueryDirection_ASCENDING};
         } else {
            return o;
         }
      });
      return this;
   }
   filter(...filters) {
      this.filters = this.filters || {condition:[]};
      const groupby = [];
      let orders = [], limits = [], conds = [], op;
      let range;
      filters.forEach(f => {
         // see if this is the incoming JSON style vs. new Filter style and support it
         if (f.condition) {
            const o = {filter:{}};
            Object.keys(f).forEach(k => o.filter[k] = f[k]);
            f = o;
         }
         if (f.filter.condition && f.filter.condition.length) {
            op = f.operator;
            f.filter.condition.forEach(c => {
               conds = conds.concat(c.conditions);
            });
         }
         if (f.filter.groupby) {
            groupby.push(f.filter.groupby);
         }
         if (f.filter.order) {
            orders = orders.concat(f.filter.order);
         }
         if (f.filter.limit) {
            limits = limits.concat(f.filter.limit);
         }
         if (f.filter.range) {
            range = f.filter.range;
         }
      });
      if (conds.length) {
         this.filters.condition.push({
            operator: op,
            conditions: conds
         });
      }
      if (groupby.length) {
         if (this.filters.groupby) {
            this.filters.groupby += ', ' + groupby.join(', ');
         } else {
            this.filters.groupby = groupby.join(', ');
         }
      }
      if (orders.length) {
         this.filters.order = orders;
      }
      if (limits.length) {
         this.filters.limit = limits;
      }
      if (range) {
         this.filters.range = range;
      }
      return this;
   }
   toSQL() {
      const fields = [], f = [];
      this.parts.forEach(part => {
         if (part instanceof ColumnDefinition) {
            fields.push(part.toSQL());
         } else if (part instanceof TableDefinition) {
            this._addtable(part);
         } else {
            where.push(part);
         }
      });
      Object.keys(this.tables).forEach(k => {
         const e = this.tables[k];
         if (e && e.alias) {
            // if we have an alias, delete any non-alias table references
            delete this.tables[SqlString.escapeId(e.table)];
         }
      });
      let tables = Object.keys(this.tables);
      if (tables.length === 0) {
         if (this.cls) {
            const t = SqlString.escapeId(tablename(this.cls));
            if (t) {
               tables = [t];
            }
         }
         if (tables.length === 0) {
            throw new Error('missing tables in SQL');
         }
      }
      if (!fields.length) {
         const alltables = Object.keys(this.tables).map(k => this.tables[k].table);
         fields.push(new AllColumns(...alltables).toSQL());
      }
      let where = {query: '', params: []};
      if (this.filters) {
         where = Filter.toWhere(this.filters);
      }
      const sql = 'SELECT ' + fields.join(', ') + ' FROM ' + tables.join(', ') + ' ' + where.query;
      return { sql: sql.trim(), params: where.params };
   }
}

/**
 * Query helper class
 * 
 * @class
 */
export default class Query {
   static escapeId(property) {
      return SqlString.escapeId(property);
   }
   static escape(property) {
      return SqlString.escape(property);
   }
   static exec(db, sql, params, Class, cb, context, processor) {
      return new Promise (
         (resolve, reject) => {
            try {
               const ts = Date.now();
               db.query(sql, params, (err, result, cached) => {
                  // expose some useful query metrics
                  // do this before the query result
                  if (context) {
                     if (cached === true) {
                        context.cached = (context.cached || 0) + 1;
                     } else {
                        context.live = (context.live || 0) + 1;
                        context.query_time = (context.query_time || 0) + Date.now() - ts;
                    }
                    if (!err && result) {
                        context.total_records = (context.total_records || 0) + (Array.isArray(result) ? result.length : 1);
                    }
                  }
                  if (err) {
                     return reject(err);
                  }
                  if (processor && typeof(processor) === 'function') {
                     return resolve(processor(result));
                  }
                  if (result && result.length) {
                     let rows = result.map(row => new Class(row, context));
                     if (cb && typeof(cb) === 'function') {
                        rows = rows.map((row, i) => cb(row, result[i], i) || row);
                     }
							return resolve(rows);
                  }
                  resolve([]);
               });
            } catch (ex) {
               reject(ex);
            }
         }
      );
   }
}
