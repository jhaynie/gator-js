import SqlString from 'sqlstring';
import Filter from './filter';

// marker interfaces
class SQLDefinition {
   constructor() {
   }
}
class ColumnDefinition extends SQLDefinition {
   constructor() {
      super();
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
      this.dontescape = false;
   }
   noescape() {
      this.dontescape = true;
      return this;
   }
   toSQL() {
      if (this.alias) {
         return SqlString.escapeId(this.name) + ' as ' + SqlString.escapeId(this.alias);
      }
      if (this.dontescape || this.name[0] === '`') {
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
   noescape() {
      this.noescape = true;
      return this;
   }
   toSQL() {
      let sql;
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
         sql = this.expr + '()';
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
      if (typeof(this.table) === 'object' && typeof(this.table.table) === 'function') {
         this.table = this.table.table();
      }
   }
   toSQL() {
      let sql = SqlString.escapeId(this.table) + '.' + SqlString.escapeId(this.column);
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
      this.table = table;
      this.column = column;
      this.alias = alias;
   }
   toSQL() {
      let col = SqlString.escapeId(this.table) + '.' + SqlString.escapeId(this.column);
      let sql = this.expr + '(' + col + ')';
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
         return this.tables.map(table => SqlString.escapeId(table) + '.*').join(', ');
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
      // if we pass in a class or an object that defines a table function
      if (typeof(this.name) === 'object' && typeof(this.name.table) === 'function') {
         this.name = this.name.table();
      }
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
   constructor() {
      this.parts = [];
      this.tables = {};
   }
   static now(alias) {
      return new ColumnExpression('now', null, alias).noescape();
   }
   static count(column, alias) {
      return new ColumnExpression('count', column || '*', alias);
   }
   static avg(column, alias) {
      return new ColumnExpression('avg', column, alias);
   }
   static sum(column, alias) {
      return new ColumnExpression('sum', column, alias);
   }
   static min(column, alias) {
      return new ColumnExpression('min', column, alias);
   }
   static max(column, alias) {
      return new ColumnExpression('max', column, alias);
   }
   static datediff(from, to, alias) {
      return new ColumnExpression('datediff', [from, to], alias);
   }
   static from_unixtime(column, alias) {
      return new ColumnExpression('from_unixtime', column, alias);
   }
   static div(col, value) {
      col = col instanceof ColumnDefinition ? col.toSQL() : SqlString.escapeId(col);
      return new Column(col + '/' + value).noescape();
   }
   static mul(col, value) {
      col = col instanceof ColumnDefinition ? col.toSQL() : SqlString.escapeId(col);
      return new Column(col + '*' + value).noescape();
   }
   static add(col, value) {
      col = col instanceof ColumnDefinition ? col.toSQL() : SqlString.escapeId(col);
      return new Column(col + '+' + value).noescape();
   }
   static sub(col, value) {
      col = col instanceof ColumnDefinition ? col.toSQL() : SqlString.escapeId(col);
      return new Column(col + '-' + value).noescape();
   }
   static mod(col, value) {
      col = col instanceof ColumnDefinition ? col.toSQL() : SqlString.escapeId(col);
      return new Column(col + '%' + value).noescape();
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
   count(column, alias) {
      this.parts.push(SQL.count(column, alias));
      return this;
   }
   avg(column, alias) {
      this.parts.push(SQL.avg(column, alias));
      return this;
   }
   sum(column, alias) {
      this.parts.push(SQL.sum(column, alias));
      return this;
   }
   min(column, alias) {
      this.parts.push(SQL.min(column, alias));
      return this;
   }
   max(column, alias) {
      this.parts.push(SQL.max(column, alias));
      return this;
   }
   div(col, value) {
      this.parts.push(SQL.div(col, value));
      return this;
   }
   mod(col, value) {
      this.parts.push(SQL.mod(col, value));
      return this;
   }
   add(col, value) {
      this.parts.push(SQL.add(col, value));
      return this;
   }
   sub(col, value) {
      this.parts.push(SQL.sub(col, value));
      return this;
   }
   mul(col, value) {
      this.parts.push(SQL.mul(col, value));
      return this;
   }
   now(alias) {
      this.parts.push(SQL.now(alias));
      return this;
   }
   datediff(from, to, alias) {
      this.parts.push(SQL.datediff(from, to, alias));
      return this;
   }
   from_unixtime(column, alias) {
      this.parts.push(SQL.from_unixtime(colum, alias));
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
   _table(t, alias) {
      if (typeof(t) === 'object' && typeof(t.table) === 'function') {
         return {name: t.table(), alias: alias};
      } else if (typeof(t) == 'object' && t instanceof TableDefinition) {
         return {name: t.name, alias: t.alias || alias};
      }
      return {name: t, alias: alias};
   }
   _addtable(t, alias) {
      const {name, _alias} = this._table(t, alias);
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
   join(tableA, colA, tableB, colB) {
      this._addtable(tableA);
      this._addtable(tableB);
      return this.filter(new Filter().join(tableA, colA, tableB, colB));
   }
   filter(...filters) {
      this.filters = this.filters || {condition:[]};
      const groupby = [];
      let orders = [], limits = [];
      let range;
      filters.forEach(f => {
         if (f.filter.condition && f.filter.condition.length) {
            this.filters.condition = this.filters.condition.concat(f.filter.condition);
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
      if (groupby.length) {
         this.filters.groupby = groupby.join(', ');
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
      const tables = Object.keys(this.tables);
      if (tables.length === 0) {
         throw new Error('missing tables in SQL');
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
