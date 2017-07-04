import SqlString from 'sqlstring';
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
   static exec(db, sql, params, Class, cb, context) {
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
