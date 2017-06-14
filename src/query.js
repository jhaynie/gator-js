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
   static exec(db, sql, params, Class, cb) {
      return new Promise (
         (resolve, reject) => {
            try {
               db.query(sql, params, (err, result) => {
                  if (err) {
                     return reject(err);
                  }
						if (result && result.length) {
                     let rows = result.map(row => new Class(row));
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
