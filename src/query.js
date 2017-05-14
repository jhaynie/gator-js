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
   static exec(db, sql, params, Class, columns) {
      return new Promise (
         (resolve, reject) => {
            try {
               db.query(sql, params, (err, result) => {
                  if (err) {
                     return reject(err);
                  }
						if (result && result.length) {
							return resolve(result.map(row => {
                        const o = {};
                        columns.map(col => o[col] = row[col]);
                        return new Class(o);
                     }));
                  }
                  resolve();
               });
            } catch (ex) {
               reject(ex);
            }
         }
      );
   }
}
