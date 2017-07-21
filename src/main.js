import * as Filter from './filter';
import * as Query from './query';

const o = {
	Filter: Filter.default,
	Query: Query.default
};

Object.keys(Filter).forEach(k => {
	if (k !== 'default') {
		o[k] = Filter[k];
	}
});

Object.keys(Query).forEach(k => {
	if (k !== 'default') {
		o[k] = Query[k];
	}
});

export default o;
