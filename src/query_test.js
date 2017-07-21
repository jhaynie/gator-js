import test from 'ava';
import Filter from './filter';
import Query from './query';
import {
	AllColumns,
	Column,
	ColumnExpression,
	ScopedColumn,
	ScopedColumnExpression,
	SQL,
	Table
} from './query';

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
	t.deepEqual(await Query.exec(db, 'select * from foo', [], MockClass), []);
});

test('simple exec', async t => {
	const db = new MockDB({
	query(sql, params, callback) {
		callback(null, [{
			foo: 'bar'
		}]);
	}
	});
	t.deepEqual(await Query.exec(db, 'select * from foo', [], MockClass), [new MockClass({foo:'bar'})]);
});

test('error', async t => {
	const db = new MockDB({
	query(sql, params, callback) {
		callback(new Error('error'));
	}
	});
	const error = await t.throws(Query.exec(db, 'select * from foo', MockClass, ['foo']));
	t.is(error.message, 'error');
});

test('escape', t => {
	t.is(Query.escape('abc'), `'abc'`);
	t.is(Query.escapeId('abc'), '`abc`');
});

test('filter rows', async t => {
	const db = new MockDB({
	query(sql, params, callback) {
		callback(null, [{
			foo: 'bar'
		}]);
	}
	});
	const fn = (row) => {row.params.count = 1; row};
	const result = await Query.exec(db, 'select * from foo', [], MockClass, fn);
	t.deepEqual(result, [new MockClass({count:1, foo:'bar'})]);
});

test('filter rows with index', async t => {
	const db = new MockDB({
	query(sql, params, callback) {
		callback(null, [{
			foo: 'bar'
		}]);
	}
	});
	const fn = (row, rs, i) => { row.params.i = i; row };
	const result = await Query.exec(db, 'select * from foo', [], MockClass, fn);
	t.deepEqual(result, [new MockClass({i:0, foo:'bar'})]);
});

test('query always returns empty array', async t => {
	const db = new MockDB({
	query(sql, params, callback) {
		callback();
	}
	});
	const fn = (row, rs, i) => { row.params.i = i; row };
	const result = await Query.exec(db, 'select * from foo', [], MockClass, fn);
	t.deepEqual(result, []);
});

test('column', t => {
	t.is(new Column('a').toSQL(), '`a`');
});

test('column with alias', t => {
	t.is(new Column('a', 'b').toSQL(), '`a` as `b`');
});

test('column expression', t => {
	t.is(new ColumnExpression('sum', 'a').toSQL(), 'sum(`a`)');
});

test('column expression with alias', t => {
	t.is(new ColumnExpression('sum', 'a', 'sum').toSQL(), 'sum(`a`) as `sum`');
});

test('scoped column expression', t => {
	t.is(new ScopedColumnExpression('sum', 'foo', 'id').toSQL(), 'sum(`foo`.`id`)');
});

test('scoped column expression with alias', t => {
	t.is(new ScopedColumnExpression('sum', 'foo', 'id', 's').toSQL(), 'sum(`foo`.`id`) as `s`');
});

test('column expression with args', t => {
	const exprs = [
	new ColumnExpression('now'),
	new Column('created_at')
	];
	t.is(new ColumnExpression('datediff', exprs).toSQL(), 'datediff(now(), `created_at`)');
});

test('scoped column', t => {
	t.is(new ScopedColumn('a', 'id').toSQL(), '`a`.`id`');
})

test('scoped column with alias', t => {
	t.is(new ScopedColumn('a', 'id', 'b').toSQL(), '`a`.`id` as `b`');
})

test('wildcard column', t => {
	t.is(new AllColumns().toSQL(), '*');
});

test('wildcard column with table', t => {
	t.is(new AllColumns('foo').toSQL(), '*');
});

test('wildcard column with multiple tables', t => {
	t.is(new AllColumns('foo','bar').toSQL(), '`foo`.*, `bar`.*');
});

test('table', t => {
	t.is(new Table('foo').toSQL(), '`foo`');
});

test('table with alias', t => {
	t.is(new Table('foo','f').toSQL(), '`foo` `f`');
});

test('table with object', t => {
	const o = {table: () => 'foo'};
	t.is(new Table(o).toSQL(), '`foo`');
});

test('table with object and alias', t => {
	const o = {table: () => 'foo'};
	t.is(new Table(o, 'f').toSQL(), '`foo` `f`');
});

test('sql builder', t => {
	const {sql} = new SQL().all().table('foo').toSQL();
	t.is(sql, 'SELECT * FROM `foo`');
});

test('sql builder with no fields', t => {
	const {sql} = new SQL().table('foo').toSQL();
	t.is(sql, 'SELECT * FROM `foo`');
});

test('sql builder with no fields and multiple tables', t => {
	const {sql} = new SQL().table('foo').table('bar').toSQL();
	t.is(sql, 'SELECT `foo`.*, `bar`.* FROM `foo`, `bar`');
});

test('sql builder with no fields and multiple tables with alias', t => {
	const {sql} = new SQL().table('foo', 'f').table('bar', 'b').toSQL();
	t.is(sql, 'SELECT `foo`.*, `bar`.* FROM `foo` `f`, `bar` `b`');
});

test('sql builder with simple column expression', t => {
	const {sql} = new SQL().columnExpr('sum', '*').table('foo').toSQL();
	t.is(sql, 'SELECT sum(*) FROM `foo`');
});

test('sql column express with alias table and column', t => {
	const {sql} = new SQL().scopedColumnExpr('count', 'foo', 'id').table('foo').toSQL();
	t.is(sql, 'SELECT count(`foo`.`id`) FROM `foo`');
});

test('sql column express with alias table and column and alias', t => {
	const {sql} = new SQL().scopedColumnExpr('count', 'a', 'id', 'total').table('foo').toSQL();
	t.is(sql, 'SELECT count(`a`.`id`) as `total` FROM `foo`');
});

test('sql count expression', t => {
	const {sql} = new SQL().count().table('foo').toSQL();
	t.is(sql, 'SELECT count(*) FROM `foo`');
});

test('sql count column expression', t => {
	const {sql} = new SQL().count('id').table('foo').toSQL();
	t.is(sql, 'SELECT count(`id`) FROM `foo`');
});

test('sql count column expression with alias', t => {
	const {sql} = new SQL().count('id', 'c').table('foo').toSQL();
	t.is('SELECT count(`id`) as `c` FROM `foo`', sql);
});

test('sql sum column expression', t => {
	const {sql} = new SQL().sum('id').table('foo').toSQL();
	t.is(sql, 'SELECT sum(`id`) FROM `foo`');
});

test('sql max column expression', t => {
	const {sql} = new SQL().max('id').table('foo').toSQL();
	t.is(sql, 'SELECT max(`id`) FROM `foo`');
});

test('sql min column expression', t => {
	const {sql} = new SQL().min('id').table('foo').toSQL();
	t.is(sql, 'SELECT min(`id`) FROM `foo`');
});

test('sql avg column expression', t => {
	const {sql} = new SQL().avg('id').table('foo').toSQL();
	t.is(sql, 'SELECT avg(`id`) FROM `foo`');
});

test('sql now column expression', t => {
	const {sql} = new SQL().now().table('foo').toSQL();
	t.is(sql, 'SELECT now() FROM `foo`');
});

test('sql datediff column expression', t => {
	const {sql} = new SQL().datediff(SQL.now(), 'created_at').table('foo').toSQL();
	t.is(sql, 'SELECT datediff(now(), `created_at`) FROM `foo`');
});

test('sql datediff column expression with nesting', t => {
	const {sql} = new SQL().datediff(SQL.now(), SQL.from_unixtime('created_at')).table('foo').toSQL();
	t.is(sql, 'SELECT datediff(now(), from_unixtime(`created_at`)) FROM `foo`');
});

test('sql datediff column expression with nesting and math', t => {
	const {sql} = new SQL().datediff(SQL.now(), SQL.from_unixtime(SQL.div('created_at', 1000))).table('foo').toSQL();
	t.is(sql, 'SELECT datediff(now(), from_unixtime(`created_at`/1000)) FROM `foo`');
});

test('sql subtract', t => {
	const {sql} = new SQL().sub(SQL.now(), 1000).table('foo').toSQL();
	t.is(sql, 'SELECT now()-1000 FROM `foo`');
});

test('sql addition', t => {
	const {sql} = new SQL().add(SQL.now(), 1000).table('foo').toSQL();
	t.is(sql, 'SELECT now()+1000 FROM `foo`');
});

test('sql multiply', t => {
	const {sql} = new SQL().mul(SQL.now(), 1000).table('foo').toSQL();
	t.is(sql, 'SELECT now()*1000 FROM `foo`');
});

test('sql divide', t => {
	const {sql} = new SQL().div(SQL.now(), 1000).table('foo').toSQL();
	t.is(sql, 'SELECT now()/1000 FROM `foo`');
});

test('sql modulo', t => {
	const {sql} = new SQL().mod(SQL.now(), 1000).table('foo').toSQL();
	t.is(sql, 'SELECT now()%1000 FROM `foo`');
});

const Issue = {
	table: () => 'issue',
	ID: 'id',
	REF_ID: 'ref_id',
	REF_TYPE: 'ref_type',
	CREATED_AT: 'created_at',
	CLOSED_AT: 'closed_at',
	STATE: 'state',
	CUSTOMER_ID: 'customer_id',
	PROJECT_ID: 'project_id'
};
const IssueProject = {
	table:() => 'issue_project',
	ID: 'id',
	NAME: 'name'
};
const JiraIssue = {
	table: () => 'jira_issue',
	ID: 'id',
	REF_ID: 'ref_id',
	REF_TYPE: 'ref_type',
	PRIORITY_ID: 'priority_id',
	STATUS_ID: 'status_id',
	CUSTOMER_ID: 'customer_id',
	ISSUE_TYPE_ID: 'issue_type_id'
};
const JiraIssuePriority = {
	table: () => 'jira_issue_priority',
	ID: 'id',
	REF_ID: 'ref_id',
	REF_TYPE: 'ref_type',
	CUSTOMER_ID: 'customer_id',
	NAME: 'name',
};
const JiraProjectStatus = {
	table: () => 'jira_project_status',
	ID: 'id',
	REF_ID: 'ref_id',
	REF_TYPE: 'ref_type',
	CUSTOMER_ID: 'customer_id',
	NAME: 'name',
};

test('complex sql', t => {
	const {sql, params} = new SQL()
	.count(SQL.scopedColumn(Issue, Issue.ID), 'total')
	.avg(
		SQL.datediff(
		SQL.now(),
		SQL.from_unixtime(
			SQL.div(
			SQL.scopedColumn(Issue, Issue.CREATED_AT),
			1000
			)
		)
		),
		'daysOpen'
	)
	.scopedColumn(JiraIssuePriority, JiraIssuePriority.NAME, 'priority')
	.scopedColumn(JiraProjectStatus, JiraProjectStatus.NAME, 'status')
	.table(Issue)
	.table(JiraIssue)
	.table(JiraIssuePriority)
	.table(JiraProjectStatus)
	.filter(
		new Filter().join(Issue, Issue.REF_ID, JiraIssue, JiraIssue.ID),
		new Filter().eq(Issue.REF_TYPE, 'REF_TYPE_HERE', Issue),
		new Filter().join(JiraIssue, JiraIssue.PRIORITY_ID, JiraIssuePriority, JiraIssuePriority.ID),
		new Filter().join(JiraIssue, JiraIssue.STATUS_ID, JiraProjectStatus, JiraProjectStatus.ID),
		new Filter().eq(Issue.STATE, 'STATE_VALUE_HERE', Issue),
		new Filter().eq(Issue.CUSTOMER_ID, 'CUSTOMER_ID_HERE', Issue),
		new Filter().eq(JiraIssue.ISSUE_TYPE_ID, 'ISSUE_TYPE_HERE', JiraIssue),
		new Filter().group('priority', 'status')
	)
	.toSQL();
	t.is(sql, 'SELECT count(`issue`.`id`) as `total`, avg(datediff(now(), from_unixtime(`issue`.`created_at`/1000))) as `daysOpen`, `jira_issue_priority`.`name` as `priority`, `jira_project_status`.`name` as `status` FROM `issue`, `jira_issue`, `jira_issue_priority`, `jira_project_status` WHERE (`issue`.`ref_id` = `jira_issue`.`id`) AND (`issue`.`ref_type` = ?) AND (`jira_issue`.`priority_id` = `jira_issue_priority`.`id`) AND (`jira_issue`.`status_id` = `jira_project_status`.`id`) AND (`issue`.`state` = ?) AND (`issue`.`customer_id` = ?) AND (`jira_issue`.`issue_type_id` = ?) GROUP BY priority, status');
	t.deepEqual(params, ['REF_TYPE_HERE', 'STATE_VALUE_HERE', 'CUSTOMER_ID_HERE', 'ISSUE_TYPE_HERE']);
});

test('complex sql terse', t => {
	const {sql, params} = new SQL()
	.count(SQL.scopedColumn(Issue, Issue.ID), 'total')
	.avg(
		SQL.datediff(
		SQL.now(),
		SQL.from_unixtime(
			SQL.div(
			SQL.scopedColumn(Issue, Issue.CREATED_AT),
			1000
			)
		)
		),
		'daysOpen'
	)
	.scopedColumn(JiraIssuePriority, JiraIssuePriority.NAME, 'priority')
	.scopedColumn(JiraProjectStatus, JiraProjectStatus.NAME, 'status')
	.join(Issue, Issue.REF_ID, JiraIssue, JiraIssue.ID)
	.join(JiraIssue, JiraIssue.PRIORITY_ID, JiraIssuePriority, JiraIssuePriority.ID)
	.join(JiraIssue, JiraIssue.STATUS_ID, JiraProjectStatus, JiraProjectStatus.ID)
	.eq(Issue.REF_TYPE, 'REF_TYPE_HERE', Issue)
	.eq(Issue.STATE, 'STATE_VALUE_HERE', Issue)
	.eq(Issue.CUSTOMER_ID, 'CUSTOMER_ID_HERE', Issue)
	.eq(JiraIssue.ISSUE_TYPE_ID, 'ISSUE_TYPE_HERE', JiraIssue)
	.groupby('priority', 'status')
	.toSQL();
	t.is(sql, 'SELECT count(`issue`.`id`) as `total`, avg(datediff(now(), from_unixtime(`issue`.`created_at`/1000))) as `daysOpen`, `jira_issue_priority`.`name` as `priority`, `jira_project_status`.`name` as `status` FROM `issue`, `jira_issue`, `jira_issue_priority`, `jira_project_status` WHERE (`issue`.`ref_id` = `jira_issue`.`id`) AND (`jira_issue`.`priority_id` = `jira_issue_priority`.`id`) AND (`jira_issue`.`status_id` = `jira_project_status`.`id`) AND (`issue`.`ref_type` = ?) AND (`issue`.`state` = ?) AND (`issue`.`customer_id` = ?) AND (`jira_issue`.`issue_type_id` = ?) GROUP BY priority, status');
	t.deepEqual(params, ['REF_TYPE_HERE', 'STATE_VALUE_HERE', 'CUSTOMER_ID_HERE', 'ISSUE_TYPE_HERE']);
});

test('complex sql terse with filter', t => {
	const filter = new Filter().eq(Issue.CUSTOMER_ID, 'CUSTOMER_ID_HERE', Issue);
	const {sql, params} = new SQL()
		.count(SQL.scopedColumn(Issue, Issue.ID), 'total')
		.avg(
			SQL.datediff(
			SQL.now(),
			SQL.from_unixtime(
				SQL.div(
				SQL.scopedColumn(Issue, Issue.CREATED_AT),
				1000
				)
			)
			),
			'daysOpen'
		)
		.scopedColumn(JiraIssuePriority, JiraIssuePriority.NAME, 'priority')
		.scopedColumn(JiraProjectStatus, JiraProjectStatus.NAME, 'status')
		.join(Issue, Issue.REF_ID, JiraIssue, JiraIssue.ID)
		.join(JiraIssue, JiraIssue.PRIORITY_ID, JiraIssuePriority, JiraIssuePriority.ID)
		.join(JiraIssue, JiraIssue.STATUS_ID, JiraProjectStatus, JiraProjectStatus.ID)
		.filter(filter)
		.eq(Issue.REF_TYPE, 'REF_TYPE_HERE', Issue)
		.eq(Issue.STATE, 'STATE_VALUE_HERE', Issue)
		.eq(JiraIssue.ISSUE_TYPE_ID, 'ISSUE_TYPE_HERE', JiraIssue)
		.groupby('priority', 'status')
		.toSQL();
	t.is(sql, 'SELECT count(`issue`.`id`) as `total`, avg(datediff(now(), from_unixtime(`issue`.`created_at`/1000))) as `daysOpen`, `jira_issue_priority`.`name` as `priority`, `jira_project_status`.`name` as `status` FROM `issue`, `jira_issue`, `jira_issue_priority`, `jira_project_status` WHERE (`issue`.`ref_id` = `jira_issue`.`id`) AND (`jira_issue`.`priority_id` = `jira_issue_priority`.`id`) AND (`jira_issue`.`status_id` = `jira_project_status`.`id`) AND (`issue`.`customer_id` = ?) AND (`issue`.`ref_type` = ?) AND (`issue`.`state` = ?) AND (`jira_issue`.`issue_type_id` = ?) GROUP BY priority, status');
	t.deepEqual(params, ['CUSTOMER_ID_HERE', 'REF_TYPE_HERE', 'STATE_VALUE_HERE', 'ISSUE_TYPE_HERE']);
});

test('pass context into SQL and use filterAugmentation function', t => {
	const context = {
		filterAugmentation: (filter, context, cls) => {
			filter = filter || {};
			filter.condition = filter.condition || [];
			filter.condition.push({
				conditions: [{
					table: 'issue',
					field: 'customer_id',
					operator: 'EQUAL',
					value: 'CUSTOMER_ID_HERE'
				}]
			});
			return filter;
		}
	};
	const {sql, params} = new SQL(context, Issue)
		.count(SQL.scopedColumn(Issue, Issue.ID), 'total')
		.avg(
			SQL.datediff(
			SQL.now(),
			SQL.from_unixtime(
				SQL.div(
				SQL.scopedColumn(Issue, Issue.CREATED_AT),
				1000
				)
			)
			),
			'daysOpen'
		)
		.scopedColumn(JiraIssuePriority, JiraIssuePriority.NAME, 'priority')
		.scopedColumn(JiraProjectStatus, JiraProjectStatus.NAME, 'status')
		.join(Issue, Issue.REF_ID, JiraIssue, JiraIssue.ID)
		.join(JiraIssue, JiraIssue.PRIORITY_ID, JiraIssuePriority, JiraIssuePriority.ID)
		.join(JiraIssue, JiraIssue.STATUS_ID, JiraProjectStatus, JiraProjectStatus.ID)
		.eq(Issue.REF_TYPE, 'REF_TYPE_HERE', Issue)
		.eq(Issue.STATE, 'STATE_VALUE_HERE', Issue)
		.eq(JiraIssue.ISSUE_TYPE_ID, 'ISSUE_TYPE_HERE', JiraIssue)
		.groupby('priority', 'status')
		.toSQL();
	t.is(sql, 'SELECT count(`issue`.`id`) as `total`, avg(datediff(now(), from_unixtime(`issue`.`created_at`/1000))) as `daysOpen`, `jira_issue_priority`.`name` as `priority`, `jira_project_status`.`name` as `status` FROM `issue`, `jira_issue`, `jira_issue_priority`, `jira_project_status` WHERE (`issue`.`customer_id` = ?) AND (`issue`.`ref_id` = `jira_issue`.`id`) AND (`jira_issue`.`priority_id` = `jira_issue_priority`.`id`) AND (`jira_issue`.`status_id` = `jira_project_status`.`id`) AND (`issue`.`ref_type` = ?) AND (`issue`.`state` = ?) AND (`jira_issue`.`issue_type_id` = ?) GROUP BY priority, status');
	t.deepEqual(params, ['CUSTOMER_ID_HERE', 'REF_TYPE_HERE', 'STATE_VALUE_HERE', 'ISSUE_TYPE_HERE']);
});

test('implicit table from constructor', t => {
	const {sql, params} = new SQL({}, Issue)
		.column(Issue.PROJECT_ID)
		.datediff(
			SQL.now(),
			SQL.max(
				SQL.from_unixtime(
					SQL.div(
						SQL.scopedColumn(Issue, Issue.CREATED_AT),
						1000
					)
				),
			),
			'days'
		)
		.groupby(Issue.PROJECT_ID).toSQL();
	t.is(sql, 'SELECT `project_id`, datediff(now(), max(from_unixtime(`issue`.`created_at`/1000))) as `days` FROM `issue` GROUP BY project_id');
});

test('greater than field', t => {
	const {sql, params} = new SQL({}, Issue)
		.gt(Issue.CREATED_AT, '1234')
		.toSQL();
	t.is(sql, 'SELECT * FROM `issue` WHERE `created_at` > ?');
	t.deepEqual(params, ['1234']);
});

test('greater than with column expression', t => {
	const {sql, params} = new SQL({}, Issue)
		.gt(
			SQL.from_unixtime(SQL.div(Issue.CREATED_AT, 1000)),
			SQL.now()
		)
		.toSQL();
	t.is(sql, 'SELECT * FROM `issue` WHERE from_unixtime(`created_at`/1000) > now()');
	t.deepEqual(params, []);
});

test('epoch seconds', t => {
	const {sql, params} = new SQL({}, Issue)
		.epochSeconds()
		.toSQL();
	t.is(sql, 'SELECT UNIX_TIMESTAMP() FROM `issue`');
	t.deepEqual(params, []);
});

test('date diff seconds', t => {
	const {sql, params} = new SQL({}, Issue)
		.datediffEpoch(Issue.CLOSED_AT, Issue.CREATED_AT, Issue)
		.toSQL();
	t.is(sql, 'SELECT datediff(from_unixtime(`issue`.`closed_at`/1000), from_unixtime(`issue`.`created_at`/1000)) FROM `issue`');
	t.deepEqual(params, []);
});

test('date diff seconds alias', t => {
	const {sql, params} = new SQL({}, Issue)
		.datediffEpoch(Issue.CLOSED_AT, Issue.CREATED_AT, Issue, 'time')
		.toSQL();
	t.is(sql, 'SELECT datediff(from_unixtime(`issue`.`closed_at`/1000), from_unixtime(`issue`.`created_at`/1000)) as `time` FROM `issue`');
	t.deepEqual(params, []);
});

test('date interval subtraction', t => {
	const q = new SQL({}, Issue)
		.count(Issue.PROJECT_ID, 'total')
		.scopedColumn(Issue, Issue.PROJECT_ID)
		.scopedColumn(Issue, Issue.STATE)
		.avg(SQL.datediffEpoch(Issue.CREATED_AT, Issue.CLOSED_AT, Issue), 'time_to_close')
		.column(IssueProject.NAME)
		.join(Issue, Issue.PROJECT_ID, IssueProject, IssueProject.ID)
		.eq(Issue.REF_TYPE, 'github', Issue)
		.groupby(Issue.PROJECT_ID, Issue.STATE, IssueProject.NAME);
	q.gt(
		SQL.from_unixtime(SQL.div(Issue.CREATED_AT, 1000, Issue)),
		SQL.sub(SQL.now(), 'INTERVAL 7 day')
	);
	const {sql, params} = q.toSQL();
	t.is(sql, 'SELECT count(`project_id`) as `total`, `issue`.`project_id`, `issue`.`state`, avg(datediff(from_unixtime(`issue`.`created_at`/1000), from_unixtime(`issue`.`closed_at`/1000))) as `time_to_close`, `name` FROM `issue`, `issue_project` WHERE (`issue`.`project_id` = `issue_project`.`id`) AND (`issue`.`ref_type` = ?) AND (from_unixtime(`issue`.`created_at`/1000) > now()-INTERVAL 7 day) GROUP BY project_id, state, name');
	t.deepEqual(params, ['github']);
});
