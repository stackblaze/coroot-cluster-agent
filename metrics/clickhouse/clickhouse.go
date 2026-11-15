// Package clickhouse collects ClickHouse server statistics from its system
// tables over the HTTP interface (no settings are changed, so a read-only
// user works). Each target is one server (replica).
package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	dUp          = common.Desc("clickhouse_up", "")
	dScrapeError = common.Desc("clickhouse_scrape_error", "", "error")
	dInfo        = common.Desc("clickhouse_info", "", "version")
	dUptime      = common.Desc("clickhouse_uptime_seconds", "Server uptime")

	dMetric      = common.Desc("clickhouse_metric", "Current value from system.metrics", "metric")
	dEvent       = common.Desc("clickhouse_events_total", "Counter from system.events", "event")
	dAsyncMetric = common.Desc("clickhouse_async_metric", "Value from system.asynchronous_metrics", "metric")

	dReplicatedTables    = common.Desc("clickhouse_replicated_tables", "Number of replicated tables on this server")
	dMutationsUnfinished = common.Desc("clickhouse_mutations_unfinished", "Number of mutations not yet done")
	dMutationsFailing    = common.Desc("clickhouse_mutations_failing", "Number of unfinished mutations whose last attempt failed")
	dRunningQueries      = common.Desc("clickhouse_running_queries", "Queries of other users running now")
	dLongestQuery        = common.Desc("clickhouse_longest_query_seconds", "Elapsed time of the longest running query of other users")
	dPartsToThrowInsert  = common.Desc("clickhouse_parts_to_throw_insert", "Active parts in a partition at which inserts are rejected (merge tree setting)")
	dPartsToDelayInsert  = common.Desc("clickhouse_parts_to_delay_insert", "Active parts in a partition at which inserts are slowed down (merge tree setting)")

	// User tables and other users' queries only: the server's own log tables
	// (database system) are written all the time and this collector queries
	// every scrape, which would hide what the application does.
	dTablesBytes      = common.Desc("clickhouse_tables_size_bytes", "Size of the active parts of all tables outside the system database")
	dTablesRows       = common.Desc("clickhouse_tables_rows", "Rows in the active parts of all tables outside the system database")
	dMaxPartsInPart   = common.Desc("clickhouse_max_parts_in_partition", "Active parts of the fullest partition outside the system database")
	dQueriesRate      = common.Desc("clickhouse_queries_per_second", "Finished queries of other users over the last minute (system.query_log)", "kind")
	dFailedRate       = common.Desc("clickhouse_failed_queries_per_second", "Failed queries of other users over the last minute")
	dQueryTimeRate    = common.Desc("clickhouse_query_seconds_per_second", "Time spent in queries of other users over the last minute")
	dInsertedRowsRate = common.Desc("clickhouse_inserted_rows_per_second", "Rows written by INSERT queries of other users over the last minute")

	dTableBytes = common.Desc("clickhouse_table_size_bytes", "Size of the table's active parts on disk", "db", "table")
	dTableRows  = common.Desc("clickhouse_table_rows", "Number of rows in the table's active parts", "db", "table")
	dTableParts = common.Desc("clickhouse_table_parts", "Number of active parts of the table", "db", "table")
)

const (
	maxResponseSize = 16 << 20
	topTables       = 20
	// queryLogWindow is how far back qUserQueries looks, in seconds.
	queryLogWindow = 60
)

// Everything is cast to Float64 (64-bit integers are quoted in JSON by
// default) and read with `FORMAT JSONCompact`.
const (
	qInfo = `SELECT version(), toFloat64(uptime())`

	qValues = `
SELECT 'metric' AS kind, metric AS name, toFloat64(value) AS value FROM system.metrics WHERE metric IN (
	'Query', 'Merge', 'PartMutation', 'TCPConnection', 'HTTPConnection', 'MySQLConnection', 'PostgreSQLConnection',
	'InterserverConnection', 'ReplicatedFetch', 'ReplicatedSend', 'ReplicatedChecks', 'ReadonlyReplica', 'DelayedInserts',
	'MemoryTracking', 'ZooKeeperSession', 'ZooKeeperRequest', 'BackgroundMergesAndMutationsPoolTask',
	'BackgroundMergesAndMutationsPoolSize', 'BackgroundFetchesPoolTask', 'BackgroundFetchesPoolSize')
UNION ALL
SELECT 'event', event, toFloat64(value) FROM system.events WHERE event IN (
	'Query', 'SelectQuery', 'InsertQuery', 'FailedQuery', 'FailedSelectQuery', 'FailedInsertQuery',
	'QueryTimeMicroseconds', 'SelectQueryTimeMicroseconds', 'InsertQueryTimeMicroseconds',
	'InsertedRows', 'InsertedBytes', 'SelectedRows', 'SelectedBytes', 'DelayedInserts', 'RejectedInserts',
	'MergedRows', 'MergedUncompressedBytes', 'ReplicatedPartFetches', 'ReplicatedPartFailedFetches',
	'ZooKeeperHardwareExceptions', 'ZooKeeperUserExceptions', 'QueryMemoryLimitExceeded')
UNION ALL
SELECT 'async', metric, toFloat64(value) FROM system.asynchronous_metrics WHERE metric IN (
	'MaxPartCountForPartition', 'ReplicasMaxAbsoluteDelay', 'ReplicasMaxQueueSize', 'ReplicasMaxInsertsInQueue',
	'ReplicasMaxMergesInQueue', 'NumberOfDatabases', 'NumberOfTables', 'TotalPartsOfMergeTreeTables',
	'TotalRowsOfMergeTreeTables', 'TotalBytesOfMergeTreeTables')
UNION ALL
SELECT 'setting', name, toFloat64OrZero(value) FROM system.merge_tree_settings WHERE name IN (
	'parts_to_throw_insert', 'parts_to_delay_insert')`

	qState = `
SELECT
	toFloat64((SELECT count() FROM system.replicas)),
	toFloat64((SELECT count() FROM system.mutations WHERE NOT is_done)),
	toFloat64((SELECT count() FROM system.mutations WHERE NOT is_done AND latest_fail_reason != '')),
	toFloat64((SELECT max(elapsed) FROM system.processes WHERE user != currentUser())),
	toFloat64((SELECT count() FROM system.processes WHERE user != currentUser()))`

	qUserParts = `
SELECT toFloat64(sum(b)), toFloat64(sum(r)), toFloat64(max(c))
FROM (
	SELECT sum(bytes_on_disk) AS b, sum(rows) AS r, count() AS c
	FROM system.parts
	WHERE active AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
	GROUP BY database, table, partition_id
)`
)

// What other users ran in the last queryLogWindow seconds. system.events
// counts every query and every written row, including this collector's
// queries and the server's own log tables, so it cannot tell an idle
// database from a busy one; the query log can.
var qUserQueries = fmt.Sprintf(`
SELECT
	toFloat64(countIf(query_kind = 'Select')),
	toFloat64(countIf(query_kind = 'Insert')),
	toFloat64(countIf(query_kind NOT IN ('Select', 'Insert'))),
	toFloat64(countIf(type != 'QueryFinish')),
	toFloat64(sum(query_duration_ms)) / 1000,
	toFloat64(sumIf(written_rows, query_kind = 'Insert'))
FROM system.query_log
WHERE event_date >= yesterday() AND event_time > now() - %d
	AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
	AND is_initial_query AND user != currentUser()`, queryLogWindow)

var qTables = fmt.Sprintf(`
SELECT database, table, toFloat64(sum(bytes_on_disk)), toFloat64(sum(rows)), toFloat64(count())
FROM system.parts
WHERE active AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
GROUP BY database, table
ORDER BY sum(bytes_on_disk) DESC
LIMIT %d`, topTables)

type Collector struct {
	baseURL  string
	username string
	password string
	client   *http.Client
	timeout  time.Duration
	logger   logger.Logger
}

// New returns a collector for the server whose HTTP interface is at baseURL
// (scheme://host:port).
func New(baseURL, username, password string, client *http.Client, timeout time.Duration, logger logger.Logger) *Collector {
	return &Collector{
		baseURL:  strings.TrimRight(baseURL, "/"),
		username: username,
		password: password,
		client:   client,
		timeout:  timeout,
		logger:   logger,
	}
}

func (c *Collector) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dUp
}

// query runs q and returns its rows (each a list of JSON values).
func (c *Collector) query(ctx context.Context, q string) ([][]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/", strings.NewReader(q+"\nFORMAT JSONCompact"))
	if err != nil {
		return nil, err
	}
	if c.username != "" {
		req.Header.Set("X-ClickHouse-User", c.username)
		req.Header.Set("X-ClickHouse-Key", c.password)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		msg := strings.TrimSpace(string(body))
		if len(msg) > 200 {
			msg = msg[:200]
		}
		return nil, fmt.Errorf("%s: %s", resp.Status, msg)
	}
	var res struct {
		Data [][]any `json:"data"`
	}
	if err = json.Unmarshal(body, &res); err != nil {
		return nil, err
	}
	return res.Data, nil
}

func str(row []any, i int) string {
	if i >= len(row) {
		return ""
	}
	s, _ := row[i].(string)
	return s
}

// num is the value at i; nan/inf and NULL arrive as JSON null and read as 0.
func num(row []any, i int) float64 {
	if i >= len(row) {
		return 0
	}
	f, _ := row[i].(float64)
	return f
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.query(ctx, qInfo)
	if err != nil || len(rows) == 0 {
		// Nothing else can work if this fails (connection or login failed).
		if err == nil {
			err = fmt.Errorf("empty response")
		}
		c.logger.Warning(err)
		ch <- common.Gauge(dUp, 0)
		ch <- common.Gauge(dScrapeError, 1, err.Error())
		return
	}
	ch <- common.Gauge(dUp, 1)
	ch <- common.Gauge(dInfo, 1, str(rows[0], 0))
	ch <- common.Gauge(dUptime, num(rows[0], 1))

	fail := func(err error) {
		c.logger.Warning(err)
		ch <- common.Gauge(dScrapeError, 1, err.Error())
	}

	if rows, err = c.query(ctx, qValues); err != nil {
		fail(err)
	} else {
		for _, r := range rows {
			name, v := str(r, 1), num(r, 2)
			switch str(r, 0) {
			case "metric":
				// This scrape is itself one HTTP connection.
				if name == "HTTPConnection" && v >= 1 {
					v--
				}
				ch <- common.Gauge(dMetric, v, name)
			case "event":
				ch <- common.Counter(dEvent, v, name)
			case "async":
				ch <- common.Gauge(dAsyncMetric, v, name)
			case "setting":
				switch name {
				case "parts_to_throw_insert":
					ch <- common.Gauge(dPartsToThrowInsert, v)
				case "parts_to_delay_insert":
					ch <- common.Gauge(dPartsToDelayInsert, v)
				}
			}
		}
	}

	if rows, err = c.query(ctx, qState); err != nil {
		fail(err)
	} else if len(rows) > 0 {
		ch <- common.Gauge(dReplicatedTables, num(rows[0], 0))
		ch <- common.Gauge(dMutationsUnfinished, num(rows[0], 1))
		ch <- common.Gauge(dMutationsFailing, num(rows[0], 2))
		ch <- common.Gauge(dLongestQuery, num(rows[0], 3))
		ch <- common.Gauge(dRunningQueries, num(rows[0], 4))
	}

	if rows, err = c.query(ctx, qUserParts); err != nil {
		fail(err)
	} else if len(rows) > 0 {
		ch <- common.Gauge(dTablesBytes, num(rows[0], 0))
		ch <- common.Gauge(dTablesRows, num(rows[0], 1))
		ch <- common.Gauge(dMaxPartsInPart, num(rows[0], 2))
	}

	// The query log can be switched off, and its table only exists after the
	// first flush: no series then, not a scrape error.
	if rows, err = c.query(ctx, qUserQueries); err != nil {
		c.logger.Warning("query log:", err)
	} else if len(rows) > 0 {
		r := rows[0]
		ch <- common.Gauge(dQueriesRate, num(r, 0)/queryLogWindow, "select")
		ch <- common.Gauge(dQueriesRate, num(r, 1)/queryLogWindow, "insert")
		ch <- common.Gauge(dQueriesRate, num(r, 2)/queryLogWindow, "other")
		ch <- common.Gauge(dFailedRate, num(r, 3)/queryLogWindow)
		ch <- common.Gauge(dQueryTimeRate, num(r, 4)/queryLogWindow)
		ch <- common.Gauge(dInsertedRowsRate, num(r, 5)/queryLogWindow)
	}

	if rows, err = c.query(ctx, qTables); err != nil {
		fail(err)
	} else {
		for _, r := range rows {
			db, table := str(r, 0), str(r, 1)
			ch <- common.Gauge(dTableBytes, num(r, 2), db, table)
			ch <- common.Gauge(dTableRows, num(r, 3), db, table)
			ch <- common.Gauge(dTableParts, num(r, 4), db, table)
		}
	}
}
