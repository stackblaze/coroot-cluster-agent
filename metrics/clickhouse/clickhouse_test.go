package clickhouse

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// collect returns every sample as name{label=value,...} -> value.
func collect(t *testing.T, c prometheus.Collector) map[string]float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 1000)
	c.Collect(ch)
	close(ch)
	res := map[string]float64{}
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatal(err)
		}
		key := strings.SplitN(strings.TrimPrefix(m.Desc().String(), `Desc{fqName: "`), `"`, 2)[0]
		for _, l := range d.Label {
			key += "," + l.GetName() + "=" + l.GetValue()
		}
		v := d.GetGauge().GetValue()
		if d.Counter != nil {
			v = d.GetCounter().GetValue()
		}
		res[key] = v
	}
	return res
}

// server answers each of the collector's queries; failOn makes the query
// containing that text fail the way ClickHouse does.
func server(t *testing.T, failOn string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-User") != "coroot" || r.Header.Get("X-ClickHouse-Key") != "secret" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("Code: 516. DB::Exception: coroot: Authentication failed"))
			return
		}
		if r.URL.RawQuery != "" {
			t.Errorf("settings must not be passed in the URL (read-only users): %s", r.URL.RawQuery)
		}
		body, _ := io.ReadAll(r.Body)
		q := string(body)
		if !strings.HasSuffix(q, "FORMAT JSONCompact") {
			t.Errorf("query without the format: %s", q)
		}
		if failOn != "" && strings.Contains(q, failOn) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Code: 497. DB::Exception: coroot: Not enough privileges"))
			return
		}
		var data string
		switch {
		case strings.Contains(q, "version()"):
			data = `[["25.3.6.56", 3600]]`
		case strings.Contains(q, "system.asynchronous_metrics"):
			data = `[["metric","Query",3],["metric","ReadonlyReplica",1],["event","FailedQuery",7],
["event","QueryTimeMicroseconds",5000000],["async","MaxPartCountForPartition",280],
["async","ReplicasMaxAbsoluteDelay",null],["setting","parts_to_throw_insert",3000],["setting","parts_to_delay_insert",1000]]`
		case strings.Contains(q, "system.mutations"):
			data = `[[4, 2, 1, 12.5]]`
		case strings.Contains(q, "system.parts"):
			data = `[["app","events",1048576,1000,12]]`
		default:
			t.Errorf("unexpected query: %s", q)
		}
		_, _ = w.Write([]byte(`{"meta":[],"data":` + data + `,"rows":1}`))
	}))
}

func TestCollect(t *testing.T) {
	srv := server(t, "")
	defer srv.Close()
	c := New(srv.URL, "coroot", "secret", srv.Client(), time.Second, logger.NewKlog("test"))
	got := collect(t, c)
	want := map[string]float64{
		"clickhouse_up":                                           1,
		"clickhouse_info,version=25.3.6.56":                       1,
		"clickhouse_uptime_seconds":                               3600,
		"clickhouse_metric,metric=Query":                          3,
		"clickhouse_metric,metric=ReadonlyReplica":                1,
		"clickhouse_events_total,event=FailedQuery":               7,
		"clickhouse_events_total,event=QueryTimeMicroseconds":     5000000,
		"clickhouse_async_metric,metric=MaxPartCountForPartition": 280,
		"clickhouse_async_metric,metric=ReplicasMaxAbsoluteDelay": 0,
		"clickhouse_parts_to_throw_insert":                        3000,
		"clickhouse_parts_to_delay_insert":                        1000,
		"clickhouse_replicated_tables":                            4,
		"clickhouse_mutations_unfinished":                         2,
		"clickhouse_mutations_failing":                            1,
		"clickhouse_longest_query_seconds":                        12.5,
		"clickhouse_table_size_bytes,db=app,table=events":         1048576,
		"clickhouse_table_rows,db=app,table=events":               1000,
		"clickhouse_table_parts,db=app,table=events":              12,
	}
	for k, v := range want {
		if g, ok := got[k]; !ok || g != v {
			t.Errorf("%s = %v (present: %v), want %v", k, g, ok, v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d samples, want %d: %v", len(got), len(want), got)
	}
}

func TestCollectPartialFailure(t *testing.T) {
	srv := server(t, "system.mutations")
	defer srv.Close()
	c := New(srv.URL, "coroot", "secret", srv.Client(), time.Second, logger.NewKlog("test"))
	got := collect(t, c)
	if got["clickhouse_up"] != 1 {
		t.Errorf("clickhouse_up = %v, want 1", got["clickhouse_up"])
	}
	if _, ok := got["clickhouse_mutations_unfinished"]; ok {
		t.Error("mutations reported although the query failed")
	}
	if _, ok := got["clickhouse_table_parts,db=app,table=events"]; !ok {
		t.Error("the queries after a failed one must still run")
	}
	found := false
	for k := range got {
		if strings.HasPrefix(k, "clickhouse_scrape_error,error=") && strings.Contains(k, "Not enough privileges") {
			found = true
		}
	}
	if !found {
		t.Errorf("no scrape error with the server's message: %v", got)
	}
}

func TestCollectLoginFailed(t *testing.T) {
	srv := server(t, "")
	defer srv.Close()
	c := New(srv.URL, "coroot", "wrong", srv.Client(), time.Second, logger.NewKlog("test"))
	got := collect(t, c)
	if got["clickhouse_up"] != 0 || len(got) != 2 {
		t.Errorf("want up=0 + one scrape error, got %v", got)
	}
}
