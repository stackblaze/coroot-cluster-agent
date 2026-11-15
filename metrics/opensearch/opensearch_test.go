package opensearch

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const nodeStatsJSON = `{"nodes":{"n1":{"name":"os-nodes-0","roles":["ingest","data","cluster_manager"],
"indices":{"docs":{"count":42},"store":{"size_in_bytes":1000},
"indexing":{"index_total":10,"index_time_in_millis":2500,"index_failed":1},
"search":{"query_total":7,"query_time_in_millis":700,"fetch_total":3,"fetch_time_in_millis":30},
"segments":{"count":5}},
"jvm":{"mem":{"heap_used_in_bytes":300,"heap_max_in_bytes":512},
"gc":{"collectors":{"young":{"collection_count":4,"collection_time_in_millis":40}}}},
"thread_pool":{"write":{"queue":2,"active":1,"rejected":9},"search":{"queue":0,"active":0,"rejected":0},
"flush":{"queue":0,"active":0,"rejected":0},"snapshot":{"queue":0,"active":0,"rejected":3}},
"breakers":{"parent":{"limit_size_in_bytes":486,"estimated_size_in_bytes":300,"tripped":2}},
"fs":{"total":{"total_in_bytes":8000,"available_in_bytes":6000}},
"http":{"current_open":11}}}}`

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
		key := m.Desc().String()
		// Desc{fqName: "name", ...}
		start := len(`Desc{fqName: "`)
		end := start
		for key[end] != '"' {
			end++
		}
		key = key[start:end]
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

func server(t *testing.T, stateKey string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if u, p, ok := r.BasicAuth(); !ok || u != "coroot" || p != "secret" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			h(w, r)
		}
	}
	mux.HandleFunc("/", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"version":{"number":"2.19.1","distribution":"opensearch"}}`))
	}))
	mux.HandleFunc("/_cluster/health", auth(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"cluster_name":"os","status":"yellow","number_of_nodes":3,"number_of_data_nodes":3,
"active_primary_shards":5,"active_shards":9,"relocating_shards":0,"initializing_shards":1,"unassigned_shards":2,
"number_of_pending_tasks":4}`))
	}))
	mux.HandleFunc("/_nodes/_local/stats/", auth(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(nodeStatsJSON))
	}))
	mux.HandleFunc("/_cluster/state/"+stateKey, auth(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"cluster_name":"os","` + stateKey + `":"n1"}`))
	}))
	return httptest.NewServer(mux)
}

func TestCollect(t *testing.T) {
	for _, stateKey := range []string{"cluster_manager_node", "master_node"} {
		srv := server(t, stateKey)
		c := New(srv.URL, "coroot", "secret", srv.Client(), time.Second, logger.NewKlog("test"))
		got := collect(t, c)
		srv.Close()

		want := map[string]float64{
			"opensearch_up": 1,
			"opensearch_info,cluster=os,distribution=opensearch,node=os-nodes-0,roles=cluster_manager,data,ingest,version=2.19.1": 1,
			"opensearch_cluster_status,status=yellow":           1,
			"opensearch_cluster_nodes":                          3,
			"opensearch_cluster_shards,state=unassigned":        2,
			"opensearch_cluster_shards,state=initializing":      1,
			"opensearch_cluster_pending_tasks":                  4,
			"opensearch_cluster_manager":                        1,
			"opensearch_indices_docs":                           42,
			"opensearch_indexing_seconds_total":                 2.5,
			"opensearch_indexing_failed_total":                  1,
			"opensearch_search_ops_total,phase=query":           7,
			"opensearch_search_seconds_total,phase=fetch":       0.03,
			"opensearch_jvm_heap_used_bytes":                    300,
			"opensearch_jvm_heap_max_bytes":                     512,
			"opensearch_jvm_gc_seconds_total,gc=young":          0.04,
			"opensearch_thread_pool_rejected_total,pool=write":  9,
			"opensearch_thread_pool_queue,pool=write":           2,
			"opensearch_thread_pool_rejected_total,pool=search": 0,
			// Not a main pool, but it rejected requests.
			"opensearch_thread_pool_rejected_total,pool=snapshot": 3,
			"opensearch_breaker_tripped_total,breaker=parent":     2,
			"opensearch_fs_available_bytes":                       6000,
			"opensearch_http_connections_open":                    11,
		}
		for k, v := range want {
			if g, ok := got[k]; !ok || g != v {
				t.Errorf("%s: %s = %v (present: %v), want %v", stateKey, k, g, ok, v)
			}
		}
		if _, ok := got["opensearch_thread_pool_rejected_total,pool=flush"]; ok {
			t.Errorf("%s: idle non-main pool reported", stateKey)
		}
		for k := range got {
			if len(k) > 23 && k[:23] == "opensearch_scrape_error" {
				t.Errorf("%s: unexpected %s", stateKey, k)
			}
		}
	}
}

func TestCollectLoginFailed(t *testing.T) {
	srv := server(t, "cluster_manager_node")
	defer srv.Close()
	c := New(srv.URL, "coroot", "wrong", srv.Client(), time.Second, logger.NewKlog("test"))
	got := collect(t, c)
	if got["opensearch_up"] != 0 {
		t.Errorf("opensearch_up = %v, want 0", got["opensearch_up"])
	}
	if len(got) != 2 {
		t.Errorf("want only up + scrape_error, got %v", got)
	}
}
