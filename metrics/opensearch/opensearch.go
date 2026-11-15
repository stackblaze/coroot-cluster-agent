// Package opensearch collects OpenSearch (and Elasticsearch) statistics from a
// node's REST API. Each target is one node: cluster-wide values (health,
// shards) are reported by every node, node values come from `_nodes/_local`.
package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	dUp          = common.Desc("opensearch_up", "")
	dScrapeError = common.Desc("opensearch_scrape_error", "", "error")
	dInfo        = common.Desc("opensearch_info", "", "version", "distribution", "cluster", "node", "roles")

	dClusterStatus       = common.Desc("opensearch_cluster_status", "Cluster health status (1 for the current one)", "status")
	dClusterNodes        = common.Desc("opensearch_cluster_nodes", "Number of nodes in the cluster")
	dClusterDataNodes    = common.Desc("opensearch_cluster_data_nodes", "Number of data nodes in the cluster")
	dClusterShards       = common.Desc("opensearch_cluster_shards", "Number of shards by state", "state")
	dClusterPendingTasks = common.Desc("opensearch_cluster_pending_tasks", "Number of cluster-level changes not yet executed")
	dClusterManager      = common.Desc("opensearch_cluster_manager", "1 if this node is the elected cluster manager")

	dDocs      = common.Desc("opensearch_indices_docs", "Number of documents on this node")
	dStoreSize = common.Desc("opensearch_indices_store_size_bytes", "Size of the shards on this node")
	dSegments  = common.Desc("opensearch_indices_segments", "Number of Lucene segments on this node")

	dIndexingOps    = common.Desc("opensearch_indexing_ops_total", "Total number of indexing operations")
	dIndexingTime   = common.Desc("opensearch_indexing_seconds_total", "Total time spent indexing")
	dIndexingFailed = common.Desc("opensearch_indexing_failed_total", "Total number of failed indexing operations")
	dSearchOps      = common.Desc("opensearch_search_ops_total", "Total number of search operations by phase", "phase")
	dSearchTime     = common.Desc("opensearch_search_seconds_total", "Total time spent searching by phase", "phase")

	dHeapUsed = common.Desc("opensearch_jvm_heap_used_bytes", "JVM heap in use")
	dHeapMax  = common.Desc("opensearch_jvm_heap_max_bytes", "JVM heap limit")
	dGcCount  = common.Desc("opensearch_jvm_gc_collections_total", "Total number of garbage collections", "gc")
	dGcTime   = common.Desc("opensearch_jvm_gc_seconds_total", "Total time spent in garbage collection", "gc")

	dPoolRejected = common.Desc("opensearch_thread_pool_rejected_total", "Total number of requests rejected by the thread pool", "pool")
	dPoolQueue    = common.Desc("opensearch_thread_pool_queue", "Number of requests queued in the thread pool", "pool")
	dPoolActive   = common.Desc("opensearch_thread_pool_active", "Number of active threads in the thread pool", "pool")

	dBreakerTripped   = common.Desc("opensearch_breaker_tripped_total", "Total number of times the circuit breaker tripped", "breaker")
	dBreakerEstimated = common.Desc("opensearch_breaker_estimated_bytes", "Memory the circuit breaker accounts for", "breaker")
	dBreakerLimit     = common.Desc("opensearch_breaker_limit_bytes", "Circuit breaker limit", "breaker")

	dFsTotal     = common.Desc("opensearch_fs_total_bytes", "Size of the data paths' filesystems")
	dFsAvailable = common.Desc("opensearch_fs_available_bytes", "Space available to OpenSearch on the data paths")
	dHttpOpen    = common.Desc("opensearch_http_connections_open", "Number of open HTTP connections")
)

// Thread pools always reported; any other pool is reported once it rejected a request.
var mainPools = map[string]bool{"write": true, "search": true, "get": true}

const maxResponseSize = 16 << 20

type Collector struct {
	baseURL  string
	username string
	password string
	client   *http.Client
	timeout  time.Duration
	logger   logger.Logger
}

// New returns a collector for the node at baseURL (scheme://host:port).
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

type rootInfo struct {
	Version struct {
		Number       string `json:"number"`
		Distribution string `json:"distribution"`
	} `json:"version"`
}

type clusterHealth struct {
	ClusterName         string  `json:"cluster_name"`
	Status              string  `json:"status"`
	NumberOfNodes       float64 `json:"number_of_nodes"`
	NumberOfDataNodes   float64 `json:"number_of_data_nodes"`
	ActivePrimaryShards float64 `json:"active_primary_shards"`
	ActiveShards        float64 `json:"active_shards"`
	RelocatingShards    float64 `json:"relocating_shards"`
	InitializingShards  float64 `json:"initializing_shards"`
	UnassignedShards    float64 `json:"unassigned_shards"`
	PendingTasks        float64 `json:"number_of_pending_tasks"`
}

type nodeStats struct {
	Name    string   `json:"name"`
	Roles   []string `json:"roles"`
	Indices struct {
		Docs struct {
			Count float64 `json:"count"`
		} `json:"docs"`
		Store struct {
			SizeInBytes float64 `json:"size_in_bytes"`
		} `json:"store"`
		Indexing struct {
			IndexTotal        float64 `json:"index_total"`
			IndexTimeInMillis float64 `json:"index_time_in_millis"`
			IndexFailed       float64 `json:"index_failed"`
		} `json:"indexing"`
		Search struct {
			QueryTotal        float64 `json:"query_total"`
			QueryTimeInMillis float64 `json:"query_time_in_millis"`
			FetchTotal        float64 `json:"fetch_total"`
			FetchTimeInMillis float64 `json:"fetch_time_in_millis"`
		} `json:"search"`
		Segments struct {
			Count float64 `json:"count"`
		} `json:"segments"`
	} `json:"indices"`
	JVM struct {
		Mem struct {
			HeapUsedInBytes float64 `json:"heap_used_in_bytes"`
			HeapMaxInBytes  float64 `json:"heap_max_in_bytes"`
		} `json:"mem"`
		GC struct {
			Collectors map[string]struct {
				CollectionCount        float64 `json:"collection_count"`
				CollectionTimeInMillis float64 `json:"collection_time_in_millis"`
			} `json:"collectors"`
		} `json:"gc"`
	} `json:"jvm"`
	ThreadPool map[string]struct {
		Queue    float64 `json:"queue"`
		Active   float64 `json:"active"`
		Rejected float64 `json:"rejected"`
	} `json:"thread_pool"`
	Breakers map[string]struct {
		LimitSizeInBytes     float64 `json:"limit_size_in_bytes"`
		EstimatedSizeInBytes float64 `json:"estimated_size_in_bytes"`
		Tripped              float64 `json:"tripped"`
	} `json:"breakers"`
	FS struct {
		Total struct {
			TotalInBytes     float64 `json:"total_in_bytes"`
			AvailableInBytes float64 `json:"available_in_bytes"`
		} `json:"total"`
	} `json:"fs"`
	HTTP struct {
		CurrentOpen float64 `json:"current_open"`
	} `json:"http"`
}

type nodesStats struct {
	Nodes map[string]nodeStats `json:"nodes"`
}

// The elected cluster manager's node id: `cluster_manager_node` since
// OpenSearch 2.0, `master_node` before it and on Elasticsearch.
type clusterState struct {
	ClusterManagerNode string `json:"cluster_manager_node"`
	MasterNode         string `json:"master_node"`
}

func (c *Collector) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", path, resp.Status)
	}
	return json.Unmarshal(body, out)
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	var errs []string
	fail := func(err error) {
		c.logger.Warning(err)
		errs = append(errs, err.Error())
	}

	var info rootInfo
	if err := c.get(ctx, "/", &info); err != nil {
		// Nothing else can work without the root endpoint (connection or login failed).
		c.logger.Warning(err)
		ch <- common.Gauge(dUp, 0)
		ch <- common.Gauge(dScrapeError, 1, err.Error())
		return
	}
	ch <- common.Gauge(dUp, 1)

	var health clusterHealth
	if err := c.get(ctx, "/_cluster/health?local=true", &health); err != nil {
		fail(err)
	} else {
		ch <- common.Gauge(dClusterStatus, 1, health.Status)
		ch <- common.Gauge(dClusterNodes, health.NumberOfNodes)
		ch <- common.Gauge(dClusterDataNodes, health.NumberOfDataNodes)
		ch <- common.Gauge(dClusterShards, health.ActivePrimaryShards, "active_primary")
		ch <- common.Gauge(dClusterShards, health.ActiveShards, "active")
		ch <- common.Gauge(dClusterShards, health.RelocatingShards, "relocating")
		ch <- common.Gauge(dClusterShards, health.InitializingShards, "initializing")
		ch <- common.Gauge(dClusterShards, health.UnassignedShards, "unassigned")
		ch <- common.Gauge(dClusterPendingTasks, health.PendingTasks)
	}

	var stats nodesStats
	nodeId := ""
	var node nodeStats
	if err := c.get(ctx, "/_nodes/_local/stats/indices,jvm,thread_pool,breaker,fs,http", &stats); err != nil {
		fail(err)
	} else {
		for id, n := range stats.Nodes {
			nodeId, node = id, n
		}
	}

	distribution := info.Version.Distribution
	if distribution == "" {
		distribution = "elasticsearch"
	}
	roles := append([]string(nil), node.Roles...)
	sort.Strings(roles)
	ch <- common.Gauge(dInfo, 1, info.Version.Number, distribution, health.ClusterName, node.Name, strings.Join(roles, ","))

	if nodeId != "" {
		c.collectNode(ch, node)

		var state clusterState
		err := c.get(ctx, "/_cluster/state/cluster_manager_node?local=true", &state)
		if err != nil || (state.ClusterManagerNode == "" && state.MasterNode == "") {
			err = c.get(ctx, "/_cluster/state/master_node?local=true", &state)
		}
		if err != nil {
			fail(err)
		} else {
			elected := 0.
			if nodeId == state.ClusterManagerNode || nodeId == state.MasterNode {
				elected = 1
			}
			ch <- common.Gauge(dClusterManager, elected)
		}
	}

	for _, e := range errs {
		ch <- common.Gauge(dScrapeError, 1, e)
	}
}

func (c *Collector) collectNode(ch chan<- prometheus.Metric, n nodeStats) {
	ch <- common.Gauge(dDocs, n.Indices.Docs.Count)
	ch <- common.Gauge(dStoreSize, n.Indices.Store.SizeInBytes)
	ch <- common.Gauge(dSegments, n.Indices.Segments.Count)

	ch <- common.Counter(dIndexingOps, n.Indices.Indexing.IndexTotal)
	ch <- common.Counter(dIndexingTime, n.Indices.Indexing.IndexTimeInMillis/1000)
	ch <- common.Counter(dIndexingFailed, n.Indices.Indexing.IndexFailed)
	ch <- common.Counter(dSearchOps, n.Indices.Search.QueryTotal, "query")
	ch <- common.Counter(dSearchTime, n.Indices.Search.QueryTimeInMillis/1000, "query")
	ch <- common.Counter(dSearchOps, n.Indices.Search.FetchTotal, "fetch")
	ch <- common.Counter(dSearchTime, n.Indices.Search.FetchTimeInMillis/1000, "fetch")

	ch <- common.Gauge(dHeapUsed, n.JVM.Mem.HeapUsedInBytes)
	ch <- common.Gauge(dHeapMax, n.JVM.Mem.HeapMaxInBytes)
	for name, gc := range n.JVM.GC.Collectors {
		ch <- common.Counter(dGcCount, gc.CollectionCount, name)
		ch <- common.Counter(dGcTime, gc.CollectionTimeInMillis/1000, name)
	}

	for name, p := range n.ThreadPool {
		if !mainPools[name] && p.Rejected == 0 {
			continue
		}
		ch <- common.Counter(dPoolRejected, p.Rejected, name)
		ch <- common.Gauge(dPoolQueue, p.Queue, name)
		ch <- common.Gauge(dPoolActive, p.Active, name)
	}

	for name, b := range n.Breakers {
		ch <- common.Counter(dBreakerTripped, b.Tripped, name)
		ch <- common.Gauge(dBreakerEstimated, b.EstimatedSizeInBytes, name)
		ch <- common.Gauge(dBreakerLimit, b.LimitSizeInBytes, name)
	}

	ch <- common.Gauge(dFsTotal, n.FS.Total.TotalInBytes)
	ch <- common.Gauge(dFsAvailable, n.FS.Total.AvailableInBytes)
	ch <- common.Gauge(dHttpOpen, n.HTTP.CurrentOpen)
}
