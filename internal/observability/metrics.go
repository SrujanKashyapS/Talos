package observability

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	nodesAlive        prometheus.Gauge
	containers        prometheus.Gauge
	schedulingLatency prometheus.Histogram
	cacheLookups      prometheus.Counter
	cacheHits         prometheus.Counter
	raftState         *prometheus.GaugeVec
}

var registerOnce sync.Once

func New() *Metrics {
	m := &Metrics{
		nodesAlive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "talos_nodes_alive",
			Help: "Number of alive nodes in cluster state",
		}),
		containers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "talos_containers",
			Help: "Number of assigned containers",
		}),
		schedulingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "talos_scheduling_latency_seconds",
			Help:    "Scheduling + launch latency per replica",
			Buckets: prometheus.DefBuckets,
		}),
		cacheLookups: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "talos_cache_lookups_total",
			Help: "Total shared cache lookups",
		}),
		cacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "talos_cache_hits_total",
			Help: "Total shared cache hits",
		}),
		raftState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "talos_raft_state",
			Help: "Raft state one-hot gauge by state label",
		}, []string{"state"}),
	}
	registerOnce.Do(func() {
		prometheus.MustRegister(
			m.nodesAlive,
			m.containers,
			m.schedulingLatency,
			m.cacheLookups,
			m.cacheHits,
			m.raftState,
		)
	})
	return m
}

func (m *Metrics) SetCluster(nodesAlive, containers int) {
	m.nodesAlive.Set(float64(nodesAlive))
	m.containers.Set(float64(containers))
}

func (m *Metrics) SetRaftState(state string) {
	for _, s := range []string{"Follower", "Candidate", "Leader", "Shutdown"} {
		v := 0.0
		if s == state {
			v = 1
		}
		m.raftState.WithLabelValues(s).Set(v)
	}
}

func (m *Metrics) ObserveSchedulingLatency(d time.Duration) {
	m.schedulingLatency.Observe(d.Seconds())
}

func (m *Metrics) RecordCacheLookup(hit bool) {
	m.cacheLookups.Inc()
	if hit {
		m.cacheHits.Inc()
	}
}
