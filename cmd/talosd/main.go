package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/SrujanKashyapS/Talos/internal/cache"
	"github.com/SrujanKashyapS/Talos/internal/control"
	"github.com/SrujanKashyapS/Talos/internal/observability"
	"github.com/SrujanKashyapS/Talos/internal/raftstore"
	"github.com/SrujanKashyapS/Talos/internal/registry"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"github.com/SrujanKashyapS/Talos/internal/scheduler"
	"github.com/SrujanKashyapS/Talos/utils"
)

func main() {
	var id, addr, raftAddr, stateDir, peers, join, metricsAddr string
	var bootstrap bool
	flag.StringVar(&id, "id", "node1", "raft node id")
	flag.StringVar(&addr, "listen", ":5000", "gRPC address")
	flag.StringVar(&raftAddr, "raft-listen", ":7000", "raft address")
	flag.StringVar(&stateDir, "state", "", "raft state directory")
	flag.StringVar(&peers, "peers", "", "id=addr,id=addr")
	flag.StringVar(&join, "join", "", "leader address")
	flag.StringVar(&metricsAddr, "metrics-listen", ":9090", "metrics address")
	flag.BoolVar(&bootstrap, "bootstrap", false, "bootstrap cluster")
	flag.Parse()

	if stateDir == "" {
		r, _ := utils.DocksmithRoot()
		stateDir = r + "/raft/" + id
	}
	var ps []raftstore.Peer
	for _, p := range strings.Split(peers, ",") {
		if p == "" {
			continue
		}
		x := strings.SplitN(p, "=", 2)
		if len(x) == 2 {
			ps = append(ps, raftstore.Peer{ID: x[0], Address: x[1]})
		}
	}
	rn, e := raftstore.Open(raftstore.Options{ID: id, Bind: raftAddr, StateDir: stateDir, Bootstrap: bootstrap, Peers: ps})
	if e != nil {
		panic(e)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	metrics := observability.New()
	srv := control.Server{Node: rn}
	gs := rpc.Server()
	rpc.RegisterRegistryServer(gs, &registry.RPCServer{})
	rpc.RegisterCacheServer(gs, &cache.Server{Node: rn, OnLookup: metrics.RecordCacheLookup})
	rpc.RegisterMembershipServer(gs, &srv)
	rpc.RegisterRaftServer(gs, &srv)
	rpc.RegisterControlServer(gs, &srv)

	go (&control.Controller{
		Node:                   rn,
		Schedule:               scheduler.LeastLoaded{},
		Interval:               time.Second,
		ObserveScheduleLatency: metrics.ObserveSchedulingLatency,
	}).Run(ctx)
	go publishClusterMetrics(ctx, rn, metrics)

	metricsSrv := &http.Server{Addr: metricsAddr, Handler: promhttp.Handler()}
	go func() {
		if e := metricsSrv.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			slog.Error("metrics server error", "error", e)
		}
	}()

	lis, e := net.Listen("tcp", addr)
	if e != nil {
		panic(e)
	}
	if join != "" && !bootstrap {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				cc, e := rpc.Dial(ctx, join)
				if e == nil {
					c := rpc.NewRaftClient(cc)
					_, e = c.JoinRaft(ctx, &rpc.JoinRaftRequest{ID: id, Address: raftAddr})
					cc.Close()
					if e == nil {
						return
					}
				}
				time.Sleep(time.Second)
			}
		}()
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shutdownCtx)
		gs.GracefulStop()
		_ = lis.Close()
		_ = rn.Shutdown()
	}()
	slog.Info("talosd listening", "addr", addr, "raft", raftAddr, "id", id, "metrics", metricsAddr)
	if e := gs.Serve(lis); e != nil && e != grpc.ErrServerStopped {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}

func publishClusterMetrics(ctx context.Context, rn *raftstore.Node, m *observability.Metrics) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			st := rn.FSM.State()
			alive := 0
			for _, n := range st.Nodes {
				if n.Alive {
					alive++
				}
			}
			containers := 0
			for _, v := range st.Assignments {
				if strings.TrimSpace(v) != "" {
					containers++
				}
			}
			m.SetCluster(alive, containers)
			m.SetRaftState(rn.Raft.State().String())
		}
	}
}
