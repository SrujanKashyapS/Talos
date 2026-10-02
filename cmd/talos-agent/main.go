package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/SrujanKashyapS/Talos/internal/agent"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"google.golang.org/grpc"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var addr, control, id string
	var heartbeat time.Duration
	var chroot bool
	flag.StringVar(&addr, "listen", ":5001", "agent address")
	flag.StringVar(&control, "control", "127.0.0.1:5000", "control address")
	flag.StringVar(&id, "node-id", "", "node id")
	flag.DurationVar(&heartbeat, "heartbeat", 2*time.Second, "heartbeat interval")
	flag.BoolVar(&chroot, "chroot", false, "enable chroot")
	flag.Parse()
	if id == "" {
		h, _ := os.Hostname()
		id = h + addr
	}
	m, e := agent.New()
	if e != nil {
		panic(e)
	}
	lis, e := net.Listen("tcp", addr)
	if e != nil {
		panic(e)
	}
	gs := rpc.Server()
	rpc.RegisterAgentServer(gs, &agent.IsolatedRPCServer{RPCServer: &agent.RPCServer{Manager: m}, Chroot: chroot})
	cc, e := rpc.Dial(context.Background(), control)
	if e != nil {
		panic(e)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go agent.RunMembership(ctx, cc, id, addr, heartbeat, m)
	go func() { <-ctx.Done(); gs.GracefulStop(); _ = lis.Close(); _ = cc.Close() }()
	slog.Info("talos-agent listening", "addr", addr, "node", id)
	if e := gs.Serve(lis); e != nil && e != grpc.ErrServerStopped {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
