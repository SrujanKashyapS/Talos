package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/SrujanKashyapS/Talos/internal/raftstore"
	"github.com/SrujanKashyapS/Talos/internal/rpc"
	"github.com/SrujanKashyapS/Talos/internal/scheduler"
	"github.com/SrujanKashyapS/Talos/internal/spec"
	"sort"
	"strings"
	"time"
)

type Server struct{ Node *raftstore.Node }
type assignment struct {
	NodeID      string
	ContainerID string
}

func (s *Server) leaderGuard() error {
	if !s.Node.IsLeader() {
		return fmt.Errorf("not leader: %s", s.Node.Leader())
	}
	return nil
}
func (s *Server) RegisterNode(ctx context.Context, r *rpc.NodeRequest) (*rpc.LeaderReply, error) {
	if e := s.leaderGuard(); e != nil {
		return nil, e
	}
	n := r.Node
	n.Alive = true
	n.LastHeartbeat = time.Now().UTC().UnixNano()
	b, err := json.Marshal(n)
	if err != nil {
		return nil, fmt.Errorf("marshal node: %w", err)
	}
	_, e := s.Node.Apply(raftstore.Command{Op: raftstore.OpNode, Key: n.ID, Value: b}, 3*time.Second)
	return &rpc.LeaderReply{Address: s.Node.Leader()}, e
}
func (s *Server) Heartbeat(ctx context.Context, r *rpc.NodeRequest) (*rpc.LeaderReply, error) {
	return s.RegisterNode(ctx, r)
}
func (s *Server) ListNodes(ctx context.Context, _ *rpc.Empty) (*rpc.NodeList, error) {
	st := s.Node.FSM.State()
	o := &rpc.NodeList{}
	for _, n := range st.Nodes {
		o.Nodes = append(o.Nodes, n)
	}
	sort.Slice(o.Nodes, func(i, j int) bool { return o.Nodes[i].ID < o.Nodes[j].ID })
	return o, nil
}
func (s *Server) JoinRaft(_ context.Context, r *rpc.JoinRaftRequest) (*rpc.Empty, error) {
	if e := s.leaderGuard(); e != nil {
		return nil, e
	}
	if e := s.Node.Join(r.ID, r.Address); e != nil {
		return nil, e
	}
	return &rpc.Empty{}, nil
}
func (s *Server) Leader(context.Context, *rpc.Empty) (*rpc.RaftStatus, error) {
	return &rpc.RaftStatus{Leader: s.Node.Leader(), State: s.Node.Raft.State().String()}, nil
}
func (s *Server) ApplyWorkload(ctx context.Context, r *rpc.ApplyWorkloadRequest) (*rpc.Empty, error) {
	if e := s.leaderGuard(); e != nil {
		return nil, e
	}
	w, e := spec.Parse(r.Spec)
	if e != nil {
		return nil, e
	}
	b, err := json.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("marshal workload: %w", err)
	}
	_, e = s.Node.Apply(raftstore.Command{Op: raftstore.OpWorkload, Key: w.Name, Value: b}, 5*time.Second)
	return &rpc.Empty{}, e
}
func (s *Server) ScaleWorkload(ctx context.Context, r *rpc.ScaleRequest) (*rpc.Empty, error) {
	if e := s.leaderGuard(); e != nil {
		return nil, e
	}
	st := s.Node.FSM.State()
	raw, ok := st.Workloads[r.Name]
	if !ok {
		return nil, fmt.Errorf("workload %s not found", r.Name)
	}
	var w spec.Spec
	if e := json.Unmarshal(raw, &w); e != nil {
		return nil, e
	}
	if r.Replicas < 0 {
		return nil, fmt.Errorf("replicas cannot be negative")
	}
	if int(r.Replicas) < w.Replicas {
		for i := int(r.Replicas); i < w.Replicas; i++ {
			if err := s.stopAssignment(ctx, st, fmt.Sprintf("%s/%d", w.Name, i)); err != nil {
				return nil, err
			}
		}
	}
	w.Replicas = int(r.Replicas)
	b, err := json.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("marshal workload: %w", err)
	}
	_, e := s.Node.Apply(raftstore.Command{Op: raftstore.OpWorkload, Key: w.Name, Value: b}, 5*time.Second)
	return &rpc.Empty{}, e
}
func (s *Server) DeleteWorkload(ctx context.Context, r *rpc.DeleteWorkloadRequest) (*rpc.Empty, error) {
	if e := s.leaderGuard(); e != nil {
		return nil, e
	}
	st := s.Node.FSM.State()
	if _, ok := st.Workloads[r.Name]; !ok {
		return nil, fmt.Errorf("workload %s not found", r.Name)
	}
	for key := range st.Assignments {
		if strings.HasPrefix(key, r.Name+"/") {
			if err := s.stopAssignment(ctx, st, key); err != nil {
				return nil, err
			}
		}
	}
	_, e := s.Node.Apply(raftstore.Command{Op: raftstore.OpDeleteWorkload, Key: r.Name}, 5*time.Second)
	return &rpc.Empty{}, e
}

func (s *Server) stopAssignment(ctx context.Context, st raftstore.State, key string) error {
	raw, ok := st.Assignments[key]
	if !ok || raw == "" {
		return nil
	}
	var a assignment
	if err := json.Unmarshal([]byte(raw), &a); err != nil {
		return fmt.Errorf("decode assignment %s: %w", key, err)
	}
	if n, ok := st.Nodes[a.NodeID]; ok && n.Alive {
		cc, err := rpc.Dial(ctx, n.Address)
		if err != nil {
			return fmt.Errorf("dial node %s: %w", a.NodeID, err)
		}
		defer cc.Close()
		if _, err := rpc.NewAgentClient(cc).StopContainer(ctx, &rpc.RunContainerRequest{ID: a.ContainerID}); err != nil {
			return fmt.Errorf("stop container %s: %w", a.ContainerID, err)
		}
	}
	_, err := s.Node.Apply(raftstore.Command{Op: raftstore.OpDeleteAssignment, Key: key}, 5*time.Second)
	return err
}
func (s *Server) ListWorkloads(context.Context, *rpc.Empty) (*rpc.WorkloadList, error) {
	st := s.Node.FSM.State()
	o := &rpc.WorkloadList{}
	workloadNames := make([]string, 0, len(st.Workloads))
	for name := range st.Workloads {
		workloadNames = append(workloadNames, name)
	}
	sort.Strings(workloadNames)
	for _, name := range workloadNames {
		raw := st.Workloads[name]
		var w spec.Spec
		if e := json.Unmarshal(raw, &w); e != nil {
			continue
		}
		running := int32(0)
		prefix := w.Name + "/"
		for k, v := range st.Assignments {
			if strings.HasPrefix(k, prefix) && v != "" {
				running++
			}
		}
		o.Items = append(o.Items, rpc.WorkloadStatus{Name: w.Name, Image: w.Image, Desired: int32(w.Replicas), Running: running})
	}
	sort.Slice(o.Items, func(i, j int) bool { return o.Items[i].Name < o.Items[j].Name })
	return o, nil
}
func (s *Server) ListControlNodes(ctx context.Context, _ *rpc.Empty) (*rpc.NodeList, error) {
	return s.ListNodes(ctx, &rpc.Empty{})
}
func (s *Server) GetWorkloadLogs(ctx context.Context, r *rpc.LogsRequest) (*rpc.LogsReply, error) {
	st := s.Node.FSM.State()
	k := fmt.Sprintf("%s/%d", r.Name, r.Replica)
	raw, ok := st.Assignments[k]
	if !ok {
		return nil, fmt.Errorf("replica %s not assigned", k)
	}
	var a assignment
	if e := json.Unmarshal([]byte(raw), &a); e != nil {
		return nil, e
	}
	n, ok := st.Nodes[a.NodeID]
	if !ok {
		return nil, fmt.Errorf("node %s not found", a.NodeID)
	}
	cc, e := rpc.Dial(ctx, n.Address)
	if e != nil {
		return nil, e
	}
	defer cc.Close()
	c := rpc.NewAgentClient(cc)
	x, e := c.GetLogs(ctx, &rpc.RunContainerRequest{ID: a.ContainerID})
	if e != nil {
		return nil, e
	}
	return &rpc.LogsReply{Data: x.Data}, nil
}

type Controller struct {
	Node                   *raftstore.Node
	Schedule               scheduler.Scheduler
	Interval               time.Duration
	ObserveScheduleLatency func(time.Duration)
}

func (c *Controller) Run(ctx context.Context) {
	d := c.Interval
	if d <= 0 {
		d = time.Second
	}
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if c.Node.IsLeader() {
				_ = c.Reconcile(ctx)
			}
		}
	}
}
func (c *Controller) Reconcile(ctx context.Context) error {
	if !c.Node.IsLeader() {
		return fmt.Errorf("not leader: %s", c.Node.Leader())
	}
	st := c.Node.FSM.State()
	nodes := make([]scheduler.NodeView, 0, len(st.Nodes))
	for _, n := range st.Nodes {
		nodes = append(nodes, scheduler.NodeView{Node: n})
	}
	workloadNames := make([]string, 0, len(st.Workloads))
	for name := range st.Workloads {
		workloadNames = append(workloadNames, name)
	}
	sort.Strings(workloadNames)
	for _, name := range workloadNames {
		raw := st.Workloads[name]
		var w spec.Spec
		if e := json.Unmarshal(raw, &w); e != nil {
			continue
		}
		reqCPU, _, e := spec.CPU(w.Resources.Request.CPU)
		if e != nil {
			return e
		}
		reqMem, e := spec.Memory(w.Resources.Request.Memory)
		if e != nil {
			return e
		}
		limitsCPU, cpuMax, e := spec.CPU(w.Resources.Limit.CPU)
		if e != nil {
			return e
		}
		_ = limitsCPU
		for i := 0; i < w.Replicas; i++ {
			k := fmt.Sprintf("%s/%d", w.Name, i)
			as := assignment{}
			rawAs := st.Assignments[k]
			if rawAs != "" {
				if err := json.Unmarshal([]byte(rawAs), &as); err != nil {
					return fmt.Errorf("decode assignment %s: %w", k, err)
				}
			}
			if !needsPlacement(st, as) {
				continue
			}
			start := time.Now()
			node, e := c.Schedule.Pick(nodes, scheduler.Request{CPU: reqCPU, Memory: reqMem})
			if e != nil {
				continue
			}
			var ni rpc.NodeInfo
			for _, n := range nodes {
				if n.Node.ID == node {
					ni = n.Node
				}
			}
			if !ni.Alive {
				continue
			}
			cc, e := rpc.Dial(ctx, ni.Address)
			if e != nil {
				continue
			}
			cl := rpc.NewAgentClient(cc)
			an, tag := split(w.Image)
			resp, e := cl.RunContainer(ctx, &rpc.RunContainerRequest{ID: containerID(w.Name, i), ImageName: an, ImageTag: tag, Command: nil, Env: w.Env, Workload: w.Name, Replica: int32(i), Resources: rpc.ResourceSpec{CPU: cpuMax, Memory: reqMem}, Chroot: false})
			if e != nil {
				cc.Close()
				continue
			}
			if c.ObserveScheduleLatency != nil {
				c.ObserveScheduleLatency(time.Since(start))
			}
			for j := range nodes {
				if nodes[j].Node.ID == node {
					nodes[j].Node.CPUUsed += reqCPU
					nodes[j].Node.MemoryUsed += reqMem
					nodes[j].Node.Containers++
					break
				}
			}
			as = assignment{NodeID: node, ContainerID: resp.ID}
			b, err := json.Marshal(as)
			if err != nil {
				if _, stopErr := cl.StopContainer(ctx, &rpc.RunContainerRequest{ID: resp.ID}); stopErr != nil {
					cc.Close()
					return fmt.Errorf("marshal assignment %s: %w (cleanup container %s: %v)", k, err, resp.ID, stopErr)
				}
				cc.Close()
				return fmt.Errorf("marshal assignment %s: %w", k, err)
			}
			_, e = c.Node.Apply(raftstore.Command{Op: raftstore.OpAssignment, Key: k, Value: b}, 5*time.Second)
			if e != nil {
				// A failed Apply may be returned after a leadership transition. Preserve
				// a committed assignment; otherwise stop the just-started container so a
				// later reconciliation does not create a duplicate replica.
				if c.Node.FSM.State().Assignments[k] != string(b) {
					if _, stopErr := cl.StopContainer(ctx, &rpc.RunContainerRequest{ID: resp.ID}); stopErr != nil {
						cc.Close()
						return fmt.Errorf("apply assignment %s: %w (cleanup container %s: %v)", k, e, resp.ID, stopErr)
					}
				}
				cc.Close()
				return fmt.Errorf("apply assignment %s: %w", k, e)
			}
			cc.Close()
		}
	}
	return nil
}

func needsPlacement(st raftstore.State, as assignment) bool {
	if as.ContainerID == "" {
		return true
	}
	n, ok := st.Nodes[as.NodeID]
	return !ok || !n.Alive
}

func containerID(workload string, replica int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", workload, replica)))
	return fmt.Sprintf("talos-%x", sum[:12])
}
func split(ref string) (string, string) {
	i := strings.LastIndex(ref, "/")
	l := ref
	if i >= 0 {
		l = ref[i+1:]
	}
	j := strings.LastIndex(l, ":")
	if j >= 0 {
		return ref[:i+1] + l[:j], l[j+1:]
	}
	return ref, "latest"
}
