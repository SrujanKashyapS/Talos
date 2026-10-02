package agent

import (
	"github.com/SrujanKashyapS/Talos/image"
	"sort"
	"strings"
)

func (m *Manager) HeldLayers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := map[string]bool{}
	for _, r := range m.items {
		if r.Status != "running" {
			continue
		}
		i := strings.LastIndex(r.Image, ":")
		if i < 0 {
			continue
		}
		man, e := image.Load(r.Image[:i], r.Image[i+1:])
		if e != nil {
			continue
		}
		for _, l := range man.Layers {
			set[l.Digest] = true
		}
	}
	o := make([]string, 0, len(set))
	for d := range set {
		o = append(o, d)
	}
	sort.Strings(o)
	return o
}
