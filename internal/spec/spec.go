package spec

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"strconv"
	"strings"
)

type Spec struct {
	Name      string    `yaml:"name"`
	Image     string    `yaml:"image"`
	Replicas  int       `yaml:"replicas"`
	Env       []string  `yaml:"env"`
	Resources Resources `yaml:"resources"`
}
type Resources struct {
	Request Resource `yaml:"request"`
	Limit   Resource `yaml:"limit"`
}
type Resource struct {
	CPU    string `yaml:"cpu"`
	Memory string `yaml:"memory"`
}

func Parse(b []byte) (Spec, error) {
	var s Spec
	if e := yaml.Unmarshal(b, &s); e != nil {
		return s, fmt.Errorf("parse yaml: %w", e)
	}
	if e := Validate(s); e != nil {
		return s, e
	}
	return s, nil
}
func Validate(s Spec) error {
	if s.Name == "" || s.Image == "" {
		return fmt.Errorf("name and image are required")
	}
	if s.Replicas < 0 {
		return fmt.Errorf("replicas cannot be negative")
	}
	if _, _, e := CPU(s.Resources.Request.CPU); e != nil {
		return fmt.Errorf("request cpu: %w", e)
	}
	if _, _, e := CPU(s.Resources.Limit.CPU); e != nil {
		return fmt.Errorf("limit cpu: %w", e)
	}
	if _, e := Memory(s.Resources.Request.Memory); e != nil {
		return fmt.Errorf("request memory: %w", e)
	}
	if _, e := Memory(s.Resources.Limit.Memory); e != nil {
		return fmt.Errorf("limit memory: %w", e)
	}
	return nil
}
func CPU(v string) (float64, string, error) {
	if v == "" {
		return 0, "", nil
	}
	v = strings.TrimSpace(v)
	if strings.HasSuffix(v, "m") {
		n, e := strconv.ParseFloat(strings.TrimSuffix(v, "m"), 64)
		if e != nil {
			return 0, "", e
		}
		if n < 0 {
			return 0, "", fmt.Errorf("negative cpu")
		}
		return n / 1000, fmt.Sprintf("%d 100000", int64(n)*100), nil
	}
	n, e := strconv.ParseFloat(v, 64)
	if e != nil {
		return 0, "", e
	}
	if n < 0 {
		return 0, "", fmt.Errorf("negative cpu")
	}
	return n, fmt.Sprintf("%d 100000", int64(n*100000)), nil
}
func Memory(v string) (int64, error) {
	if v == "" {
		return 0, nil
	}
	v = strings.TrimSpace(v)
	units := []struct {
		suffix string
		mul    int64
	}{{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40}, {"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"T", 1 << 40}}
	for _, u := range units {
		if strings.HasSuffix(v, u.suffix) {
			n, e := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(v, u.suffix)), 64)
			if e != nil {
				return 0, e
			}
			if n < 0 {
				return 0, fmt.Errorf("negative memory")
			}
			return int64(n * float64(u.mul)), nil
		}
	}
	n, e := strconv.ParseInt(v, 10, 64)
	return n, e
}
