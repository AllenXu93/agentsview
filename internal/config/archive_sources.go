package config

import (
	"fmt"
	"strings"
)

// ArchiveSource selects retained history independently of its mount path.
type ArchiveSource struct {
	Name         string            `toml:"name"`
	Repository   string            `toml:"repository"`
	Snapshot     string            `toml:"snapshot"`
	Directory    string            `toml:"directory"`
	MachineNames map[string]string `toml:"machine_names"`
}

func (c Config) ValidateArchiveSources() error {
	seen := make(map[string]bool, len(c.ArchiveSources))
	labels := make(map[string]string)
	for i, source := range c.ArchiveSources {
		fields := []struct{ name, value string }{{"name", source.Name}}
		if source.Directory != "" {
			if source.Repository != "" || source.Snapshot != "" {
				return fmt.Errorf("archive source %d requires either directory or repository with snapshot", i+1)
			}
			fields = append(fields, struct{ name, value string }{"directory", source.Directory})
		} else {
			fields = append(fields, struct{ name, value string }{"repository", source.Repository}, struct{ name, value string }{"snapshot", source.Snapshot})
		}
		for _, field := range fields {
			if strings.TrimSpace(field.value) == "" || strings.ContainsRune(field.value, '\x00') {
				return fmt.Errorf("archive source %d requires a non-empty %s", i+1, field.name)
			}
		}
		if seen[source.Name] {
			return fmt.Errorf("duplicate archive source name %q", source.Name)
		}
		seen[source.Name] = true
		for id, label := range source.MachineNames {
			if ValidateInstallationID(id) != nil || strings.TrimSpace(label) == "" || strings.ContainsRune(label, '\x00') || len(label) > 4096 {
				return fmt.Errorf("archive source %q has an invalid machine name", source.Name)
			}
			if previous, ok := labels[id]; ok && previous != label {
				return fmt.Errorf("archive sources configure different names for machine %s", id)
			}
			labels[id] = label
		}
	}
	return nil
}

// Path returns the configured storage location.
func (s ArchiveSource) Path() string {
	if s.Directory != "" {
		return s.Directory
	}
	return s.Repository
}
