package config

import (
	"fmt"
	"strings"
)

// ArchiveSource selects immutable history independently of its mount path.
type ArchiveSource struct {
	Name       string `toml:"name"`
	Repository string `toml:"repository"`
	Snapshot   string `toml:"snapshot"`
}

func (c Config) ValidateArchiveSources() error {
	seen := make(map[string]bool, len(c.ArchiveSources))
	for i, source := range c.ArchiveSources {
		for _, field := range []struct{ name, value string }{
			{"name", source.Name}, {"repository", source.Repository}, {"snapshot", source.Snapshot},
		} {
			if strings.TrimSpace(field.value) == "" || strings.ContainsRune(field.value, '\x00') {
				return fmt.Errorf("archive source %d requires a non-empty %s", i+1, field.name)
			}
		}
		if seen[source.Name] {
			return fmt.Errorf("duplicate archive source name %q", source.Name)
		}
		seen[source.Name] = true
	}
	return nil
}
