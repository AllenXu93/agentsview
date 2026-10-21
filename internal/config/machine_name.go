package config

import (
	"errors"
	"os"
	"strings"
)

// applyMachineNameResolution sets LocalMachineName from the config file,
// AGENTSVIEW_MACHINE, or the hostname already stored by Default.
func (c *Config) applyMachineNameResolution() {
	name, err := ResolveMachineName(
		c.fileMachineName,
		os.Getenv("AGENTSVIEW_MACHINE"),
		c.LocalMachineName,
	)
	if err != nil {
		return
	}
	c.LocalMachineName = name
}

// ResolveMachineName chooses the display name recorded on sessions from this
// installation.
//
// Priority is the config value, then AGENTSVIEW_MACHINE, then the operating
// system hostname. configured is machine_name when that key is set, and
// otherwise local_machine_name. An empty configured or env value is skipped.
// The hostname must be non-empty when it is the only remaining source.
func ResolveMachineName(configured, envValue, hostname string) (string, error) {
	if name := strings.TrimSpace(configured); name != "" {
		return name, nil
	}
	if name := strings.TrimSpace(envValue); name != "" {
		return name, nil
	}
	name := strings.TrimSpace(hostname)
	if name == "" {
		return "", errors.New("machine name is empty")
	}
	return name, nil
}
