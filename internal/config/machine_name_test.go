package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveMachineNamePriority(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		envValue   string
		hostname   string
		want       string
		wantErr    string
	}{
		{
			name:       "config beats env and hostname",
			configured: " laptop ",
			envValue:   "from-env",
			hostname:   "from-host",
			want:       "laptop",
		},
		{
			name:     "env beats hostname",
			envValue: " from-env ",
			hostname: "from-host",
			want:     "from-env",
		},
		{
			name:     "hostname is the fallback",
			hostname: " from-host ",
			want:     "from-host",
		},
		{
			name:       "blank config and env fall through to hostname",
			configured: " ",
			envValue:   "\t",
			hostname:   "from-host",
			want:       "from-host",
		},
		{
			name:    "empty hostname is an error",
			wantErr: "machine name is empty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveMachineName(tt.configured, tt.envValue, tt.hostname)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
