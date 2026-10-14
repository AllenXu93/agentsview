package testjsonl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const AntigravityTitleSchema = `CREATE TABLE IF NOT EXISTS conversation_summaries (conversation_id TEXT PRIMARY KEY, title TEXT)`

func WriteAntigravityBrainTranscript(t *testing.T, root, id, body string) string {
	t.Helper()
	path := filepath.Join(root, "brain", id, ".system_generated", "logs", "transcript.jsonl")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}
