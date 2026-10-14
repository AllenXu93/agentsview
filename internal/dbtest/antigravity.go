package dbtest

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/testjsonl"
)

const AntigravityTitleID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

func WriteAntigravityTitleTranscript(t *testing.T, root, content string) string {
	t.Helper()
	return testjsonl.WriteAntigravityBrainTranscript(t, root, AntigravityTitleID, `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"`+content+`"}`+"\n")
}

func SetAntigravityTitle(t *testing.T, root string, title any) string {
	t.Helper()
	path := filepath.Join(root, parser.AntigravitySummariesDBName)
	conn, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(t.Context(), testjsonl.AntigravityTitleSchema)
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), `INSERT INTO conversation_summaries VALUES (?, ?) ON CONFLICT(conversation_id) DO UPDATE SET title = excluded.title`, AntigravityTitleID, title)
	require.NoError(t, err)
	return path
}
