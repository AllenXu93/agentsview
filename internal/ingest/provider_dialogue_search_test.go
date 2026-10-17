package ingest_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/ingest"
	"go.kenn.io/agentsview/internal/parser"
)

func TestOpenHandsActionSearchSources(t *testing.T) {
	root := t.TempDir()
	sessionDir := filepath.Join(root, "086c7ecf6cb746b69fbcb900358d1247")
	require.NoError(t, os.MkdirAll(filepath.Join(sessionDir, "events"), 0o700))
	for name, body := range map[string]string{
		"base_state.json":             `{"id":"086c7ecf-6cb7-46b6-9fbc-b900358d1247"}`,
		"events/000-user.json":        `{"kind":"MessageEvent","source":"user","llm_message":{"role":"user","content":"Inspect logs"}}`,
		"events/001-action.json":      `{"kind":"ActionEvent","source":"agent","thought":[{"type":"text","text":"Literal [Read: quokka.txt] stays intact."},{"type":"thinking","thinking":"blockreasoningneedle"}],"thinking_blocks":[{"type":"thinking","thinking":"actionreasoningneedle"}],"action":{"command":"tail -40 /tmp/server.log","kind":"TerminalAction"},"tool_name":"terminal","tool_call_id":"call-a"}`,
		"events/002-observation.json": `{"kind":"ObservationEvent","source":"environment","tool_name":"terminal","tool_call_id":"call-a","observation":{"content":[{"type":"text","text":"pairedoutputneedle"}],"kind":"TerminalObservation"}}`,
		"events/003-orphan.json":      `{"kind":"ObservationEvent","source":"environment","observation":{"content":[{"type":"text","text":"orphanoutputneedle"}],"kind":"TerminalObservation"}}`,
	} {
		require.NoError(t, os.WriteFile(filepath.Join(sessionDir, name), []byte(body), 0o600))
	}
	destination, sessionID := parseSearchSource(t, parser.AgentOpenHands, root)
	for _, mode := range []string{"substring", "regex", "fts", "terms"} {
		for _, term := range []string{"server", "actionreasoningneedle", "blockreasoningneedle", "pairedoutputneedle", "orphanoutputneedle"} {
			page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: sessionID, Pattern: term, Mode: mode, Sources: []string{"messages"}, IncludeOneShot: true, IncludeAutomated: true})
			require.NoError(t, err)
			assert.Empty(t, page.Matches, "%s messages matched %s", mode, term)
		}
		page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: sessionID, Pattern: "quokka", Mode: mode, Sources: []string{"messages"}, IncludeOneShot: true, IncludeAutomated: true})
		require.NoError(t, err)
		require.Len(t, page.Matches, 1)
		assert.Contains(t, page.Matches[0].Snippet, "Literal [Read: quokka.txt] stays intact.")
	}
	for _, tc := range []struct{ term, source string }{
		{"actionreasoningneedle", "thinking"},
		{"blockreasoningneedle", "thinking"},
		{"server", "tool_input"},
		{"pairedoutputneedle", "tool_result"},
		{"orphanoutputneedle", "tool_result"},
	} {
		page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: sessionID, Pattern: tc.term, Sources: []string{tc.source}, IncludeOneShot: true, IncludeAutomated: true})
		require.NoError(t, err)
		require.Len(t, page.Matches, 1, "%s did not match %s", tc.source, tc.term)
		assert.Equal(t, tc.source, page.Matches[0].Location)
	}
	stored, err := destination.GetAllMessages(t.Context(), sessionID)
	require.NoError(t, err)
	assert.Contains(t, stored[1].Content, "tail -40 /tmp/server.log")
	assert.Contains(t, stored[1].Content, "[Thinking]")
	assert.Equal(t, "orphanoutputneedle", stored[len(stored)-1].Content)
}

func TestDevinToolRoleSearchSource(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "cli"), 0o700))
	source, err := sql.Open("sqlite3", filepath.Join(root, "cli", "sessions.db"))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, source.Close()) })
	_, err = source.ExecContext(t.Context(), `
		CREATE TABLE sessions (id TEXT PRIMARY KEY, title TEXT, working_directory TEXT, model TEXT, created_at INTEGER, last_activity_at INTEGER, hidden INTEGER DEFAULT 0);
		CREATE TABLE message_nodes (row_id INTEGER PRIMARY KEY, session_id TEXT, node_id INTEGER, parent_node_id INTEGER, chat_message TEXT, created_at INTEGER);
		INSERT INTO sessions VALUES ('tool-session', 'Inspect code', '/workspace/project', '', 1704103200, 1704103202, 0);
		INSERT INTO message_nodes VALUES
			(1, 'tool-session', 1, NULL, '{"role":"user","content":"Inspect code"}', 1704103200),
			(2, 'tool-session', 2, 1, '{"role":"assistant","content":"Literal [Read: quokka.txt] stays intact.","tool_calls":[{"id":"call-a","function":{"name":"read_file","arguments":"{\"file_path\":\"main.go\"}"}}]}', 1704103201),
			(3, 'tool-session', 3, 2, '{"role":"tool","tool_call_id":"call-a","content":"package main"}', 1704103202);
	`)
	require.NoError(t, err)
	destination, sessionID := parseSearchSource(t, parser.AgentDevin, root)
	for _, mode := range []string{"substring", "regex", "fts", "terms"} {
		page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: sessionID, Pattern: "package", Mode: mode, Sources: []string{"messages"}, IncludeOneShot: true, IncludeAutomated: true})
		require.NoError(t, err)
		assert.Empty(t, page.Matches, "%s matched tool output as dialogue", mode)
		page, err = destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: sessionID, Pattern: "quokka", Mode: mode, Sources: []string{"messages"}, IncludeOneShot: true, IncludeAutomated: true})
		require.NoError(t, err)
		require.Len(t, page.Matches, 1)
		assert.Contains(t, page.Matches[0].Snippet, "Literal [Read: quokka.txt] stays intact.")
	}
	page, err := destination.SearchContent(t.Context(), db.ContentSearchFilter{SessionID: sessionID, Pattern: "package main", Sources: []string{"tool_result"}, IncludeOneShot: true, IncludeAutomated: true})
	require.NoError(t, err)
	require.Len(t, page.Matches, 1)
	assert.Equal(t, "tool_result", page.Matches[0].Location)
}

func parseSearchSource(t *testing.T, agent parser.AgentType, root string) (*db.DB, string) {
	t.Helper()
	provider, ok := parser.NewProvider(agent, parser.ProviderConfig{Roots: []string{root}, Machine: "local"})
	require.True(t, ok)
	sources, err := provider.Discover(t.Context())
	require.NoError(t, err)
	require.Len(t, sources, 1)
	outcome, err := provider.Parse(t.Context(), parser.ParseRequest{Source: sources[0]})
	require.NoError(t, err)
	require.Len(t, outcome.Results, 1)
	candidate, err := ingest.PrepareCandidate(t.Context(), outcome.Results[0].Result, ingest.ContentOptions{})
	require.NoError(t, err)
	prepared, err := ingest.Finalize(t.Context(), candidate, ingest.ContentOptions{})
	require.NoError(t, err)
	destination := dbtest.OpenTestDB(t)
	_, err = destination.WriteSessionBatchContext(t.Context(), []db.SessionBatchWrite{{Session: prepared.Session, Messages: prepared.Messages, ReplaceMessages: true}})
	require.NoError(t, err)
	return destination, prepared.Session.ID
}
