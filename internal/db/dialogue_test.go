package db

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDialogueColumnUpgradeKeepsArchiveInPlace(t *testing.T) {
	for _, source := range []string{"messages", "message_search_content"} {
		t.Run(source, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "archive.db")
			d := testDBAtPath(t, path, "source")
			insertSession(t, d, "pi:kept", "project", func(s *Session) { s.Agent = "pi"; s.UserMessageCount = 1 })
			const content = "[Thinking]\nreasoningneedle\n[/Thinking]\nAnswer"
			insertMessages(t, d,
				Message{SessionID: "pi:kept", Ordinal: 0, Role: "user", Content: "Question"},
				Message{SessionID: "pi:kept", Ordinal: 1, Role: "assistant", Content: content, ThinkingText: "reasoningneedle", HasThinking: true},
			)
			before, err := d.GetAllMessages(t.Context(), "pi:kept")
			require.NoError(t, err)
			require.NoError(t, d.DropFTS(t.Context()))
			legacyView := "ALTER TABLE messages DROP COLUMN dialogue_text;"
			if source == "message_search_content" {
				legacyView = "CREATE VIEW message_search_content AS SELECT id, session_id, COALESCE(dialogue_text, content) AS content FROM messages;"
			}
			_, err = d.getWriter().Exec(t.Context(), fmt.Sprintf(`DROP VIEW message_search_content;
 %s
 CREATE VIRTUAL TABLE messages_fts USING fts5(content, content='%s', content_rowid='id', tokenize='porter unicode61');
 INSERT INTO messages_fts(messages_fts) VALUES('rebuild');
 PRAGMA user_version=%d; UPDATE sessions SET data_version=%d;`, legacyView, source, CurrentDataVersion()-1, CurrentDataVersion()-1))
			require.NoError(t, err)
			require.NoError(t, d.Close())
			d, err = Open(t.Context(), path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, d.Close()) })
			assert.False(t, d.NeedsResync())
			stored, err := d.GetAllMessages(t.Context(), "pi:kept")
			require.NoError(t, err)
			require.Len(t, stored, 2)
			assert.Equal(t, before[1].ID, stored[1].ID)
			assert.Equal(t, content, stored[1].Content)
			assert.Nil(t, stored[1].DialogueText)
			assert.Equal(t, "reasoningneedle", stored[1].ThinkingText)
			session, err := d.GetSession(t.Context(), "pi:kept")
			require.NoError(t, err)
			assert.Equal(t, CurrentDataVersion()-1, session.DataVersion)
			for _, mode := range []string{"fts", "terms"} {
				page, err := d.SearchContent(t.Context(), ContentSearchFilter{SessionID: "pi:kept", Pattern: "reasoningneedle", Mode: mode, IncludeOneShot: true})
				require.NoError(t, err)
				require.Len(t, page.Matches, 1)
				assert.Equal(t, "message", page.Matches[0].Location)
			}
		})
	}
}

func TestDialogueProjectionSeparatesMessagesAndThinking(t *testing.T) {
	for _, classified := range []bool{true, false} {
		t.Run(fmt.Sprint(classified), func(t *testing.T) {
			d := testDB(t)
			insertSession(t, d, "search", "project", func(s *Session) { s.UserMessageCount = 2 })
			message := Message{SessionID: "search", Ordinal: 1, Role: "assistant",
				Content:      "answerneedle [Thinking] reasoningneedle [/Thinking] [Read: file.txt]",
				ThinkingText: "reasoningneedle"}
			if classified {
				message.DialogueText = new("answerneedle")
			}
			insertMessages(t, d, Message{SessionID: "search", Role: "user", Content: "Question"}, message)
			for _, mode := range []string{"substring", "regex", "fts", "terms"} {
				for _, term := range []string{"answerneedle", "reasoningneedle", "file.txt"} {
					page, err := d.SearchContent(t.Context(), ContentSearchFilter{Pattern: term, Mode: mode, Sources: []string{"messages"}})
					require.NoError(t, err)
					if classified && term != "answerneedle" {
						assert.Empty(t, page.Matches, "%s %s", mode, term)
					} else {
						require.Len(t, page.Matches, 1, "%s %s", mode, term)
						assert.Equal(t, "message", page.Matches[0].Location)
					}
				}
			}
			for _, mode := range []string{"substring", "regex"} {
				for _, sources := range [][]string{nil, {"thinking"}} {
					page, err := d.SearchContent(t.Context(), ContentSearchFilter{Pattern: "reasoningneedle", Mode: mode, Sources: sources})
					require.NoError(t, err)
					require.Len(t, page.Matches, 1)
					location := "thinking"
					if !classified && sources == nil {
						location = "message"
					}
					assert.Equal(t, location, page.Matches[0].Location)
				}
			}
			for _, mode := range []string{"terms", "semantic", "hybrid"} {
				_, err := d.SearchContent(t.Context(), ContentSearchFilter{Pattern: "reasoningneedle", Mode: mode, Sources: []string{"thinking"}})
				var inputErr *SearchInputError
				require.ErrorAs(t, err, &inputErr)
			}
		})
	}
}

func TestSearchRetainsRenderedContentAlongsideDialogue(t *testing.T) {
	for _, tc := range []struct{ name, dialogue, hidden, replacement string }{
		{"porter", "answerneedle", "reasoningneedle", "replacementneedle"},
		{"cjk", "中文搜索", "工具输出", "秘密推理"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "cjk" && !simpleFTSRuntimeConfig.available() {
				t.Skip("simple FTS5 runtime is not installed for this test process")
			}
			d := testDB(t)
			insertSession(t, d, "search", "project", func(s *Session) { s.UserMessageCount = 2 })
			messages := []Message{
				{SessionID: "search", Ordinal: 0, Role: "user", Content: "Question"},
				{SessionID: "search", Ordinal: 1, Role: "assistant", Content: tc.dialogue + " [Thinking] " + tc.hidden, DialogueText: new(tc.dialogue)},
				{SessionID: "search", Ordinal: 2, Role: "assistant", Content: "Done"},
			}
			insertMessages(t, d, messages...)
			for _, phase := range []string{"insert", "update", "delete"} {
				t.Run(phase, func(t *testing.T) {
					if phase == "update" {
						messages[1].Content = tc.dialogue + " [Thinking] " + tc.replacement
						require.NoError(t, d.ReplaceSessionMessages(t.Context(), "search", messages))
					} else if phase == "delete" {
						require.NoError(t, d.ReplaceSessionMessages(t.Context(), "search", nil))
					}
					for _, term := range []string{tc.dialogue, tc.hidden, tc.replacement} {
						ordinary, err := d.Search(t.Context(), SearchFilter{Query: term})
						require.NoError(t, err)
						wantOrdinary := 0
						if phase != "delete" && (term == tc.dialogue || phase == "insert" && term == tc.hidden || phase == "update" && term == tc.replacement) {
							wantOrdinary = 1
						}
						assert.Len(t, ordinary.Results, wantOrdinary, term)
						page, err := d.SearchContent(t.Context(), ContentSearchFilter{Pattern: term, Mode: "fts", Sources: []string{"messages"}})
						require.NoError(t, err)
						if phase != "delete" && term == tc.dialogue {
							require.Len(t, page.Matches, 1)
							assert.Equal(t, tc.dialogue, page.Matches[0].Snippet)
						} else {
							assert.Empty(t, page.Matches, term)
						}
					}
				})
			}
		})
	}
}

func TestProjectionOnlyReparsePreservesTranscriptFreshness(t *testing.T) {
	d := testDB(t)
	insertSession(t, d, "projection", "project")
	messages := []Message{{SessionID: "projection", Role: "assistant", Content: "Answer [Thinking] reasoning"}}
	insertMessages(t, d, messages...)
	_, err := d.getWriter().Exec(t.Context(), `UPDATE sessions SET
		secrets_rules_version = 'secrets-current', friction_rules_version = 'friction-current',
		local_modified_at = '2026-05-01T10:00:00Z' WHERE id = 'projection'`)
	require.NoError(t, err)
	before, err := d.GetSession(t.Context(), "projection")
	require.NoError(t, err)
	for _, dialogue := range []string{"Answer", ""} {
		messages[0].DialogueText = new(dialogue)
		require.NoError(t, d.ReplaceSessionMessages(t.Context(), "projection", messages))
		stored, err := d.GetAllMessages(t.Context(), "projection")
		require.NoError(t, err)
		require.Len(t, stored, 1)
		assert.Equal(t, new(dialogue), stored[0].DialogueText)
		after, err := d.GetSession(t.Context(), "projection")
		require.NoError(t, err)
		assert.Equal(t, before.TranscriptRevision, after.TranscriptRevision)
		var secretVersion, frictionVersion, modified string
		require.NoError(t, d.getReader().QueryRow(t.Context(), `SELECT secrets_rules_version,
			friction_rules_version, local_modified_at FROM sessions WHERE id = 'projection'`).Scan(&secretVersion, &frictionVersion, &modified))
		assert.Equal(t, "secrets-current", secretVersion)
		assert.Equal(t, "friction-current", frictionVersion)
		assert.Equal(t, "2026-05-01T10:00:00Z", modified)
	}
}
