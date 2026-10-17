package ingest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/ingest"
	"go.kenn.io/agentsview/internal/parser"
)

func TestDialogueProjectionSurvivesPreparation(t *testing.T) {
	const dialogue = "answerneedle Literal [Read: quotedneedle.txt]"
	parsed := parser.ParseResult{
		Session: parser.ParsedSession{ID: "dialogue", Agent: parser.AgentPi},
		Messages: []parser.ParsedMessage{{
			Role: parser.RoleAssistant, Content: dialogue + " [Read: inputneedle.txt]",
			DialogueText: new(dialogue), ThinkingText: "reasoningneedle",
		}},
	}
	candidate, err := ingest.PrepareCandidate(t.Context(), parsed, ingest.ContentOptions{})
	require.NoError(t, err)
	prepared, err := ingest.Finalize(t.Context(), candidate, ingest.ContentOptions{})
	require.NoError(t, err)
	require.Len(t, prepared.Messages, 1)
	assert.Equal(t, new(dialogue), prepared.Messages[0].DialogueText)
}
