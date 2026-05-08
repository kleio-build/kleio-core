package kleio

import (
	"strings"
	"testing"
)

func TestNewWorkItemVocabulary_LegacyInProgressToActive(t *testing.T) {
	vocab, err := NewWorkItemVocabulary(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := vocab.NormalizeStatus(StatusInProgress)
	if err != nil {
		t.Fatal(err)
	}
	if got != WorkItemStatusActive {
		t.Fatalf("want %s, got %s", WorkItemStatusActive, got)
	}
}

func TestNormalizeStatus_CustomAliasRoundTrip(t *testing.T) {
	status := map[string][]string{
		WorkItemStatusDone: {"completed", "shipped"},
	}
	vocab, err := NewWorkItemVocabulary(status, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := vocab.NormalizeStatus("completed")
	if err != nil || got != WorkItemStatusDone {
		t.Fatalf("completed: got %q err %v", got, err)
	}

	got2, err := vocab.NormalizeStatus(" shipped ")
	if err != nil || got2 != WorkItemStatusDone {
		t.Fatalf("shipped trimmed: got %q err %v", got2, err)
	}
}

func TestNormalizeStatus_BuiltinPlanAndBacklogSynonyms(t *testing.T) {
	vocab, err := NewWorkItemVocabulary(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ raw, want string }{
		{"pending", WorkItemStatusOpen},
		{"completed", WorkItemStatusDone},
		{"cancelled", WorkItemStatusIgnored},
		{"wontfix", WorkItemStatusIgnored},
		{StatusInProgress, WorkItemStatusActive},
	} {
		got, err := vocab.NormalizeStatus(tc.raw)
		if err != nil || got != tc.want {
			t.Fatalf("%q: got %q err %v want %q", tc.raw, got, err, tc.want)
		}
	}
}

func TestNormalizeStatus_UnknownUpstreamNeedsReview(t *testing.T) {
	vocab, err := NewWorkItemVocabulary(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := vocab.NormalizeStatus("whatever_tool_emits")
	if err != nil {
		t.Fatal(err)
	}
	if got != WorkItemStatusNeedsReview {
		t.Fatalf("want %s, got %s", WorkItemStatusNeedsReview, got)
	}
}

func TestNormalizeStatus_CanonicalPassthrough(t *testing.T) {
	vocab, err := NewWorkItemVocabulary(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{
		WorkItemStatusOpen, WorkItemStatusActive, WorkItemStatusBlocked,
		WorkItemStatusDone, WorkItemStatusIgnored, WorkItemStatusSuperseded, WorkItemStatusNeedsReview,
	} {
		t.Run(s, func(t *testing.T) {
			got, err := vocab.NormalizeStatus(s)
			if err != nil || got != s {
				t.Fatalf("got %q err %v", got, err)
			}
		})
	}
}

func TestNewWorkItemVocabulary_DuplicateAliasError(t *testing.T) {
	status := map[string][]string{
		WorkItemStatusOpen: {"dup"},
		WorkItemStatusDone: {"dup"},
	}
	_, err := NewWorkItemVocabulary(status, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	low := strings.ToLower(err.Error())
	if !strings.Contains(low, "alias") || !strings.Contains(low, "dup") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewWorkItemVocabulary_InvalidCanonicalKeyError(t *testing.T) {
	status := map[string][]string{"not_a_canonical": {"x"}}
	_, err := NewWorkItemVocabulary(status, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewWorkItemVocabulary_EmptyAliasError(t *testing.T) {
	status := map[string][]string{WorkItemStatusOpen: {"  "}}
	_, err := NewWorkItemVocabulary(status, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNormalizeGranularity_AliasRoundTrip(t *testing.T) {
	gran := map[string][]string{
		WorkItemGranularityContainer: {"epic"},
		WorkItemGranularityItem:      {"story"},
	}
	vocab, err := NewWorkItemVocabulary(nil, gran)
	if err != nil {
		t.Fatal(err)
	}

	g, err := vocab.NormalizeGranularity(" epic ")
	if err != nil || g != WorkItemGranularityContainer {
		t.Fatalf("epic: got %q err %v", g, err)
	}

	g, err = vocab.NormalizeGranularity("story")
	if err != nil || g != WorkItemGranularityItem {
		t.Fatalf("story: got %q err %v", g, err)
	}
}

func TestNormalizeGranularity_CanonicalPassthrough(t *testing.T) {
	vocab, err := NewWorkItemVocabulary(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range []string{WorkItemGranularityContainer, WorkItemGranularityItem, WorkItemGranularitySubitem} {
		got, err := vocab.NormalizeGranularity(g)
		if err != nil || got != g {
			t.Fatalf("%s: got %q err %v", g, got, err)
		}
	}
}

func TestNormalizeGranularity_UnknownError(t *testing.T) {
	vocab, err := NewWorkItemVocabulary(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = vocab.NormalizeGranularity("mystery")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNormalizeStatus_EmptyRawError(t *testing.T) {
	vocab, err := NewWorkItemVocabulary(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = vocab.NormalizeStatus("   ")
	if err == nil {
		t.Fatal("expected error")
	}
}
