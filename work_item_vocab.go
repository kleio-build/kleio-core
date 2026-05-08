package kleio

import (
	"fmt"
	"strings"
	"sync"
)

// Canonical work item lifecycle statuses (WI-CAN-001). These are the only
// stable internal values normalization may produce besides config-load errors.
const (
	WorkItemStatusOpen        = "open"
	WorkItemStatusActive      = "active"
	WorkItemStatusBlocked     = "blocked"
	WorkItemStatusDone        = "done"
	WorkItemStatusIgnored     = "ignored"
	WorkItemStatusSuperseded  = "superseded"
	WorkItemStatusNeedsReview = "needs_review"
)

// Canonical work item granularity (WI-CAN-004). Persisted wiring comes in a
// later change; these constants are named for ingestion and derivation code.
const (
	WorkItemGranularityContainer = "container"
	WorkItemGranularityItem      = "item"
	WorkItemGranularitySubitem   = "subitem"
)

var canonicalWorkItemStatuses = map[string]struct{}{
	WorkItemStatusOpen:        {},
	WorkItemStatusActive:      {},
	WorkItemStatusBlocked:     {},
	WorkItemStatusDone:        {},
	WorkItemStatusIgnored:     {},
	WorkItemStatusSuperseded:  {},
	WorkItemStatusNeedsReview: {},
}

var canonicalWorkItemGranularities = map[string]struct{}{
	WorkItemGranularityContainer: {},
	WorkItemGranularityItem:      {},
	WorkItemGranularitySubitem:   {},
}

// WorkItemVocabulary holds merged alias lookup tables built from workspace
// configuration (WI-CAN-002, WI-CAN-005). Unknown upstream normalization policy
// for status is WI-CAN-003: map to WorkItemStatusNeedsReview (never silent drop).
//
// Granularity has no sentinel; NormalizeGranularity returns an error when the
// raw value is neither canonical nor a configured alias (WI-CAN-005).
type WorkItemVocabulary struct {
	statusLookup      map[string]string
	granularityLookup map[string]string
}

// BuiltinStatusAliases maps canonical status to legacy or common upstream
// labels merged with project YAML before building inverse lookups (WI-CAN-002).
// Covers backlog (`in_progress`) and Cursor plan YAML (see OpenSpec WI-PLN-003 / design mapping table).
var BuiltinStatusAliases = map[string][]string{
	WorkItemStatusOpen:    {"pending"},
	WorkItemStatusActive:  {StatusInProgress},
	WorkItemStatusDone:    {"completed"},
	WorkItemStatusIgnored: {"cancelled", "wontfix"},
}

// NewWorkItemVocabulary merges BuiltinStatusAliases with statusAliases and
// granularityAliases from workspace config.
//
// Config validation (WI-CAN-002 / WI-CAN-005): every map key MUST be canonical;
// every alias MUST be non-empty; an alias MUST NOT target two canonical values.
func NewWorkItemVocabulary(statusAliases, granularityAliases map[string][]string) (*WorkItemVocabulary, error) {
	sCanon, err := mergeAliasDeclarations("status", canonicalWorkItemStatuses, BuiltinStatusAliases, statusAliases)
	if err != nil {
		return nil, err
	}
	gCanon, err := mergeAliasDeclarations("granularity", canonicalWorkItemGranularities, nil, granularityAliases)
	if err != nil {
		return nil, err
	}

	statusInv, err := buildInverseMap("status", sCanon, canonicalWorkItemStatuses)
	if err != nil {
		return nil, err
	}
	granInv, err := buildInverseMap("granularity", gCanon, canonicalWorkItemGranularities)
	if err != nil {
		return nil, err
	}

	return &WorkItemVocabulary{
		statusLookup:      statusInv,
		granularityLookup: granInv,
	}, nil
}

var builtinVocabOnce sync.Once
var builtinVocab *WorkItemVocabulary

// BuiltinWorkItemVocabulary returns the process-wide builtins-only vocabulary
// (BuiltinStatusAliases with no workspace overrides). Safe for concurrent use.
func BuiltinWorkItemVocabulary() *WorkItemVocabulary {
	builtinVocabOnce.Do(func() {
		v, err := NewWorkItemVocabulary(nil, nil)
		if err != nil {
			panic("kleio: builtin work item vocabulary: " + err.Error())
		}
		builtinVocab = v
	})
	return builtinVocab
}

func mergeAliasDeclarations(kind string, allowed map[string]struct{}, builtins, override map[string][]string) (map[string][]string, error) {
	out := make(map[string][]string)
	for canon, aliases := range builtins {
		if _, ok := allowed[canon]; !ok {
			return nil, fmt.Errorf("%s aliases: internal default key %q is not canonical", kind, canon)
		}
		out[canon] = append(out[canon], aliases...)
	}
	for canon, aliases := range override {
		if _, ok := allowed[canon]; !ok {
			return nil, fmt.Errorf("%s_aliases: unknown canonical key %q", kind, canon)
		}
		out[canon] = append(out[canon], aliases...)
	}
	return out, nil
}

func buildInverseMap(kind string, canonToAliases map[string][]string, allowed map[string]struct{}) (map[string]string, error) {
	inv := make(map[string]string)
	for canon := range allowed {
		if _, exists := inv[canon]; !exists {
			inv[canon] = canon
		}
	}
	for canon, aliases := range canonToAliases {
		for _, alias := range aliases {
			a := strings.TrimSpace(alias)
			if a == "" {
				return nil, fmt.Errorf("%s_aliases: empty alias for canonical %q", kind, canon)
			}
			if prev, ok := inv[a]; ok && prev != canon {
				return nil, fmt.Errorf("%s_aliases: alias %q maps to both %q and %q", kind, a, prev, canon)
			}
			inv[a] = canon
		}
	}
	return inv, nil
}

// NormalizeStatus maps raw upstream strings to canonical lifecycle values.
//
// Policy (WI-CAN-003): if raw is neither canonical nor a configured alias after
// strings.TrimSpace, the result is (WorkItemStatusNeedsReview, nil).
func (v *WorkItemVocabulary) NormalizeStatus(raw string) (string, error) {
	if v == nil {
		return "", fmt.Errorf("work item vocabulary: nil receiver")
	}
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", fmt.Errorf("NormalizeStatus: empty raw value")
	}
	if canon, ok := v.statusLookup[s]; ok {
		return canon, nil
	}
	return WorkItemStatusNeedsReview, nil
}

// NormalizeGranularity maps raw strings to container | item | subitem using
// configured aliases only (no builtins). Unknown values return an error.
func (v *WorkItemVocabulary) NormalizeGranularity(raw string) (string, error) {
	if v == nil {
		return "", fmt.Errorf("work item vocabulary: nil receiver")
	}
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", fmt.Errorf("NormalizeGranularity: empty raw value")
	}
	if canon, ok := v.granularityLookup[s]; ok {
		return canon, nil
	}
	return "", fmt.Errorf("NormalizeGranularity: unknown granularity %q", raw)
}
