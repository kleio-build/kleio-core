package kleio

import (
	"context"
	"encoding/json"
	"fmt"
)

// IntrinsicQualityRuleVersion bumps when scoring heuristics change; stored inside quality_reasons JSON.
const IntrinsicQualityRuleVersion = "2026-05-08-v1"

// IntrinsicQualitySignals is the machine-readable contributor block (RFC-QLG-002 sketch).
type IntrinsicQualitySignals struct {
	SourceConfidence     float64 `json:"source_confidence"`
	HasStructuredOrigin  bool    `json:"has_structured_origin"`
	HasEntityAnchor      bool    `json:"has_entity_anchor"`
	LinkedToPlan         bool    `json:"linked_to_plan"`
	LinkedToCommit       bool    `json:"linked_to_commit"`
	IsDuplicateCandidate bool    `json:"is_duplicate_candidate"`
}

type intrinsicQualityReasonsDoc struct {
	RuleVersion string                  `json:"rule_version"`
	Signals     IntrinsicQualitySignals `json:"signals"`
	DedupeOf    *string                 `json:"dedupe_of"`
}

// LabelTrustRank ranks label sources for upsert tie-break (human > plan > inferred).
func LabelTrustRank(source string) int {
	switch source {
	case AuthorTypeHuman:
		return 40
	case "cursor_plan":
		return 10
	default:
		return 0
	}
}

// RecomputeIntrinsicQuality derives intrinsic score + reasons from store evidence and persists via UpdateWorkItemQuality.
func RecomputeIntrinsicQuality(ctx context.Context, store Store, workItemID string) error {
	if store == nil || workItemID == "" {
		return nil
	}
	wi, err := store.GetWorkItem(ctx, workItemID)
	if err != nil || wi == nil {
		return err
	}

	links, err := store.QueryLinks(ctx, LinkFilter{
		TargetID: workItemID,
		LinkType: LinkTypeDerivedFrom,
	})
	if err != nil {
		return err
	}

	var sig IntrinsicQualitySignals
	planCount := 0
	for _, l := range links {
		ev, err := store.GetEvent(ctx, l.SourceID)
		if err != nil || ev == nil {
			continue
		}
		if ev.SourceType == "cursor_plan" {
			sig.LinkedToPlan = true
			planCount++
		}
		if ev.SourceType == SourceTypeLocalGit {
			sig.LinkedToCommit = true
		}
		if ev.StructuredData != "" {
			var sd map[string]any
			if json.Unmarshal([]byte(ev.StructuredData), &sd) == nil {
				if _, ok := sd[StructuredKeyTodoID].(string); ok {
					sig.HasStructuredOrigin = true
				} else if off, ok := sd[StructuredKeySourceOffset].(string); ok && off != "" {
					sig.HasStructuredOrigin = true
				}
			}
		}
	}

	base := 0.35
	if sig.LinkedToPlan {
		base += 0.25
	}
	if sig.HasStructuredOrigin {
		base += 0.2
	}
	if planCount > 1 {
		sig.IsDuplicateCandidate = true
		base -= 0.05
	}
	if sig.LinkedToCommit {
		base += 0.1
	}
	if base > 1 {
		base = 1
	}
	if base < 0 {
		base = 0
	}
	sig.SourceConfidence = base

	if wi.RepoName != "" {
		lbl := &WorkItemLabel{
			WorkItemID: workItemID,
			LabelText:  "project:" + wi.RepoName,
			Source:     "intrinsic_quality",
			Confidence: sig.SourceConfidence,
		}
		_ = store.UpsertWorkItemLabel(ctx, lbl)
	}

	doc := intrinsicQualityReasonsDoc{
		RuleVersion: IntrinsicQualityRuleVersion,
		Signals:     sig,
		DedupeOf:    nil,
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if err := store.UpdateWorkItemQuality(ctx, workItemID, sig.SourceConfidence, string(raw)); err != nil {
		return fmt.Errorf("update work item quality: %w", err)
	}
	return nil
}
