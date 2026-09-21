package service

import "strings"

func normalizeGroupModelsListConfig(cfg GroupModelsListConfig) GroupModelsListConfig {
	out := GroupModelsListConfig{Enabled: cfg.Enabled}
	if len(cfg.Models) == 0 {
		return out
	}

	seen := make(map[string]struct{}, len(cfg.Models))
	out.Models = make([]string, 0, len(cfg.Models))
	for _, model := range cfg.Models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		if _, ok := seen[model]; ok {
			continue
		}
		seen[model] = struct{}{}
		out.Models = append(out.Models, model)
	}
	if len(out.Models) == 0 {
		out.Models = nil
	}
	return out
}

func (g *Group) CustomModelsListEnabled() bool {
	return g != nil && g.ModelsListConfig.Enabled && len(g.ModelsListConfig.Models) > 0
}

// CodexModelsSelection 合并 Codex manifest 链路两份独立的分组配置：
// models_list_config（展示列表，决定“显示什么”）优先，其后拼上
// model_allowlist（准入白名单）的条目；任一启用即返回非空候选集，
// 两者都未启用时返回 nil（链路保持原有不过滤行为）。
func (g *Group) CodexModelsSelection() []string {
	if g == nil {
		return nil
	}
	var selection []string
	if g.CustomModelsListEnabled() {
		selection = append(selection, g.ModelsListConfig.Models...)
	}
	if g.ModelAllowlistEnabled() {
		selection = append(selection, g.ModelAllowlist.Models...)
	}
	return selection
}
