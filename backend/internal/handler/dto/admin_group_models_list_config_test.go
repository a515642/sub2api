//go:build unit

package dto

import (
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// TestAdminGroupRoundTripsModelsListConfig 复现合并回归：上游把本地
// models_list_config（/v1/models 展示列表）整体改名成 model_allowlist 时，AdminGroup
// DTO 与 GroupFromServiceAdmin mapper 的字段被 1:1 替换而非并存 —— 管理端响应不再
// 返回 models_list_config，前端无法回显/编辑该配置（同时掩盖了持久化层的静默丢弃）。
func TestAdminGroupRoundTripsModelsListConfig(t *testing.T) {
	g := &service.Group{
		ID:             7,
		Name:           "display-config-group",
		ModelAllowlist: service.GroupModelAllowlist{Enabled: true, Models: []string{"gpt-5.4"}},
		ModelsListConfig: service.GroupModelsListConfig{
			Enabled: true,
			Models:  []string{"gpt-5.4", "gpt-5.4-mini", "legacy-gpt-4.1"},
		},
	}

	out := GroupFromServiceAdmin(g)
	require.NotNil(t, out)

	// 管理端响应必须同时携带两个独立功能的配置（json 序列化层面）。
	raw, err := json.Marshal(out)
	require.NoError(t, err)
	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &payload))

	require.Contains(t, payload, "models_list_config",
		"DEFECT REPRODUCED: admin group response omits models_list_config — frontend cannot read back the display config")
	require.Contains(t, payload, "model_allowlist",
		"model_allowlist must stay (independent upstream feature)")

	var display service.GroupModelsListConfig
	require.NoError(t, json.Unmarshal(payload["models_list_config"], &display))
	require.True(t, display.Enabled)
	require.Equal(t, []string{"gpt-5.4", "gpt-5.4-mini", "legacy-gpt-4.1"}, display.Models)
}
