package handler

import (
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// TestPinnedCodexModelsStillAppliesModelsListConfigFilter 复现 pinned Codex
// manifest 链路（CodexModelsManifestConfig.Enabled 时走固定账号发现）：
// 上游引入该链路时读的是 CustomModelsListEnabled/ModelsListConfig（commit
// cb3103397），合并改名后被替换为 ModelAllowlist —— 仅设置展示配置的分组
// 在 pinned manifest 中不再被过滤。
func TestPinnedCodexModelsStillAppliesModelsListConfigFilter(t *testing.T) {
	accounts := []service.Account{
		newPinnedCodexAccount(2, service.StatusActive, true, false),
		newPinnedCodexAccount(3, service.StatusActive, true, false),
	}
	upstream := &codexModelsPinnedHTTPUpstream{bodies: map[int64]string{
		2: `{"models":[{"slug":"model-a"}]}`,
		3: `{"models":[{"slug":"model-b"}]}`,
	}}
	handler := newPinnedCodexTestHandler(accounts, upstream, 3)
	group := &service.Group{
		ID:       94,
		Platform: service.PlatformOpenAI,
		ModelsListConfig: service.GroupModelsListConfig{
			Enabled: true,
			Models:  []string{"model-b"},
		},
		CodexModelsManifestConfig: service.GroupCodexModelsManifestConfig{
			Enabled:    true,
			AccountIDs: []int64{2, 3},
		},
	}

	recorder := performPinnedCodexModelsRequest(t, handler, group, "")
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, []string{"model-b"}, codexHandlerManifestSlugs(t, recorder),
		"DEFECT REPRODUCED: pinned Codex manifest 不再读取 models_list_config（上游原名 CustomModelsList）")
}
