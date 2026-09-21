//go:build integration

package repository

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// TestGroupRepoPersistsModelsListConfig 复现合并回归：本地定制字段 models_list_config
// （自定义 /v1/models 展示列表）在 group_repo 的 create/update builder 中丢失
// SetModelsListConfig 调用 —— 管理端提交的配置被静默丢弃（create 落 ent 默认空值，
// update 则完全不触碰该列），且认证投影（GetByKeyForAuth WithGroup Select）也未
// 选出该列，热路径读到的永远是空配置。
//
// 合并背景：上游 cff3f8985 把该功能整体改名成 model_allowlist，合并时上游版本的
// group_repo.go / api_key_repo.go 投影覆盖了本地实现；fork 的请求 DTO 与 service
// 归一化仍在接受并归一化该字段，形成「接受输入、静默丢弃」的断链。
func TestGroupRepoPersistsModelsListConfig(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	repo := newGroupRepositoryWithSQL(tx.Client(), tx)

	// --- 复现 create：提交非空展示配置，落库后必须原样读回。 ---
	groupIn := &service.Group{
		Name:           "models-list-persist-repro",
		Platform:       service.PlatformOpenAI,
		RateMultiplier: 1,
		Status:         service.StatusActive,
		ModelAllowlist: service.GroupModelAllowlist{Enabled: false},
		ModelsListConfig: service.GroupModelsListConfig{
			Enabled: true,
			Models:  []string{"gpt-5.4", "gpt-5.4-mini", "legacy-gpt-4.1"},
		},
	}
	require.NoError(t, repo.Create(ctx, groupIn), "create group with display config")
	createdID := groupIn.ID

	fetched, err := repo.GetByID(ctx, createdID)
	require.NoError(t, err)
	require.True(t, fetched.ModelsListConfig.Enabled,
		"DEFECT REPRODUCED: create dropped models_list_config — service normalized input was silently discarded")
	require.Equal(t, []string{"gpt-5.4", "gpt-5.4-mini", "legacy-gpt-4.1"}, fetched.ModelsListConfig.Models,
		"DEFECT REPRODUCED: create persisted wrong models_list_config")

	// --- 复现 update：修改展示列表后必须生效。 ---
	fetched.ModelsListConfig = service.GroupModelsListConfig{
		Enabled: true,
		Models:  []string{"gpt-5.4"},
	}
	require.NoError(t, repo.Update(ctx, fetched), "update group display config")
	refetched, err := repo.GetByID(ctx, createdID)
	require.NoError(t, err)
	require.Equal(t, []string{"gpt-5.4"}, refetched.ModelsListConfig.Models,
		"DEFECT REPRODUCED: update dropped models_list_config changes")
	require.True(t, refetched.ModelsListConfig.Enabled)
}
