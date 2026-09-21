//go:build integration

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// TestGetByKeyForAuthCarriesGroupModelsListConfig 复现认证投影缺陷：本地定制字段
// models_list_config（/v1/models 展示列表）未进入 GetByKeyForAuth 的 WithGroup
// Select 投影 —— ent 只加载被选列，热路径 apiKey.Group.ModelsListConfig 恒为空值，
// 展示功能在网关侧静默失效。该投影的注释本身就要求「新增快照分组字段时必须同步
// 本投影」，合并时上游版本覆盖投影列表导致该列被漏选。
func TestGetByKeyForAuthCarriesGroupModelsListConfig(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	group := mustCreateGroup(t, integrationEntClient, &service.Group{
		Name: fmt.Sprintf("models-list-config-proj-group-%d", suffix), Platform: service.PlatformOpenAI,
		RateMultiplier: 1,
		ModelsListConfig: service.GroupModelsListConfig{
			Enabled: true,
			Models:  []string{"gpt-5.4", "gpt-5.4-mini", "legacy-gpt-4.1"},
		},
	})
	user := mustCreateUser(t, integrationEntClient, &service.User{
		Email: fmt.Sprintf("models-list-config-proj-%d@example.com", suffix), Concurrency: 5,
	})
	groupID := group.ID
	keyValue := fmt.Sprintf("sk-models-list-config-proj-%d", suffix)
	apiKeyRepo := NewAPIKeyRepository(integrationEntClient, integrationDB)
	key := &service.APIKey{UserID: user.ID, GroupID: &groupID, Key: keyValue, Name: "models-list-config-proj", Status: service.StatusActive}
	require.NoError(t, apiKeyRepo.Create(ctx, key))
	t.Cleanup(func() {
		_, err := integrationDB.ExecContext(ctx, "DELETE FROM auth_cache_invalidation_outbox WHERE cache_key = encode(sha256(convert_to($1, 'UTF8')), 'hex')", keyValue)
		require.NoError(t, err)
		_, err = integrationDB.ExecContext(ctx, "DELETE FROM api_keys WHERE id = $1", key.ID)
		require.NoError(t, err)
		_, err = integrationDB.ExecContext(ctx, "DELETE FROM users WHERE id = $1", user.ID)
		require.NoError(t, err)
		_, err = integrationDB.ExecContext(ctx, "DELETE FROM groups WHERE id = $1", group.ID)
		require.NoError(t, err)
	})

	got, err := apiKeyRepo.GetByKeyForAuth(ctx, keyValue)
	require.NoError(t, err)
	require.NotNil(t, got.Group)
	require.True(t, got.Group.ModelsListConfig.Enabled,
		"DEFECT REPRODUCED: models_list_config 必须进入认证投影（投影漏列会让 /v1/models 展示功能在热路径静默失效）")
	require.Equal(t, []string{"gpt-5.4", "gpt-5.4-mini", "legacy-gpt-4.1"}, got.Group.ModelsListConfig.Models)
}
