package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// 以下用例复现合并回归：上游把本地 models_list_config（/v1/models 展示列表）
// 整体改名成 model_allowlist 时，所有展示读取分支被 1:1 替换为白名单分支，
// CustomModelsListEnabled() 在合并后没有任何调用方 —— 展示配置彻底失效。
// 按本地双列设计，两者必须独立生效：展示列表决定“列表显示什么”，
// 准入白名单决定“请求允许调什么”；这里逐链路断言展示配置仍被读取。

// TestModelsListConfigStillFiltersModelsEndpoint 复现 /v1/models 主链路：
// 仅设置 ModelsListConfig（不设 ModelAllowlist）时，列表必须按展示配置过滤。
func TestModelsListConfigStillFiltersModelsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)

	groupID := int64(91)
	h := newGatewayModelsHandlerForTest(
		&gatewayModelsAccountRepoStub{
			byGroup: map[int64][]service.Account{
				groupID: {
					{
						ID:       1,
						Platform: service.PlatformOpenAI,
						Credentials: map[string]any{
							"model_mapping": map[string]any{
								"gpt-5.4":         "gpt-5.4",
								"gpt-5.5":         "gpt-5.5",
								"legacy-gpt-2024": "legacy-gpt-2024",
							},
						},
					},
				},
			},
		},
	)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
		Group: &service.Group{
			ID:       groupID,
			Platform: service.PlatformOpenAI,
			ModelsListConfig: service.GroupModelsListConfig{
				Enabled: true,
				Models:  []string{"gpt-5.5", "missing-model", "gpt-5.4"},
			},
		},
	})

	h.Models(c)

	require.Equal(t, http.StatusOK, rec.Code)

	var got gatewayModelsResponseForTest
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, []string{"gpt-5.5", "gpt-5.4"}, modelIDsForTest(got.Data),
		"DEFECT REPRODUCED: models_list_config（展示列表）不再被 /v1/models 读取")
}

// TestModelsListConfigStillFiltersCompositeModelsEndpoint 复现 composite 平台链路。
func TestModelsListConfigStillFiltersCompositeModelsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)

	groupID := int64(92)
	h := newGatewayModelsHandlerForTest(
		&gatewayModelsAccountRepoStub{
			byGroup: map[int64][]service.Account{
				groupID: {
					{
						ID:       1,
						Platform: service.PlatformOpenAI,
						Credentials: map[string]any{
							"model_mapping": map[string]any{
								"gpt-5.4": "gpt-5.4",
								"gpt-5.5": "gpt-5.5",
							},
						},
					},
					{
						ID:       2,
						Platform: service.PlatformGemini,
						Credentials: map[string]any{
							"model_mapping": map[string]any{
								"gemini-2.5-flash": "gemini-2.5-flash",
							},
						},
					},
				},
			},
		},
	)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
		Group: &service.Group{
			ID:       groupID,
			Platform: service.PlatformComposite,
			ModelsListConfig: service.GroupModelsListConfig{
				Enabled: true,
				Models:  []string{"gpt-5.5", "gemini-2.5-flash"},
			},
		},
	})

	h.Models(c)

	require.Equal(t, http.StatusOK, rec.Code)

	var got gatewayModelsResponseForTest
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, []string{"gpt-5.5", "gemini-2.5-flash"}, modelIDsForTest(got.Data),
		"DEFECT REPRODUCED: composite 平台不再读取 models_list_config")
}

// TestModelsListConfigStillFiltersCodexModelsManifest 复现 CodexModels 链路。
func TestModelsListConfigStillFiltersCodexModelsManifest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	groupID := int64(93)
	h := newGatewayModelsHandlerForTest(
		&gatewayModelsAccountRepoStub{
			byGroup: map[int64][]service.Account{
				groupID: {
					{
						ID:       1,
						Platform: service.PlatformOpenAI,
						Credentials: map[string]any{
							"model_mapping": map[string]any{"gpt-5.5": "gpt-5.5"},
						},
					},
					{
						ID:       2,
						Platform: service.PlatformGrok,
						Credentials: map[string]any{
							"model_mapping": map[string]any{"grok-4.6": "grok-4.6"},
						},
					},
				},
			},
		},
	)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/models?client_version=0.147.0", nil)
	c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
		Group: &service.Group{
			ID:       groupID,
			Platform: service.PlatformComposite,
			ModelsListConfig: service.GroupModelsListConfig{
				Enabled: true,
				Models:  []string{"grok-4.6"},
			},
		},
	})

	h.CodexModels(c)

	require.Equal(t, http.StatusOK, rec.Code)
	var got codexModelsResponseForTest
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	slugs := make([]string, 0, len(got.Models))
	for _, model := range got.Models {
		slugs = append(slugs, model.Slug)
	}
	require.Equal(t, []string{"grok-4.6"}, slugs,
		"DEFECT REPRODUCED: CodexModels manifest 不再读取 models_list_config")
}
