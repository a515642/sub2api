//go:build unit

package handler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// ===== 复现审计缺陷 D7：Seedance 计费 claim 泄漏 → 48h 内计费永久丢失 =====
//
// 链路（Seedance inline 计费，handler/seedance.go + grok_media.go）：
//  1. create 成功 → StoreGrokVideoPendingBilling 落 create-time 快照；
//  2. status poll 观察到完成（Usage.OutputTokens > 0）→
//     prepareSeedanceCompletionBilling → ClaimGrokVideoBilling（SETNX，TTL 48h）
//     → 返回 merged 账单 → recordGrokMediaUsage → RecordUsage；
//  3. RecordUsage 失败（计费 DB 抖动等）→ recordGrokMediaUsage 只打日志；
//     service 层 ReleaseGrokVideoBilling 的注释明说"RecordUsage 失败后清除
//     claim 以便后续 poll 重试计费"，但没有任何调用方（死代码）；
//  4. claim 挂满 48h TTL：用户几秒/几分钟后重试 status poll（视频任务的
//     自然行为）→ ClaimGrokVideoBilling 返回 false → 之后 48h 内所有 poll
//     都不再计费 → 计费永久丢失。
//
// 对照：dedup claim 的语义是"计费已提交"而非"计费已成功"——失败必须回滚。
//
// 失败注入：RecordUsage 走生产（非 SIMPLE）分支，applyUsageBilling →
// usageBillingRepo.Apply 注入错误（grokSeedanceClaimLeakBillingRepo.applyErr），
// RecordUsage 原样返回错误——与生产中计费 DB 事务失败等价。
// 注意必须用生产模式：SIMPLE 模式在 Apply 之前就短路（只记 usage log），
// 注入点永远走不到。

// grokSeedanceClaimLeakUserRepo 提供 CheckBillingEligibility 所需的正余额。
// UserRepository 其余方法由嵌入接口兜底（本链路不会触达）。
type grokSeedanceClaimLeakUserRepo struct {
	service.UserRepository
	balance float64
}

func (r *grokSeedanceClaimLeakUserRepo) GetByID(context.Context, int64) (*service.User, error) {
	return &service.User{ID: 10, Balance: r.balance}, nil
}

// grokSeedanceClaimLeakBillingRepo 在生产模式计费链上计数并注入 Apply 失败。
type grokSeedanceClaimLeakBillingRepo struct {
	applyErr error
	applies  int
}

func (r *grokSeedanceClaimLeakBillingRepo) Apply(context.Context, *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	r.applies++
	if r.applyErr != nil {
		return nil, r.applyErr
	}
	return &service.UsageBillingApplyResult{Applied: true}, nil
}

func (r *grokSeedanceClaimLeakBillingRepo) ReserveBatchImageBalance(context.Context, *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	return &service.BatchImageBalanceHoldResult{Applied: true}, nil
}
func (r *grokSeedanceClaimLeakBillingRepo) CaptureBatchImageBalance(context.Context, *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	return &service.BatchImageBalanceHoldResult{Applied: true}, nil
}
func (r *grokSeedanceClaimLeakBillingRepo) ReleaseBatchImageBalance(context.Context, *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	return &service.BatchImageBalanceHoldResult{Applied: true}, nil
}

var _ service.UsageBillingRepository = (*grokSeedanceClaimLeakBillingRepo)(nil)

// newGrokSeedanceClaimLeakHandler 与 newGrokVideoHoldLeakHandler 相同的链路，
// 但跑生产（非 simple）模式并注入正余额 user repo：
//  1. RecordUsage 才会走 applyUsageBilling → usageBillingRepo.Apply（注入点）；
//  2. CheckBillingEligibility 的余额检查才会通过（余额 > 0）。
//
// 生产模式下余额检查会经 GetUserBalance → userRepo.GetByID 回源，
// 故 billing cache 必须携带 user repo 桩（返回固定正余额）。
func newGrokSeedanceClaimLeakHandler(t *testing.T, billingRepo service.UsageBillingRepository) (*OpenAIGatewayHandler, *grokMediaSlotBindings, *grokMediaSlotUpstream) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	// 生产（standard）模式：空 RunMode 即非 simple，余额/计费链路全开。
	cfg := &config.Config{}
	cfg.Gateway.Scheduling.StickySessionWaitTimeout = 20 * time.Millisecond
	cfg.Gateway.Scheduling.StickySessionMaxWaiting = 3
	accounts := make([]service.Account, 1)
	accounts[0] = service.Account{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true, Concurrency: 50, Priority: 0,
		GroupIDs: []int64{24},
		Credentials: map[string]any{
			"api_key":             "test-key",
			"base_url":            "https://ark.cn-beijing.volces.com/api/v3",
			"openai_capabilities": []string{"seedance"},
		}}
	slots := &grokMediaSlotsCache{accounts: map[string]int64{}, users: map[string]int64{}}
	concurrency := service.NewConcurrencyService(slots)
	bindings := &grokMediaSlotBindings{owner: 1}
	upstream := &grokMediaSlotUpstream{call: func(*http.Request, int64) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{"id":"task-ark","status":"queued"}`))}, nil
	}}
	repo := grokMediaSlotRepo{openAIImagesFailoverAccountRepo: openAIImagesFailoverAccountRepo{accounts: accounts}}
	provider := service.NewGrokTokenProvider(repo, nil)
	gateway := service.NewOpenAIGatewayService(repo, nil, billingRepo, nil, nil, nil, bindings, cfg, nil, concurrency,
		service.NewBillingService(cfg, nil), nil, nil, upstream, nil, nil, provider, nil, nil, nil, nil, nil)
	groupID := int64(24)
	require.NoError(t, gateway.BindGrokMediaVideoRequestAccount(context.Background(), &groupID, "task", 10, 20, 1))
	billing := service.NewBillingCacheService(nil, &grokSeedanceClaimLeakUserRepo{balance: 100}, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	handler := NewOpenAIGatewayHandler(gateway, concurrency, billing, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
	return handler, bindings, upstream
}

// TestSeedanceBillingClaimReleasedOnRecordUsageFailure 复现：RecordUsage 失败后
// claim 必须释放，下一次 status poll 才能重试计费；否则首笔计费失败后 48h 内
// 的所有 poll 都不再计费（claim 挂满 TTL，计费永久丢失）。
func TestSeedanceBillingClaimReleasedOnRecordUsageFailure(t *testing.T) {
	billingRepo := &grokSeedanceClaimLeakBillingRepo{applyErr: errors.New("billing db down")}
	// 生产模式（非 simple）：RecordUsage 才会走到 applyUsageBilling → Apply。
	h, bindings, upstream := newGrokSeedanceClaimLeakHandler(t, billingRepo)

	upstream.call = func(req *http.Request, _ int64) (*http.Response, error) {
		body := `{"id":"task-ark","status":"queued"}`
		if req.Method == http.MethodGet {
			body = `{"id":"task-ark","status":"succeeded","usage":{"completion_tokens":12345}}`
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
	newContext := func(method string) (*gin.Context, *httptest.ResponseRecorder) {
		c, w := grokMediaSlotContext(context.Background(), method == http.MethodPost)
		key, ok := middleware2.GetAPIKeyFromContext(c)
		require.True(t, ok)
		key.Group.Platform = service.PlatformOpenAI
		body := ""
		if method == http.MethodPost {
			body = `{"model":"doubao-seedance","content":[{"type":"text","text":"waves"}]}`
		}
		c.Request = httptest.NewRequest(method, "/api/v3/contents/generations/tasks", strings.NewReader(body))
		c.Params = gin.Params{{Key: "task_id", Value: "task-ark"}}
		return c, w
	}

	// create → pending 快照落地。
	c, w := newContext(http.MethodPost)
	h.SeedanceTasks(c)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, bindings.pending, 1, "create 必须落 pending 快照")

	// 第一次完成 poll：claim 成功，RecordUsage 因计费 DB 故障失败。
	c, w = newContext(http.MethodGet)
	h.SeedanceTasks(c)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, 1, billingRepo.applies, "第一次完成 poll 必须尝试计费（Apply 一次）")

	// 第二次 poll（用户几秒后的自然重试行为）：计费 DB 已恢复，必须能重试计费。
	// 修复前：claim 挂着（48h TTL）→ ClaimGrokVideoBilling 返回 false →
	// prepareSeedanceCompletionBilling 返回 nil → 不再计费 →
	// 首笔失败后 48h 内计费永久丢失。
	billingRepo.applyErr = nil // 计费 DB 恢复
	c, w = newContext(http.MethodGet)
	h.SeedanceTasks(c)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, 2, billingRepo.applies,
		"DEFECT REPRODUCED: RecordUsage 失败后 claim 必须释放，计费恢复后的下一次 poll 必须重试计费（否则失败被 claim 永久吞掉，48h 内无重试入口）")
}
