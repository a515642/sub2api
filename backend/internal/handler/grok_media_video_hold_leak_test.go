//go:build unit

package handler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// ===== 复现审计缺陷 D1/D2/D4：视频 create 预冻结余额泄漏簇 =====
//
// 共同链路（Grok 异步视频 create，handler/grok_media.go）：
//  1. isGrokVideoCreateEndpoint && !Seedance → ReserveVideoTaskBalance 冻结余额；
//  2. 以下任一情形发生后，队列里没有该任务的条目：
//     - D2a: 上游 200 但响应体无 request_id → result.ResponseID == ""
//       → "ResponseID != ''" 的 store 分支整体跳过；
//     - D2b: StoreVideoTaskPendingBilling 两次尝试都失败（Redis 故障）；
//     - D1:  forward 期间 panic → recoverResponsesPanic 兜底返回 500，
//       但 videoHold 是局部变量，恢复路径无人释放；
//     - D4:  客户端断开 → requestCtx 取消 → forward 失败分支的 release
//       用已取消的 requestCtx 调仓储（BeginTx(ctx) 随之失败）→ 释放失败。
//  3. reconciler 永远看不到该任务 → 冻结余额永久泄漏
//    （比无限重排更糟：没有队列 TTL 兜底，也没有任何重试入口）。
//
// 对照：forward 失败分支（err != nil && videoHold != nil）已有 release 兜底，
// 说明这些泄漏是遗漏而非设计。

// grokVideoHoldLeakBillingRepo 计数 reserve/release，两者差值即泄漏。
// ctx 感知：模拟 Postgres BeginTx(ctx) 对已取消 ctx 返回 context.Canceled
// 的语义（客户端断开时真实仓储必然如此）。
type grokVideoHoldLeakBillingRepo struct {
	mu       sync.Mutex
	reserved int
	released int
}

func (r *grokVideoHoldLeakBillingRepo) Apply(context.Context, *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	return &service.UsageBillingApplyResult{Applied: true}, nil
}
func (r *grokVideoHoldLeakBillingRepo) ReserveBatchImageBalance(ctx context.Context, _ *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reserved++
	return &service.BatchImageBalanceHoldResult{Applied: true}, nil
}
func (r *grokVideoHoldLeakBillingRepo) CaptureBatchImageBalance(context.Context, *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	return &service.BatchImageBalanceHoldResult{Applied: true}, nil
}
func (r *grokVideoHoldLeakBillingRepo) ReleaseBatchImageBalance(ctx context.Context, _ *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released++
	return &service.BatchImageBalanceHoldResult{Applied: true}, nil
}

var _ service.UsageBillingRepository = (*grokVideoHoldLeakBillingRepo)(nil)

func (r *grokVideoHoldLeakBillingRepo) counts() (reserved, released int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reserved, r.released
}

// newGrokVideoHoldLeakHandler 组装与 newGrokMediaSlotHandler 相同的链路，
// 但注入计数型计费仓储（构造后无法替换，须在构造时传入）。
// platforms 非空时按 newGrokMediaSlotHandler 的变体语义配置 Seedance 账号
// （ark base_url + seedance capability）。
func newGrokVideoHoldLeakHandler(t *testing.T, billingRepo service.UsageBillingRepository, platforms ...string) (*OpenAIGatewayHandler, *grokMediaSlotBindings, *grokMediaSlotUpstream) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	accounts := make([]service.Account, 1)
	accounts[0] = service.Account{ID: 1, Platform: service.PlatformGrok, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true, Concurrency: 50, Priority: 0,
		GroupIDs: []int64{24}, Credentials: map[string]any{"api_key": "test-key", "access_token": "test-token"}}
	if len(platforms) > 0 {
		accounts[0].Platform = platforms[0]
		accounts[0].Credentials["base_url"] = "https://ark.cn-beijing.volces.com/api/v3"
		accounts[0].Credentials["openai_capabilities"] = []string{"seedance"}
	}
	slots := &grokMediaSlotsCache{accounts: map[string]int64{}, users: map[string]int64{}}
	concurrency := service.NewConcurrencyService(slots)
	bindings := &grokMediaSlotBindings{owner: 1}
	upstream := &grokMediaSlotUpstream{call: func(*http.Request, int64) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{"request_id":"task","status":"pending"}`))}, nil
	}}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Gateway.Scheduling.StickySessionWaitTimeout = 20 * time.Millisecond
	cfg.Gateway.Scheduling.StickySessionMaxWaiting = 3
	repo := grokMediaSlotRepo{openAIImagesFailoverAccountRepo: openAIImagesFailoverAccountRepo{accounts: accounts}}
	provider := service.NewGrokTokenProvider(repo, nil)
	// billingService=nil 时视频计价返回 0（reserve 静默跳过），必须提供真实计价器
	// 才能驱动 reserve/release 链路（与 service 层 repro 的做法一致）。
	gateway := service.NewOpenAIGatewayService(repo, nil, billingRepo, nil, nil, nil, bindings, cfg, nil, concurrency,
		service.NewBillingService(cfg, nil), nil, nil, upstream, nil, nil, provider, nil, nil, nil, nil, nil)
	groupID := int64(24)
	require.NoError(t, gateway.BindGrokMediaVideoRequestAccount(context.Background(), &groupID, "task", 10, 20, 1))
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	handler := NewOpenAIGatewayHandler(gateway, concurrency, billing, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
	return handler, bindings, upstream
}

// grokVideoHoldLeakContext 与 grokMediaSlotContext 相同，但分组带非零计费倍率
// （RateMultiplier=1；共享构造器里倍率为零值 0，会把视频计价打成 0 → reserve
// 静默跳过，无法驱动 reserve/release 链路）。
func grokVideoHoldLeakContext(t *testing.T, ctx context.Context) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	c, w := grokMediaSlotContext(ctx, true)
	key, ok := middleware2.GetAPIKeyFromContext(c)
	require.True(t, ok)
	key.Group.RateMultiplier = 1
	return c, w
}

// TestGrokVideoCreateEmptyResponseIDReleasesHold 复现 D2a：上游 200 但响应体无
// request_id → ResponseID == "" → store 分支整体跳过 → 已冻结余额无人释放。
func TestGrokVideoCreateEmptyResponseIDReleasesHold(t *testing.T) {
	billingRepo := &grokVideoHoldLeakBillingRepo{}
	h, bindings, upstream := newGrokVideoHoldLeakHandler(t, billingRepo)

	// 上游 200 但响应体不含任何 request_id/id 字段 → result.ResponseID == ""。
	upstream.call = func(*http.Request, int64) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{"status":"pending"}`))}, nil
	}

	c, w := grokVideoHoldLeakContext(t, context.Background())
	h.GrokVideoGeneration(c)

	require.Equal(t, http.StatusOK, w.Code, "上游成功时 create 应 200，body=%s", w.Body.String())

	reserved, released := billingRepo.counts()
	require.Equal(t, 1, reserved, "视频 create 必须预冻结余额")
	require.Zero(t, bindings.pending, "响应无 request_id 时不会产生 pending 快照")

	// DEFECT 断言（修复前 reserved=1、released=0 → hold 泄漏）。
	require.Equal(t, reserved, released,
		"DEFECT REPRODUCED: create 成功但响应无 request_id 时，已冻结余额必须释放（store 分支被跳过导致 reconciler 永远看不到该任务，冻结余额永久泄漏）")
}

// TestGrokVideoCreateStoreFailureReleasesHold 复现 D2b：create 成功、ResponseID
// 非空，但 StoreVideoTaskPendingBilling 两次尝试都失败（Redis 故障）→
// 无队列条目 → reconciler 永远看不到该任务 → 已冻结余额无人释放。
func TestGrokVideoCreateStoreFailureReleasesHold(t *testing.T) {
	billingRepo := &grokVideoHoldLeakBillingRepo{}
	h, bindings, _ := newGrokVideoHoldLeakHandler(t, billingRepo)

	// SetGrokVideoPendingBilling 永远失败（模拟 Redis 持续故障）。
	bindings.setErr = errors.New("redis unavailable")

	c, w := grokVideoHoldLeakContext(t, context.Background())
	h.GrokVideoGeneration(c)

	require.Equal(t, http.StatusOK, w.Code, "上游成功时 create 应 200，body=%s", w.Body.String())

	reserved, released := billingRepo.counts()
	require.Equal(t, 1, reserved, "视频 create 必须预冻结余额")
	require.Zero(t, bindings.pending, "store 两次失败时不得残留 pending 快照")

	// DEFECT 断言（修复前 reserved=1、released=0 → hold 泄漏）。
	require.Equal(t, reserved, released,
		"DEFECT REPRODUCED: pending 快照两次落库失败时，已冻结余额必须释放（无队列条目 → reconciler 永远看不到该任务，冻结余额永久泄漏）")
}

// TestGrokVideoCreateClientGoneReleasesHold 复现 D4：客户端在 forward 期间断开
// → requestCtx 取消 → forward 失败分支的 release 用已取消的 requestCtx 调仓储
// （BeginTx(ctx) 随之失败）→ 已冻结余额无人释放。release 必须脱离客户端 ctx。
func TestGrokVideoCreateClientGoneReleasesHold(t *testing.T) {
	billingRepo := &grokVideoHoldLeakBillingRepo{}
	h, _, upstream := newGrokVideoHoldLeakHandler(t, billingRepo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upstream.call = func(*http.Request, int64) (*http.Response, error) {
		cancel() // 客户端在上游返回前断开
		return nil, context.Canceled
	}

	c, _ := grokVideoHoldLeakContext(t, ctx)
	h.GrokVideoGeneration(c)

	reserved, released := billingRepo.counts()
	require.Equal(t, 1, reserved, "视频 create 必须预冻结余额")

	// DEFECT 断言（修复前 released=0：release 用的 requestCtx 已取消）。
	require.Equal(t, reserved, released,
		"DEFECT REPRODUCED: 客户端断开后 release 不得使用已取消的 requestCtx（BeginTx 随 ctx 取消而失败，冻结余额永久泄漏）；必须在脱离客户端生命周期的 ctx 上释放")
}

// TestGrokVideoCreatePanicReleasesHold 复现 D1：reserve 成功后 forward 期间 panic
// → recoverResponsesPanic 兜底 500，但 videoHold 是局部变量，恢复路径无人释放
// → 已冻结余额无人释放。
func TestGrokVideoCreatePanicReleasesHold(t *testing.T) {
	billingRepo := &grokVideoHoldLeakBillingRepo{}
	h, _, upstream := newGrokVideoHoldLeakHandler(t, billingRepo)

	upstream.call = func(*http.Request, int64) (*http.Response, error) {
		panic("test upstream panic")
	}

	c, w := grokVideoHoldLeakContext(t, context.Background())
	h.GrokVideoGeneration(c)
	require.NotNil(t, w, "panic 应被兜底恢复而非崩溃测试进程")

	reserved, released := billingRepo.counts()
	require.Equal(t, 1, reserved, "视频 create 必须预冻结余额")

	// DEFECT 断言（修复前 released=0：panic 恢复路径不释放 hold）。
	require.Equal(t, reserved, released,
		"DEFECT REPRODUCED: forward 期间 panic 被兜底恢复后，已冻结余额必须释放（恢复路径无人持有 videoHold，冻结余额永久泄漏）")
}

// TestGrokVideoCreateSuccessKeepsHoldForReconciler 对照组：正常路径
// （上游 200 带 request_id、store 成功）hold 移交后台对账器，函数退出时
// 兜底释放表必须为空——不得把已移交的 hold 再释放一次（对账器随后 capture）。
func TestGrokVideoCreateSuccessKeepsHoldForReconciler(t *testing.T) {
	billingRepo := &grokVideoHoldLeakBillingRepo{}
	h, bindings, _ := newGrokVideoHoldLeakHandler(t, billingRepo)

	c, w := grokVideoHoldLeakContext(t, context.Background())
	h.GrokVideoGeneration(c)

	require.Equal(t, http.StatusOK, w.Code, "上游成功时 create 应 200，body=%s", w.Body.String())

	reserved, released := billingRepo.counts()
	require.Equal(t, 1, reserved, "视频 create 必须预冻结余额")
	require.Len(t, bindings.pending, 1, "store 成功必须产生 pending 快照")

	// 对照断言：正常路径 hold 已移交对账器，不得释放。
	require.Equal(t, 0, released,
		"正常路径 hold 已移交后台对账器，函数退出时不得释放（对账器负责后续 capture/release）")
}
