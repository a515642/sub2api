//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

// ===== 复现审计缺陷：视频结算失败无限重排 + 冻结余额永不释放 =====
//
// 场景（与 0.2.7 合并后的生产链路一致）：
//  1. create 时按请求体默认 480p/8s 预留 hold（grok-imagine-video-1.5 → 0.08×8 = $0.64）；
//  2. 上游完成体返回 720p/15s（完成元数据可高于 create 快照）；
//  3. reconciler 重定价 actual = 0.14×15 = $2.10 > hold $0.64
//     → CaptureBatchImageBalance 返回 ErrBatchImageSettlementCostExceedsHold（永久错误）；
//  4. 当前实现每个失败分支都 15s 重排、无重试上限、无终态释放
//     → 任务每 15s 重试一次直到 30 天 TTL 过期，冻结余额永不释放。
//
// 对比基准：batch_image_settlement.go:114-116 有 isBatchImageSettlementRetryExhausted
// 终态出口（重试 5 次后 failExhaustedSettlement 释放冻结余额）。

// --- 测试桩 ---

type videoBillingQueueStub struct {
	mu          sync.Mutex
	entries     map[string][]byte
	requeueKeys []string
	setCalls    int
	setFailures int // >0 时前 N 次 Set 失败（模拟 Redis 瞬时抖动）
	deletedKeys []string
}

func (q *videoBillingQueueStub) SetVideoTaskBilling(_ context.Context, key string, payload []byte, _ time.Time, _ time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.setCalls++
	if q.setFailures > 0 {
		q.setFailures--
		return errors.New("redis transient failure")
	}
	q.entries[key] = payload
	q.requeueKeys = append(q.requeueKeys, key)
	return nil
}

// ClaimDueVideoTask 模拟生产 Lua 脚本语义：ZREM 出队 + GET payload 原子返回。
// 出队后条目即从 zset 消失——这正是 requeue 失败导致任务丢失的窗口。
func (q *videoBillingQueueStub) ClaimDueVideoTask(_ context.Context, _ time.Time) (string, []byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for key, payload := range q.entries {
		delete(q.entries, key) // ZREM
		return key, payload, nil
	}
	return "", nil, nil
}

// --- GatewayCache 其余方法（桩不参与本链路） ---

func (q *videoBillingQueueStub) GetSessionAccountID(_ context.Context, _ int64, _ string) (int64, error) {
	return 0, errors.New("not found")
}
func (q *videoBillingQueueStub) SetSessionAccountID(_ context.Context, _ int64, _ string, _ int64, _ time.Duration) error {
	return nil
}
func (q *videoBillingQueueStub) RefreshSessionTTL(_ context.Context, _ int64, _ string, _ time.Duration) error {
	return nil
}
func (q *videoBillingQueueStub) DeleteSessionAccountID(_ context.Context, _ int64, _ string) error {
	return nil
}
func (q *videoBillingQueueStub) SetGrokVideoPendingBilling(_ context.Context, _ string, _ []byte, _ time.Duration) error {
	return nil
}
func (q *videoBillingQueueStub) GetGrokVideoPendingBilling(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}
func (q *videoBillingQueueStub) ClaimGrokVideoBilled(_ context.Context, _ string, _ time.Duration) (bool, error) {
	return true, nil
}
func (q *videoBillingQueueStub) ReleaseGrokVideoBilled(_ context.Context, _ string) error {
	return nil
}
func (q *videoBillingQueueStub) SetReasoningContent(_ context.Context, _ string, _ string, _ time.Duration) error {
	return nil
}
func (q *videoBillingQueueStub) GetReasoningContent(_ context.Context, _ string) (string, error) {
	return "", ErrReasoningContentNotFound
}

func (q *videoBillingQueueStub) DeleteVideoTaskBilling(_ context.Context, key string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.entries, key)
	q.deletedKeys = append(q.deletedKeys, key)
	return nil
}

type videoBillingRepoStub struct {
	UsageBillingRepository
	mu            sync.Mutex
	released      int
	captured      int
	releaseErr    error
	applyErr      error // RecordUsage → applyUsageBilling → Apply 的失败注入
	captureClaims map[string]string
	captureErrFor func(cmd *BatchImageBalanceHoldCommand) error
}

func (r *videoBillingRepoStub) ReserveBatchImageBalance(_ context.Context, _ *BatchImageBalanceHoldCommand) (*BatchImageBalanceHoldResult, error) {
	return &BatchImageBalanceHoldResult{}, nil
}

func (r *videoBillingRepoStub) CaptureBatchImageBalance(ctx context.Context, cmd *BatchImageBalanceHoldCommand) (*BatchImageBalanceHoldResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.captureErrFor != nil {
		if err := r.captureErrFor(cmd); err != nil {
			return nil, err
		}
	}
	// 模拟生产 dedup 语义：同一 capture request_id 幂等；不同指纹
	//（轮间 actual 漂移）报 ErrUsageBillingRequestConflict。
	if r.captureClaims == nil {
		r.captureClaims = map[string]string{}
	}
	if existing, ok := r.captureClaims[cmd.RequestID]; ok {
		if existing != cmd.RequestFingerprint {
			return nil, ErrUsageBillingRequestConflict
		}
		return &BatchImageBalanceHoldResult{}, nil
	}
	r.captureClaims[cmd.RequestID] = cmd.RequestFingerprint
	r.captured++
	return &BatchImageBalanceHoldResult{}, nil
}

func (r *videoBillingRepoStub) ReleaseBatchImageBalance(_ context.Context, _ *BatchImageBalanceHoldCommand) (*BatchImageBalanceHoldResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.releaseErr != nil {
		return nil, r.releaseErr
	}
	r.released++
	return &BatchImageBalanceHoldResult{}, nil
}

// Apply 满足 RecordUsage → applyUsageBilling → repo.Apply 的成功结算路径；
// applyErr 非空时注入失败（RecordUsage 持久失败场景）。
func (r *videoBillingRepoStub) Apply(_ context.Context, _ *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.applyErr != nil {
		return nil, r.applyErr
	}
	return &UsageBillingApplyResult{Applied: true}, nil
}

type videoBillingAccountRepoStub struct {
	AccountRepository
	account *Account
	err     error
}

func (s *videoBillingAccountRepoStub) GetByID(_ context.Context, _ int64) (*Account, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.account, nil
}

type videoBillingHTTPUpstream struct {
	mu    sync.Mutex
	calls int
	body  string
}

func (u *videoBillingHTTPUpstream) Do(_ *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls++
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(u.body)),
	}, nil
}

func (u *videoBillingHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxyURL, accountID, concurrency)
}

func newVideoBillingReconcilerTestService(t *testing.T, captureErr error) (*OpenAIGatewayService, *videoBillingQueueStub, *videoBillingRepoStub) {
	t.Helper()

	queue := &videoBillingQueueStub{entries: map[string][]byte{}}
	billingRepo := &videoBillingRepoStub{captureErrFor: func(cmd *BatchImageBalanceHoldCommand) error {
		if cmd.ActualAmount-cmd.HoldAmount > 0.00000001 {
			return ErrBatchImageSettlementCostExceedsHold
		}
		return captureErr
	}}
	// 官方 Grok 状态体：done + video.url + video.duration=15（完成时长高于
	// create 快照的 8s → actual > hold → 永久 capture 失败）。
	upstream := &videoBillingHTTPUpstream{body: `{"status":"done","model":"grok-imagine-video-1.5","video":{"duration":15,"url":"https://video.example/v.mp4"}}`}
	account := &Account{
		ID:       7,
		Platform: PlatformGrok,
		Type:     AccountTypeAPIKey,
		Status:   StatusActive,
		Credentials: map[string]any{
			"api_key":         "sk-test",
			"xai_base_url":    "https://api.x.ai",
			"grok_media_base": "https://api.x.ai",
		},
	}
	svc := &OpenAIGatewayService{
		cache:            queue,
		accountRepo:      &videoBillingAccountRepoStub{account: account},
		usageBillingRepo: billingRepo,
		httpUpstream:     upstream,
		cfg:              &config.Config{},
		billingService:   NewBillingService(&config.Config{}, nil),
		deferredService:  NewDeferredService(nil, nil, time.Hour),
	}
	return svc, queue, billingRepo
}

func newVideoBillingTestAPIKeyService(t *testing.T, svc *OpenAIGatewayService, apiKey *APIKey) *APIKeyService {
	t.Helper()
	apiKeyService := &APIKeyService{}
	apiKeyService.apiKeyRepo = &videoBillingAPIKeyRepoStub{apiKey: apiKey}
	svc.SetVideoTaskAPIKeyService(apiKeyService)
	return apiKeyService
}

type videoBillingAPIKeyRepoStub struct {
	APIKeyRepository
	apiKey *APIKey
}

func (r *videoBillingAPIKeyRepoStub) GetByID(_ context.Context, _ int64) (*APIKey, error) {
	return r.apiKey, nil
}

// runReconcileOnce drives reconcileVideoTask once with the given payload.
func runReconcileOnce(svc *OpenAIGatewayService, queue *videoBillingQueueStub, key string, pending GrokVideoPendingBilling) {
	payload, err := json.Marshal(pending)
	if err != nil {
		panic(err)
	}
	queue.entries[key] = payload
	svc.reconcileVideoTask(context.Background(), key, payload)
}

// runReconcileRounds drives reconcileVideoTask N times, each round picking up
// the queue's current payload so retry counters persist across rounds.
func runReconcileRounds(svc *OpenAIGatewayService, queue *videoBillingQueueStub, key string, pending GrokVideoPendingBilling, rounds int) GrokVideoPendingBilling {
	for range rounds {
		payload, err := json.Marshal(pending)
		if err != nil {
			panic(err)
		}
		queue.entries[key] = payload
		svc.reconcileVideoTask(context.Background(), key, payload)
		if updated := queue.entries[key]; len(updated) > 0 {
			if err := json.Unmarshal(updated, &pending); err != nil {
				panic(err)
			}
		}
	}
	return pending
}

// TestReconcileVideoTaskCostExceedsHoldRetriesUntilExhausted 复现并锁定核心缺陷的修复：
// actual > hold 是永久错误（ErrBatchImageSettlementCostExceedsHold），
// 修复前每 15s 无限重排且永不释放（复现断言：5 轮全重排、0 删除、0 释放）；
// 修复后按 videoTaskSettlementMaxRetries 计数，第 5 次失败即终态：
// 释放冻结余额并删除队列条目。
func TestReconcileVideoTaskCostExceedsHoldRetriesUntilExhausted(t *testing.T) {
	svc, queue, billingRepo := newVideoBillingReconcilerTestService(t, nil)

	group := &Group{ID: 3, Platform: PlatformGrok, RateMultiplier: 1}
	groupID := group.ID
	apiKey := &APIKey{ID: 11, UserID: 5, GroupID: &groupID, Group: group, User: &User{ID: 5}}
	newVideoBillingTestAPIKeyService(t, svc, apiKey)

	// create 快照：480p/8s → hold = 0.08×8 = 0.64。
	pending := GrokVideoPendingBilling{
		RequestID:            "task-cost-exceeds",
		UserID:               5,
		APIKeyID:             11,
		AccountID:            7,
		Platform:             VideoTaskPlatformGrok,
		Model:                "grok-imagine-video-1.5",
		BillingModel:         "grok-imagine-video-1.5",
		VideoResolution:      VideoBillingResolution480P,
		VideoDurationSeconds: 8,
		CreatedAt:            time.Now().UTC().Format(time.RFC3339Nano),
		HoldID:               NewVideoTaskHoldID(),
		HoldAmount:           0.64,
	}
	key := "grok_video_pending:" + pending.RequestID

	// 连续跑 5 轮 reconcile（重试上限），每轮取回队列 payload 携带递增计数。
	final := runReconcileRounds(svc, queue, key, pending, videoTaskSettlementMaxRetries)

	queue.mu.Lock()
	requeues := len(queue.requeueKeys)
	deletions := len(queue.deletedKeys)
	queue.mu.Unlock()

	// 修复后语义：前 4 次失败重排计数，第 5 次达到上限立即终态。
	require.Equal(t, videoTaskSettlementMaxRetries-1, requeues,
		"前 N-1 次失败应重排携带递增的 retry_count")
	require.Equal(t, 1, deletions,
		"达到重试上限后必须删除队列条目（终态）")
	require.Equal(t, 1, billingRepo.released,
		"达到重试上限后必须释放冻结余额（终态出口）")
	// 终态那轮（第 5 次）删除条目不重排，队列中最后可见的计数是第 4 轮的。
	require.Equal(t, videoTaskSettlementMaxRetries-1, final.RetryCount,
		"重试计数必须按失败次数递增（而非停留在 1）")
	_ = fmt.Sprint() // keep fmt import when assertions change
}

// TestExhaustedVideoTaskReleaseFailureRequeuesNotDrops 锁定终态出口的兜底语义：
// 已耗尽任务在释放失败（瞬时 DB 故障）时必须重排等待下一轮重试释放，
// 绝不能把任务从队列静默丢弃——否则冻结余额永远无法释放。
func TestExhaustedVideoTaskReleaseFailureRequeuesNotDrops(t *testing.T) {
	svc, queue, billingRepo := newVideoBillingReconcilerTestService(t, nil)

	group := &Group{ID: 3, Platform: PlatformGrok, RateMultiplier: 1}
	groupID := group.ID
	apiKey := &APIKey{ID: 11, UserID: 5, GroupID: &groupID, Group: group, User: &User{ID: 5}}
	newVideoBillingTestAPIKeyService(t, svc, apiKey)

	pending := GrokVideoPendingBilling{
		RequestID:            "task-exhausted-release-fails",
		UserID:               5,
		APIKeyID:             11,
		AccountID:            7,
		Platform:             VideoTaskPlatformGrok,
		Model:                "grok-imagine-video-1.5",
		BillingModel:         "grok-imagine-video-1.5",
		VideoResolution:      VideoBillingResolution480P,
		VideoDurationSeconds: 8,
		CreatedAt:            time.Now().UTC().Format(time.RFC3339Nano),
		HoldID:               NewVideoTaskHoldID(),
		HoldAmount:           0.64,
		// 已达耗尽上限（上一轮 reconcile 记录后重排的状态）。
		RetryCount:    videoTaskSettlementMaxRetries,
		LastErrorCode: "VIDEO_SETTLEMENT_CAPTURE_FAILED",
	}
	key := "grok_video_pending:" + pending.RequestID

	// 第一轮：释放失败 → 必须重排（不丢弃）。
	billingRepo.releaseErr = errors.New("transient db failure")
	runReconcileOnce(svc, queue, key, pending)
	queue.mu.Lock()
	require.Equal(t, 1, len(queue.requeueKeys), "释放失败的耗尽任务必须重排等待重试释放")
	require.Len(t, queue.deletedKeys, 0, "释放失败时不得删除队列条目")
	queue.mu.Unlock()

	// 第二轮：释放成功 → 终态（释放 + 删除）。
	billingRepo.releaseErr = nil
	runReconcileOnce(svc, queue, key, pending)
	queue.mu.Lock()
	require.Len(t, queue.deletedKeys, 1, "释放成功后必须删除队列条目")
	queue.mu.Unlock()
	require.Equal(t, 1, billingRepo.released, "仅第二轮释放成功（第一轮失败不计数）")
}

// TestReconcileVideoTaskRequeueFailureDoesNotLoseTask 复现 D6：
// ClaimDueVideoTask 的 Lua 是 ZREM+GET 原子出队——claim 后进程在 requeue 前
// 失败/崩溃时条目已从 zset 移除，requeue 的 Set 失败（Redis 抖动）会让任务
// 永久丢失：队列无条目、reconciler 永远看不到、Postgres 侧 hold 永久冻结
// （无 TTL 兜底、无重试入口）。requeue 失败时必须重试 Set 而不是只打日志。
func TestReconcileVideoTaskRequeueFailureDoesNotLoseTask(t *testing.T) {
	svc, queue, billingRepo := newVideoBillingReconcilerTestService(t, nil)

	group := &Group{ID: 3, Platform: PlatformGrok, RateMultiplier: 1}
	groupID := group.ID
	apiKey := &APIKey{ID: 11, UserID: 5, GroupID: &groupID, Group: group, User: &User{ID: 5}}
	newVideoBillingTestAPIKeyService(t, svc, apiKey)

	// 触发一个会重排的瞬时失败路径：账号不可用（GetByID 报错）。
	svc.accountRepo = &videoBillingAccountRepoStub{err: errors.New("db down")}

	pending := GrokVideoPendingBilling{
		RequestID:            "task-requeue-fails",
		UserID:               5,
		APIKeyID:             11,
		AccountID:            7,
		Platform:             VideoTaskPlatformGrok,
		Model:                "grok-imagine-video-1.5",
		BillingModel:         "grok-imagine-video-1.5",
		VideoResolution:      VideoBillingResolution480P,
		VideoDurationSeconds: 8,
		CreatedAt:            time.Now().UTC().Format(time.RFC3339Nano),
		HoldID:               NewVideoTaskHoldID(),
		HoldAmount:           0.64,
	}
	key := "grok_video_pending:" + pending.RequestID
	payload, err := json.Marshal(pending)
	require.NoError(t, err)
	queue.entries[key] = payload

	// 第一次 Set（requeue）失败（Redis 抖动）。
	queue.setFailures = 1
	// 模拟 reconciler 主循环的一轮：claim（ZREM 出队）→ reconcile。
	claimedKey, claimedPayload, err := queue.ClaimDueVideoTask(context.Background(), time.Now())
	require.NoError(t, err)
	require.Equal(t, key, claimedKey)
	svc.reconcileVideoTask(context.Background(), claimedKey, claimedPayload)

	// 下一轮 claim 必须仍能取到该任务：claim 出队后 requeue 的 Set 失败
	// 时任务不得永久丢失（真实语义：zset 无条目、reconciler 永远看不到、
	// Postgres 侧 hold 永久冻结，无 TTL 兜底、无重试入口）。
	reclaimedKey, reclaimedPayload, err := queue.ClaimDueVideoTask(context.Background(), time.Now())
	require.NoError(t, err)
	require.Equal(t, key, reclaimedKey, "DEFECT REPRODUCED: requeue 失败后任务不得从队列永久丢失（hold 永久冻结）")
	require.NotEmpty(t, reclaimedPayload)
	require.Equal(t, 0, billingRepo.released, "任务未终态，不得释放")
}

// TestReconcileVideoTaskSettledThenRecordFailureMustNotReleaseHold 复现
// 审计缺陷（Agent 3 Defect 2 的危害半）：capture 成功后 RecordUsage 持久失败
// → 5 轮重试耗尽 → 终态出口对"已被 capture 消费"的 hold 执行 release：
//  1. 用户有其它并发视频 hold 时：frozen_balance 是共享池，release 从
//     其它任务的冻结资金里凭空退款（资金错乱，实际是双倍退款——
//     capture 已把金额记为已消费，release 又全额退回余额）；
//  2. 无其它 hold 时：frozen insufficient → release 永久失败无限重排
//     （与 D6 修复前同型：无终态出口）。
//
// capture 与 release 使用不同的 dedup request_id（capture:<holdID> vs
// release:<holdID>），release 的 claim 不会被 capture 的 dedup 挡住——
// 幂等层防不住"已结算再释放"。正确语义：capture 已成功（金额已收）的
// 任务耗尽后只删除队列条目（放弃补 usage log），绝不 release。
func TestReconcileVideoTaskSettledThenRecordFailureMustNotReleaseHold(t *testing.T) {
	svc, queue, billingRepo := newVideoBillingReconcilerTestService(t, nil)

	group := &Group{ID: 3, Platform: PlatformGrok, RateMultiplier: 1}
	groupID := group.ID
	apiKey := &APIKey{ID: 11, UserID: 5, GroupID: &groupID, Group: group, User: &User{ID: 5}}
	newVideoBillingTestAPIKeyService(t, svc, apiKey)

	// 完成元数据与快照一致 → capture 必成功（actual = hold）。
	svc.httpUpstream.(*videoBillingHTTPUpstream).body = `{"status":"done","model":"grok-imagine-video-1.5","video":{"duration":8,"url":"https://video.example/v.mp4"}}`

	pending := GrokVideoPendingBilling{
		RequestID:            "task-settled-record-fails",
		UserID:               5,
		APIKeyID:             11,
		AccountID:            7,
		Platform:             VideoTaskPlatformGrok,
		Model:                "grok-imagine-video-1.5",
		BillingModel:         "grok-imagine-video-1.5",
		VideoResolution:      VideoBillingResolution480P,
		VideoDurationSeconds: 8,
		CreatedAt:            time.Now().UTC().Format(time.RFC3339Nano),
		HoldID:               NewVideoTaskHoldID(),
		HoldAmount:           0.64,
	}
	key := "grok_video_pending:" + pending.RequestID

	// RecordUsage 持久失败（usage log 写不进去，模拟 DB 故障）。
	billingRepo.applyErr = errors.New("usage log db down")

	// 连续跑 5 轮 reconcile：第 1 轮 capture 成功 + RecordUsage 失败，
	// 后续轮次重试 RecordUsage，第 5 轮达到耗尽上限走终态。
	final := runReconcileRounds(svc, queue, key, pending, videoTaskSettlementMaxRetries)

	queue.mu.Lock()
	deletions := len(queue.deletedKeys)
	queue.mu.Unlock()

	// capture 只发生一次（金额已收，重复轮次幂等命中 dedup）。
	require.Equal(t, 1, billingRepo.captured, "capture 应恰好成功一次（后续轮次幂等）")
	// DEFECT 断言：耗尽终态绝不能 release 已被 capture 消费的 hold。
	require.Equal(t, 0, billingRepo.released,
		"DEFECT REPRODUCED: capture 已成功（金额已结算）的任务耗尽后不得 release（frozen 共享池会被双倍退款；无其它 hold 时则 frozen 不足→永久重排）")
	require.Equal(t, 1, deletions, "耗尽终态必须删除队列条目（放弃补 usage log，不再无限重排）")
	_ = final
}

// TestReconcileVideoTaskCaptureConflictStillReleasesHold 对照分支：capture
// 从未成功（轮间定价漂移 → 指纹冲突被 dedup 挡住）而耗尽的任务，hold 未被
// 消费，终态必须照常 release（冻结余额退还用户）——上一用例的"不 release"
// 例外只适用于已结算任务，不能误伤未结算路径。
func TestReconcileVideoTaskCaptureConflictStillReleasesHold(t *testing.T) {
	svc, queue, billingRepo := newVideoBillingReconcilerTestService(t, nil)

	group := &Group{ID: 3, Platform: PlatformGrok, RateMultiplier: 1}
	groupID := group.ID
	apiKey := &APIKey{ID: 11, UserID: 5, GroupID: &groupID, Group: group, User: &User{ID: 5}}
	newVideoBillingTestAPIKeyService(t, svc, apiKey)

	pending := GrokVideoPendingBilling{
		RequestID:            "task-capture-conflict",
		UserID:               5,
		APIKeyID:             11,
		AccountID:            7,
		Platform:             VideoTaskPlatformGrok,
		Model:                "grok-imagine-video-1.5",
		BillingModel:         "grok-imagine-video-1.5",
		VideoResolution:      VideoBillingResolution480P,
		VideoDurationSeconds: 8,
		CreatedAt:            time.Now().UTC().Format(time.RFC3339Nano),
		HoldID:               NewVideoTaskHoldID(),
		HoldAmount:           0.64,
	}
	key := "grok_video_pending:" + pending.RequestID

	// 预置 dedup 指纹（模拟某一轮 capture 已以另一 actual 提交过），
	// 本轮 actual=0.64（duration 8s）→ 指纹必然不同 → 每轮 capture 都
	// ErrUsageBillingRequestConflict，5 轮耗尽。
	svc.httpUpstream.(*videoBillingHTTPUpstream).body = `{"status":"done","model":"grok-imagine-video-1.5","video":{"duration":8,"url":"https://video.example/v.mp4"}}`
	billingRepo.mu.Lock()
	billingRepo.captureClaims = map[string]string{BatchImageCaptureRequestID(pending.HoldID): "stale-fingerprint-from-earlier-round"}
	billingRepo.mu.Unlock()

	_ = runReconcileRounds(svc, queue, key, pending, videoTaskSettlementMaxRetries)

	queue.mu.Lock()
	deletions := len(queue.deletedKeys)
	queue.mu.Unlock()

	require.Zero(t, billingRepo.captured, "全程指纹冲突，capture 从未成功")
	// 对照断言：未结算任务的耗尽终态必须照常 release。
	require.Equal(t, 1, billingRepo.released,
		"capture 从未成功（指纹冲突）的任务耗尽后必须释放冻结余额（hold 未被消费，钱要退）")
	require.Equal(t, 1, deletions, "耗尽终态必须删除队列条目")
}

// TestReconcileVideoTaskSettlesWhenCompletionMatchesSnapshot 验证修复后的正常
// 结算路径：完成元数据与 create 快照一致（480p/8s）→ actual = hold → capture
// 成功、删除队列条目、不释放、不消耗重试计数。
func TestReconcileVideoTaskSettlesWhenCompletionMatchesSnapshot(t *testing.T) {
	svc, queue, billingRepo := newVideoBillingReconcilerTestService(t, nil)

	group := &Group{ID: 3, Platform: PlatformGrok, RateMultiplier: 1}
	groupID := group.ID
	apiKey := &APIKey{ID: 11, UserID: 5, GroupID: &groupID, Group: group, User: &User{ID: 5}}
	newVideoBillingTestAPIKeyService(t, svc, apiKey)

	// 对照组：状态体 duration 与 create 快照一致（8s）。
	svc.httpUpstream.(*videoBillingHTTPUpstream).body = `{"status":"done","model":"grok-imagine-video-1.5","video":{"duration":8,"url":"https://video.example/v.mp4"}}`

	pending := GrokVideoPendingBilling{
		RequestID:            "task-ok",
		UserID:               5,
		APIKeyID:             11,
		AccountID:            7,
		Platform:             VideoTaskPlatformGrok,
		Model:                "grok-imagine-video-1.5",
		BillingModel:         "grok-imagine-video-1.5",
		VideoResolution:      VideoBillingResolution480P,
		VideoDurationSeconds: 8,
		CreatedAt:            time.Now().UTC().Format(time.RFC3339Nano),
		HoldID:               NewVideoTaskHoldID(),
		HoldAmount:           0.64,
	}
	key := "grok_video_pending:" + pending.RequestID
	runReconcileOnce(svc, queue, key, pending)

	// 成功路径：完成元数据 480p/8s 与快照一致 → actual = hold → 正常结算。
	require.Equal(t, 1, billingRepo.captured, "正常结算路径必须 capture")
	require.Equal(t, 0, billingRepo.released)
	queue.mu.Lock()
	require.Len(t, queue.deletedKeys, 1, "成功结算后必须删除队列条目")
	queue.mu.Unlock()
}
