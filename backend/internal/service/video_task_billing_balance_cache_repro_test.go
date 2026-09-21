//go:build unit

package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// ===== 复现审计缺陷：视频结算把"已被 hold 消费"的金额再扣一次余额缓存 =====
//
// 生产时序（异步视频任务的 hold 结算链）：
//  1. create 预留 hold：DB balance -= hold，并失效余额缓存（reserveVideoTaskBalance）；
//  2. 生成期间同一用户的并发请求缓存未命中 → 回源 DB → 异步回填缓存（GetUserBalance）；
//  3. 结算：captureVideoTaskBalance 把 DB 余额改到最终值并再次失效缓存；
//     紧接着在 Apply 的 DB 往返窗口内，并发请求又一次未命中回源，把 capture
//     之后的正确余额回填进缓存；
//  4. RecordUsage → applyUsageBilling → repo.Apply：BalanceAlreadyReserved=true
//     使 buildUsageBillingCommand 不设置 cmd.BalanceCost —— 这笔钱在 Apply 之外
//     （capture）已经结清，Apply 没有再动 DB 余额；
//     但 finalizePostUsageBilling 仍无条件走 syncBalanceCacheAfterDeduction →
//     QueueDeductBalance(actual)，把已经正确的缓存余额又扣了一次。
//
// 结果：缓存余额比 DB 少一个视频单价。余额接近最低留存（MinimumBalanceReserve）
// 的用户会被 checkBalanceEligibility 误判余额不足而拒绝请求，直到缓存过期。
// 该入队在 BalanceAlreadyReserved 路径下没有任何对应事实：capture/release 已经
// 失效过缓存，键不存在时扣减是 no-op，键存在时扣的就是错的钱。

// videoBillingBalanceCacheStub 复刻生产 Redis 语义（repository/billing_cache.go
// 的 deductBalanceScript）：键不存在时扣减是 no-op（脚本返回 0，不会写负数）。
type videoBillingBalanceCacheStub struct {
	*billingCacheWorkerStub

	mu          sync.Mutex
	balances    map[int64]float64
	deductCalls int

	deductOnce sync.Once
	deducted   chan struct{}
}

// waitForDeduct 等待异步缓存写入池落盘一次扣减，返回是否在窗口内到达。
func (c *videoBillingBalanceCacheStub) waitForDeduct(window time.Duration) bool {
	c.mu.Lock()
	if c.deducted == nil {
		c.deducted = make(chan struct{})
	}
	deducted := c.deducted
	c.mu.Unlock()

	select {
	case <-deducted:
		return true
	case <-time.After(window):
		return false
	}
}

func (c *videoBillingBalanceCacheStub) GetUserBalance(_ context.Context, userID int64) (float64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	balance, ok := c.balances[userID]
	if !ok {
		return 0, errors.New("cache miss")
	}
	return balance, nil
}

func (c *videoBillingBalanceCacheStub) SetUserBalance(_ context.Context, userID int64, balance float64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.balances[userID] = balance
	return nil
}

func (c *videoBillingBalanceCacheStub) DeductUserBalance(_ context.Context, userID int64, amount float64) error {
	c.mu.Lock()
	c.deductCalls++
	if balance, ok := c.balances[userID]; ok {
		c.balances[userID] = balance - amount
	}
	if c.deducted == nil {
		c.deducted = make(chan struct{})
	}
	deducted := c.deducted
	c.mu.Unlock()

	c.deductOnce.Do(func() { close(deducted) })
	return nil
}

func (c *videoBillingBalanceCacheStub) InvalidateUserBalance(_ context.Context, userID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.balances, userID)
	return nil
}

func (c *videoBillingBalanceCacheStub) deductCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deductCalls
}

func (c *videoBillingBalanceCacheStub) balance(userID int64) (float64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	balance, ok := c.balances[userID]
	return balance, ok
}

// videoBillingConcurrentRefillRepoStub 在 Apply（usage log 的 DB 往返）期间
// 模拟同一用户的并发请求回填余额缓存——即"扣减落盘时缓存键存在且值正确"这一
// 生产窗口，避免用例依赖 worker 池的调度时序。
type videoBillingConcurrentRefillRepoStub struct {
	*videoBillingRepoStub

	applies int
	onApply func()
}

func (r *videoBillingConcurrentRefillRepoStub) Apply(ctx context.Context, cmd *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	r.applies++
	if r.onApply != nil {
		r.onApply()
	}
	return r.videoBillingRepoStub.Apply(ctx, cmd)
}

func TestReconcileVideoTaskMustNotDeductBalanceCacheAfterCapture(t *testing.T) {
	svc, queue, billingRepo := newVideoBillingReconcilerTestService(t, nil)

	group := &Group{ID: 3, Platform: PlatformGrok, RateMultiplier: 1}
	groupID := group.ID
	apiKey := &APIKey{ID: 11, UserID: 5, GroupID: &groupID, Group: group, User: &User{ID: 5}}
	newVideoBillingTestAPIKeyService(t, svc, apiKey)

	// 完成元数据与 create 快照一致（480p/8s）→ actual = hold = 0.64 → capture 成功。
	svc.httpUpstream.(*videoBillingHTTPUpstream).body = `{"status":"done","model":"grok-imagine-video-1.5","video":{"duration":8,"url":"https://video.example/v.mp4"}}`

	const (
		userID    = int64(5)
		dbBalance = 9.36 // capture 结清后的 DB 余额（预留 0.64 后：10.00 → 9.36）
	)

	cache := &videoBillingBalanceCacheStub{balances: map[int64]float64{}}
	cacheSvc := NewBillingCacheService(cache, nil, nil, nil, nil, nil, &config.Config{}, nil)
	t.Cleanup(cacheSvc.Stop)
	svc.billingCacheService = cacheSvc

	refillRepo := &videoBillingConcurrentRefillRepoStub{
		videoBillingRepoStub: billingRepo,
		onApply: func() {
			// capture 已失效缓存；并发请求此刻未命中并回源，把 DB 里的正确余额回填。
			_ = cache.SetUserBalance(context.Background(), userID, dbBalance)
		},
	}
	svc.usageBillingRepo = refillRepo

	pending := GrokVideoPendingBilling{
		RequestID:            "task-balance-cache-deduct",
		UserID:               userID,
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

	require.Equal(t, 1, billingRepo.captured,
		"正常结算路径必须 capture（这笔钱由 capture 在 Apply 之外结清）")
	require.Equal(t, 1, refillRepo.applies,
		"本用例前提：usage log 的 Apply 已执行，且它没有扣 DB 余额（BalanceAlreadyReserved）")

	// 给异步缓存写入池一个落盘窗口：有缺陷时那次多余的扣减会在此窗口内到达。
	deducted := cache.waitForDeduct(300 * time.Millisecond)

	// 无论扣减是否到达，结算后缓存余额都必须等于 DB 余额。
	balance, ok := cache.balance(userID)
	require.True(t, ok, "capture 失效后的并发回填必须保留（结算不得把缓存键删掉）")
	require.InDelta(t, dbBalance, balance, 1e-9,
		"DEFECT REPRODUCED: 结算后缓存余额必须等于 DB 余额，否则余额接近最低留存的用户被误判余额不足")
	require.False(t, deducted,
		"DEFECT REPRODUCED: Apply 未扣 DB 余额（BalanceAlreadyReserved=true），结算不得再入队缓存扣减")
	require.Zero(t, cache.deductCount(),
		"DEFECT REPRODUCED: Apply 未扣 DB 余额（BalanceAlreadyReserved=true），结算不得再入队缓存扣减")
}
