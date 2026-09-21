package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

const videoTaskHoldPrefix = "video_task:"

// 视频结算重试上限与失败码前缀，对齐 batch_image_settlement 的终态设计：
// capture/计价等可重复失败达到上限后释放冻结余额并删除队列条目，
// 否则永久错误（如完成元数据价格超预留上限）会每 15s 无限重排、
// 冻结余额直到 30 天 TTL 过期都不释放。
const (
	videoTaskSettlementMaxRetries  = 5
	videoTaskSettlementErrorPrefix = "VIDEO_SETTLEMENT_"
	videoTaskRetryExhaustedCode    = "VIDEO_SETTLEMENT_RETRY_EXHAUSTED"
)

func NewVideoTaskHoldID() string {
	return videoTaskHoldPrefix + strings.ReplaceAll(uuid.NewString(), "-", "")
}

func videoTaskHoldCommand(requestID, taskID string, userID, apiKeyID int64, holdAmount, actualAmount float64, payloadHash string) *BatchImageBalanceHoldCommand {
	return &BatchImageBalanceHoldCommand{
		RequestID:          requestID,
		APIKeyID:           apiKeyID,
		UserID:             userID,
		BatchID:            taskID,
		HoldAmount:         holdAmount,
		ActualAmount:       actualAmount,
		RequestPayloadHash: payloadHash,
	}
}

func (s *OpenAIGatewayService) reserveVideoTaskBalance(ctx context.Context, userID, apiKeyID int64, taskID string, amount float64, payloadHash string) error {
	if s == nil || s.usageBillingRepo == nil {
		return errors.New("video task billing repository is unavailable")
	}
	if amount <= 0 {
		return nil
	}
	_, err := s.usageBillingRepo.ReserveBatchImageBalance(ctx, videoTaskHoldCommand(BatchImageHoldRequestID(taskID), taskID, userID, apiKeyID, amount, 0, payloadHash))
	if err == nil {
		if s.billingCacheService != nil {
			_ = s.billingCacheService.InvalidateUserBalance(ctx, userID)
		}
		slog.Info("video_billing.hold_reserved", "task_id", taskID, "user_id", userID, "api_key_id", apiKeyID, "amount", amount)
	} else {
		slog.Warn("video_billing.hold_reserve_failed", "task_id", taskID, "user_id", userID, "api_key_id", apiKeyID, "amount", amount, "error", err)
	}
	return err
}

func (s *OpenAIGatewayService) ReserveVideoTaskBalance(ctx context.Context, userID, apiKeyID int64, taskID string, amount float64, payloadHash string) error {
	return s.reserveVideoTaskBalance(ctx, userID, apiKeyID, taskID, amount, payloadHash)
}

func (s *OpenAIGatewayService) captureVideoTaskBalance(ctx context.Context, task *GrokVideoPendingBilling, userID, apiKeyID int64, actualAmount float64) error {
	if s == nil || s.usageBillingRepo == nil || task == nil || task.HoldAmount <= 0 {
		return nil
	}
	_, err := s.usageBillingRepo.CaptureBatchImageBalance(ctx,
		videoTaskHoldCommand(BatchImageCaptureRequestID(task.HoldID), task.HoldID, userID, apiKeyID, task.HoldAmount, actualAmount, StableVideoTaskBillingRequestID(task.Platform, task.RequestID)))
	if err == nil {
		if s.billingCacheService != nil {
			_ = s.billingCacheService.InvalidateUserBalance(ctx, userID)
		}
		slog.Info("video_billing.hold_captured", "task_id", task.RequestID, "platform", task.Platform, "user_id", userID, "api_key_id", apiKeyID, "hold_amount", task.HoldAmount, "actual_amount", actualAmount, "released_amount", QuantizeUsageBillingAmount(task.HoldAmount-actualAmount))
	} else {
		slog.Warn("video_billing.hold_capture_failed", "task_id", task.RequestID, "platform", task.Platform, "user_id", userID, "api_key_id", apiKeyID, "hold_amount", task.HoldAmount, "actual_amount", actualAmount, "error", err)
	}
	return err
}

func (s *OpenAIGatewayService) releaseVideoTaskBalance(ctx context.Context, task *GrokVideoPendingBilling, userID, apiKeyID int64) error {
	if s == nil || s.usageBillingRepo == nil || task == nil || task.HoldAmount <= 0 {
		return nil
	}
	_, err := s.usageBillingRepo.ReleaseBatchImageBalance(ctx,
		videoTaskHoldCommand(BatchImageReleaseRequestID(task.HoldID), task.HoldID, userID, apiKeyID, task.HoldAmount, 0, StableVideoTaskBillingRequestID(task.Platform, task.RequestID)))
	if err == nil {
		if s.billingCacheService != nil {
			_ = s.billingCacheService.InvalidateUserBalance(ctx, userID)
		}
		slog.Info("video_billing.hold_released", "task_id", task.RequestID, "platform", task.Platform, "user_id", userID, "api_key_id", apiKeyID, "amount", task.HoldAmount)
	} else {
		slog.Warn("video_billing.hold_release_failed", "task_id", task.RequestID, "platform", task.Platform, "user_id", userID, "api_key_id", apiKeyID, "amount", task.HoldAmount, "error", err)
	}
	return err
}

func (s *OpenAIGatewayService) ReleaseVideoTaskBalance(ctx context.Context, task *GrokVideoPendingBilling, userID, apiKeyID int64) error {
	return s.releaseVideoTaskBalance(ctx, task, userID, apiKeyID)
}

// CalculateVideoTaskCost uses the same pricing path as final usage billing,
// but with the deterministic create-time video units.
func (s *OpenAIGatewayService) CalculateVideoTaskCost(ctx context.Context, apiKey *APIKey, account *Account, model, resolution string, durationSeconds int) (float64, error) {
	return s.calculateVideoTaskCostAt(ctx, apiKey, account, model, resolution, durationSeconds, time.Now())
}

func (s *OpenAIGatewayService) calculateVideoTaskCostAt(ctx context.Context, apiKey *APIKey, account *Account, model, resolution string, durationSeconds int, pricingAt time.Time) (float64, error) {
	if s == nil || apiKey == nil || account == nil || apiKey.User == nil {
		return 0, errors.New("video task pricing context is incomplete")
	}
	result := &OpenAIForwardResult{Model: strings.TrimSpace(model), BillingModel: strings.TrimSpace(model), VideoCount: 1, VideoResolution: resolution, VideoDurationSeconds: durationSeconds}
	baseMultiplier := 1.0
	if s.cfg != nil {
		baseMultiplier = s.cfg.Default.RateMultiplier
	}
	if apiKey.GroupID != nil && apiKey.Group != nil {
		baseMultiplier = resolveAccountUserBillingMultiplier(account, s.resolveUserGroupRate(ctx, apiKey.User.ID, *apiKey.GroupID, apiKey.Group.RateMultiplier))
	}
	videoMultiplier := resolveVideoRateMultiplier(apiKey, baseMultiplier)
	candidates := usageBillingModelCandidates(model, model, "", "", model, model)
	candidates = s.filterCNProviderBillingModelCandidates(ctx, account, apiKey, candidates)
	cost, err := s.calculateOpenAIRecordUsageCost(ctx, result, apiKey, account, candidates, baseMultiplier, baseMultiplier, videoMultiplier, baseMultiplier, UsageTokens{}, "", openAILongContextBillingGate(account), pricingAt)
	if err != nil {
		return 0, fmt.Errorf("calculate video task cost: %w", err)
	}
	return QuantizeUsageBillingAmount(cost.ActualCost), nil
}

// SetVideoTaskAPIKeyService supplies the worker with an owner snapshot after
// the gateway service has been constructed (avoiding a constructor cycle).
func (s *OpenAIGatewayService) SetVideoTaskAPIKeyService(v *APIKeyService) {
	if s != nil {
		s.videoTaskAPIKeyService = v
	}
}

func (s *OpenAIGatewayService) StartVideoTaskBillingReconciler() {
	if s == nil {
		return
	}
	if _, ok := s.cache.(VideoTaskBillingCache); !ok || s.videoTaskAPIKeyService == nil {
		return
	}
	go s.runVideoTaskBillingReconciler()
}

func (s *OpenAIGatewayService) runVideoTaskBillingReconciler() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		queue, ok := s.cache.(VideoTaskBillingCache)
		if !ok {
			cancel()
			return
		}
		key, payload, err := queue.ClaimDueVideoTask(ctx, time.Now())
		if err == nil && key != "" && len(payload) > 0 {
			s.reconcileVideoTask(ctx, key, payload)
		}
		cancel()
		time.Sleep(5 * time.Second)
	}
}

func (s *OpenAIGatewayService) reconcileVideoTask(ctx context.Context, key string, payload []byte) {
	var pending GrokVideoPendingBilling
	if err := json.Unmarshal(payload, &pending); err != nil {
		slog.Error("video_billing.invalid_task_payload", "queue_key", key, "error", err)
		return
	}
	slog.Debug("video_billing.reconcile_started", "task_id", pending.RequestID, "platform", pending.Platform, "user_id", pending.UserID, "api_key_id", pending.APIKeyID, "amount", pending.HoldAmount)
	queue, ok := s.cache.(VideoTaskBillingCache)
	if !ok {
		slog.Error("video_billing.cache_unavailable", "task_id", pending.RequestID)
		return
	}
	// 重试耗尽检查必须先于各类可重复失败分支（计价/capture/记录），
	// 否则永久错误（如完成元数据价格超预留上限）绕过耗尽出口无限重排。
	if isVideoTaskSettlementRetryExhausted(&pending) {
		if !s.failExhaustedVideoTaskSettlement(ctx, queue, key, &pending) {
			// 终态释放失败（瞬时故障）时必须重排，下一轮重试释放；
			// 直接丢弃会让已耗尽任务的冻结余额永远无法释放。
			_ = s.requeueVideoTask(ctx, queue, key, payload)
		}
		return
	}
	account, err := s.accountRepo.GetByID(ctx, pending.AccountID)
	if err != nil || account == nil {
		slog.Warn("video_billing.account_unavailable", "task_id", pending.RequestID, "account_id", pending.AccountID, "error", err)
		_ = s.requeueVideoTask(ctx, queue, key, payload)
		return
	}
	status, result, err := s.pollVideoTaskStatus(ctx, account, pending.Platform, pending.RequestID)
	if err != nil {
		slog.Warn("video_billing.poll_failed", "task_id", pending.RequestID, "platform", pending.Platform, "account_id", pending.AccountID, "error", err)
		_ = s.requeueVideoTask(ctx, queue, key, payload)
		return
	}
	if status == "pending" {
		slog.Debug("video_billing.poll_pending", "task_id", pending.RequestID, "platform", pending.Platform)
		_ = s.requeueVideoTask(ctx, queue, key, payload)
		return
	}
	if status != "succeeded" {
		slog.Info("video_billing.task_failed", "task_id", pending.RequestID, "platform", pending.Platform, "user_id", pending.UserID, "amount", pending.HoldAmount)
		if err := s.releaseVideoTaskBalance(ctx, &pending, pending.UserID, pending.APIKeyID); err != nil {
			_ = s.requeueVideoTask(ctx, queue, key, payload)
			return
		}
		_ = queue.DeleteVideoTaskBilling(ctx, key)
		return
	}
	apiKey, err := s.videoTaskAPIKeyService.GetByID(ctx, pending.APIKeyID)
	if err != nil || apiKey == nil {
		slog.Warn("video_billing.api_key_unavailable", "task_id", pending.RequestID, "api_key_id", pending.APIKeyID, "error", err)
		_ = s.requeueVideoTask(ctx, queue, key, payload)
		return
	}
	apiKey.User = &User{ID: pending.UserID}
	if err := mergeVideoTaskBillingResult(result, &pending); err != nil {
		slog.Error("video_billing.completion_metadata_missing", "task_id", pending.RequestID, "platform", pending.Platform, "user_id", pending.UserID, "error", err)
		if s.recordVideoTaskSettlementFailure(ctx, queue, key, &pending, payload, "VIDEO_SETTLEMENT_COMPLETION_METADATA_MISSING", err) {
			return
		}
		_ = s.requeueVideoTask(ctx, queue, key, payload)
		return
	}
	applyVideoTaskTotalDuration(result, &pending, time.Now())
	result.VideoCount = 1
	result.RequestID = StableVideoTaskBillingRequestID(pending.Platform, pending.RequestID)
	pricingAt, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(pending.CreatedAt))
	if pricingAt.IsZero() {
		pricingAt = time.Now()
	}
	// capture 已成功（金额已结算）的重试轮次：跳过重定价与 capture，
	// 只重试 RecordUsage。重算 actual 会因轮间定价漂移（管理员改倍率/
	// 峰谷变化）产生不同指纹，被 dedup 判为 fingerprint conflict，
	// 白白消耗重试次数后滑向终态 release（对已消费的 hold 双倍退款）。
	actualAmount := pending.SettledActualAmount
	if actualAmount <= 0 {
		var err error
		actualAmount, err = s.calculateVideoTaskCostAt(ctx, apiKey, account, result.BillingModel, result.VideoResolution, result.VideoDurationSeconds, pricingAt)
		if err != nil {
			slog.Warn("video_billing.actual_cost_failed", "task_id", pending.RequestID, "platform", pending.Platform, "user_id", pending.UserID, "error", err)
			if s.recordVideoTaskSettlementFailure(ctx, queue, key, &pending, payload, "VIDEO_SETTLEMENT_PRICING_FAILED", err) {
				return
			}
			_ = s.requeueVideoTask(ctx, queue, key, payload)
			return
		}
	}
	if err := s.captureVideoTaskBalance(ctx, &pending, pending.UserID, pending.APIKeyID, actualAmount); err != nil {
		if s.recordVideoTaskSettlementFailure(ctx, queue, key, &pending, payload, "VIDEO_SETTLEMENT_CAPTURE_FAILED", err) {
			return
		}
		_ = s.requeueVideoTask(ctx, queue, key, payload)
		return
	}
	// capture 成功即落结算快照：金额已收、frozen 已扣。后续重试轮次凭
	// SettledActualAmount 识别"已结算"，耗尽终态据此只删条目、不 release。
	pending.SettledActualAmount = actualAmount
	if err := s.RecordUsage(ctx, &OpenAIRecordUsageInput{Result: result, APIKey: apiKey, User: apiKey.User, Account: account, APIKeyService: s.videoTaskAPIKeyService, BalanceAlreadyReserved: true, SettledBalanceCost: &actualAmount, PricingAt: pricingAt, QuotaPlatform: pending.Platform}); err != nil {
		slog.Warn("video_billing.usage_record_failed", "task_id", pending.RequestID, "platform", pending.Platform, "user_id", pending.UserID, "hold_amount", pending.HoldAmount, "actual_amount", actualAmount, "error", err)
		if s.recordVideoTaskSettlementFailure(ctx, queue, key, &pending, payload, "VIDEO_SETTLEMENT_USAGE_RECORD_FAILED", err) {
			return
		}
		_ = s.requeueVideoTask(ctx, queue, key, payload)
		return
	}
	slog.Info("video_billing.settled", "task_id", pending.RequestID, "platform", pending.Platform, "user_id", pending.UserID, "api_key_id", pending.APIKeyID, "hold_amount", pending.HoldAmount, "actual_amount", actualAmount)
	_ = queue.DeleteVideoTaskBilling(ctx, key)
}

// isVideoTaskSettlementRetryExhausted 判断视频结算是否已达重试上限。
// 覆盖所有 VIDEO_SETTLEMENT_* 失败码；pending/poll 等瞬时轮询失败不计数
// （任务可能几分钟后才完成，轮询失败无限重试是既有语义）。
func isVideoTaskSettlementRetryExhausted(pending *GrokVideoPendingBilling) bool {
	return pending != nil &&
		pending.RetryCount >= videoTaskSettlementMaxRetries &&
		strings.HasPrefix(pending.LastErrorCode, videoTaskSettlementErrorPrefix)
}

// recordVideoTaskSettlementFailure 记录一次视频结算失败并递增 retry_count，
// 重排携带新计数的 payload；达到上限时立即终态（释放冻结余额 + 删除条目）。
// 返回 true 表示失败路径已自行处理重排/终态，调用方不得再用旧 payload 重排
// （否则会覆盖掉递增后的计数，永久失败任务重试计数永远停留在 1）；
// 仅当 payload 编码失败（极罕见）才返回 false，由调用方原样重排兜底。
func (s *OpenAIGatewayService) recordVideoTaskSettlementFailure(ctx context.Context, queue VideoTaskBillingCache, key string, pending *GrokVideoPendingBilling, payload []byte, code string, cause error) bool {
	pending.RetryCount++
	pending.LastErrorCode = code
	updated, err := json.Marshal(pending)
	if err != nil {
		slog.Error("video_billing.settlement_failure_encode_failed", "queue_key", key, "error", err)
		return false
	}
	if isVideoTaskSettlementRetryExhausted(pending) {
		// 达到上限立即终态：释放冻结余额、删除队列条目，不再等下一轮。
		return s.failExhaustedVideoTaskSettlement(ctx, queue, key, pending)
	}
	slog.Warn("video_billing.settlement_failure_recorded",
		"task_id", pending.RequestID, "platform", pending.Platform,
		"retry_count", pending.RetryCount, "max_retries", videoTaskSettlementMaxRetries,
		"code", code, "error", cause)
	if err := s.setVideoTaskBillingWithRetry(ctx, queue, key, updated, time.Now().Add(videoTaskRetryDelay)); err != nil {
		// 入队失败（含重试）时退回旧 payload 原样重排的兜底也失败：任务已
		// 从队列丢失，只能记录（hold 的释放依赖管理员介入或 TTL 兜底失效）。
		slog.Error("video_billing.settlement_failure_requeue_failed", "queue_key", key, "task_id", pending.RequestID, "error", err)
	}
	return true
}

// failExhaustedVideoTaskSettlement 是重试耗尽后的终态出口：释放冻结余额
// （与 batch_image_settlement.failExhaustedSettlement 对齐）并删除队列条目。
// 释放使用与 capture 相同的稳定请求指纹，幂等重试不会指纹冲突。
// 例外：capture 已成功（SettledActualAmount > 0，金额已结算、frozen 已扣）
// 的任务只删除条目、放弃补写 usage log，绝不 release——capture 与 release 的
// dedup request_id 不同，幂等层防不住"已结算再释放"，release 会从 frozen
// 共享池双倍退款；无其它并发 hold 时则 frozen 不足、release 永久失败。
func (s *OpenAIGatewayService) failExhaustedVideoTaskSettlement(ctx context.Context, queue VideoTaskBillingCache, key string, pending *GrokVideoPendingBilling) bool {
	if pending == nil {
		return true
	}
	if pending.SettledActualAmount > 0 {
		slog.Warn("video_billing.settlement_retry_exhausted_settled",
			"task_id", pending.RequestID, "platform", pending.Platform,
			"user_id", pending.UserID, "api_key_id", pending.APIKeyID,
			"hold_amount", pending.HoldAmount, "settled_amount", pending.SettledActualAmount,
			"retry_count", pending.RetryCount, "last_error_code", pending.LastErrorCode,
			"terminal_code", videoTaskRetryExhaustedCode)
		_ = queue.DeleteVideoTaskBilling(ctx, key)
		return true
	}
	slog.Warn("video_billing.settlement_retry_exhausted",
		"task_id", pending.RequestID, "platform", pending.Platform,
		"user_id", pending.UserID, "api_key_id", pending.APIKeyID,
		"hold_amount", pending.HoldAmount,
		"retry_count", pending.RetryCount, "last_error_code", pending.LastErrorCode,
		"terminal_code", videoTaskRetryExhaustedCode)
	if err := s.releaseVideoTaskBalance(ctx, pending, pending.UserID, pending.APIKeyID); err != nil {
		slog.Error("video_billing.exhausted_release_failed", "task_id", pending.RequestID, "hold_id", pending.HoldID, "error", err)
		return false
	}
	_ = queue.DeleteVideoTaskBilling(ctx, key)
	return true
}

func applyVideoTaskTotalDuration(result *OpenAIForwardResult, pending *GrokVideoPendingBilling, discoveredAt time.Time) {
	if result == nil || pending == nil {
		return
	}
	createdAtUnix := result.VideoCreatedAtUnix
	if createdAtUnix <= 0 {
		createdAtUnix = pending.UpstreamCreatedAtUnix
	}
	if createdAtUnix > 0 && result.VideoCompletedAtUnix >= createdAtUnix {
		result.Duration = time.Duration(result.VideoCompletedAtUnix-createdAtUnix) * time.Second
		return
	}
	if localDuration := GrokVideoE2EDuration(pending.CreatedAt, discoveredAt); localDuration > 0 {
		result.Duration = localDuration
	}
}

func mergeVideoTaskBillingResult(result *OpenAIForwardResult, pending *GrokVideoPendingBilling) error {
	if result == nil || pending == nil {
		return errors.New("video task completion result is missing")
	}
	if strings.TrimSpace(result.Model) == "" {
		result.Model = pending.Model
	}
	if strings.TrimSpace(result.BillingModel) == "" {
		result.BillingModel = firstNonEmpty(result.Model, pending.BillingModel, pending.Model)
	}
	if result.VideoDurationSeconds <= 0 {
		result.VideoDurationSeconds = pending.VideoDurationSeconds
	}
	if strings.TrimSpace(result.VideoResolution) == "" {
		result.VideoResolution = pending.VideoResolution
	}
	if result.VideoDurationSeconds <= 0 {
		return errors.New("video completion duration is unavailable")
	}
	if strings.TrimSpace(result.VideoResolution) == "" {
		return errors.New("video completion resolution is unavailable")
	}
	result.VideoDurationSeconds = NormalizeVideoBillingDurationSecondsOrDefault(result.VideoDurationSeconds)
	result.VideoResolution = NormalizeVideoBillingResolutionOrDefault(result.VideoResolution)
	return nil
}

const (
	videoTaskRetryDelay = 15 * time.Second
	videoTaskQueueTTL   = 30 * 24 * time.Hour
)

// videoTaskSetRetryDelays 是入队失败的即时重试退避：ClaimDueVideoTask 的
// Lua 是 ZREM+GET 原子出队——claim 成功后 Set 失败（Redis 瞬时抖动）会让
// 任务永久丢失（zset 无条目、reconciler 永远看不到、Postgres 侧 hold
// 永久冻结，无 TTL 兜底、无重试入口）。Set 失败时必须在进程内重试，
// 不能只打日志后放弃。
var videoTaskSetRetryDelays = []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second}

func (s *OpenAIGatewayService) setVideoTaskBillingWithRetry(ctx context.Context, queue VideoTaskBillingCache, key string, payload []byte, nextPoll time.Time) error {
	var err error
	for attempt, delay := range append([]time.Duration{0}, videoTaskSetRetryDelays...) {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return err
			case <-time.After(delay):
			}
		}
		if err = queue.SetVideoTaskBilling(ctx, key, payload, nextPoll, videoTaskQueueTTL); err == nil {
			return nil
		}
		slog.Warn("video_billing.queue_set_retry", "queue_key", key, "attempt", attempt+1, "error", err)
	}
	return err
}

func (s *OpenAIGatewayService) requeueVideoTask(ctx context.Context, queue VideoTaskBillingCache, key string, payload []byte) error {
	err := s.setVideoTaskBillingWithRetry(ctx, queue, key, payload, time.Now().Add(videoTaskRetryDelay))
	if err != nil {
		slog.Error("video_billing.requeue_failed", "queue_key", key, "error", err)
	}
	return err
}

func (s *OpenAIGatewayService) pollVideoTaskStatus(ctx context.Context, account *Account, platform, requestID string) (string, *OpenAIForwardResult, error) {
	token, _, err := s.GetRequestCredential(ctx, nil, account)
	if err != nil {
		return "", nil, err
	}
	var url string
	if platform == VideoTaskPlatformOpenAI {
		url = buildOpenAIEndpointURL(account.GetOpenAIFormatBaseURL(), "/v1/videos/"+requestID)
	} else {
		url, err = buildGrokMediaURL(account, s.cfg, GrokMediaEndpointVideoStatus, requestID)
	}
	if err != nil {
		return "", nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	account.ApplyHeaderOverrides(req.Header)
	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return "", nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return "", nil, err
	}
	if resp.StatusCode >= 400 {
		return "", nil, fmt.Errorf("video status returned %d", resp.StatusCode)
	}
	if platform == VideoTaskPlatformOpenAI {
		result := parseOpenAIVideoResult(body, requestID, 0)
		if result.VideoCount > 0 {
			return "succeeded", result, nil
		}
		status := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "status").String()))
		if status == "failed" || status == "cancelled" {
			return "failed", result, nil
		}
		return "pending", result, nil
	}
	if IsGrokVideoStatusBillable(body) {
		result := ExtractGrokVideoBillingFromStatusBody(body, nil, requestID)
		return "succeeded", result, nil
	}
	status := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "status").String()))
	if status == "failed" || status == "expired" || status == "cancelled" {
		return "failed", nil, nil
	}
	return "pending", nil, nil
}
