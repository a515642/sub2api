//go:build integration

package repository

import (
	"context"
	"database/sql"
	"testing"
	"time"

	dbmigrations "github.com/Wei-Shaw/sub2api/migrations"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestMigrationUpgradeLegacyForkDBKeepsModelsListConfigSemantics 复现升级链缺陷：
// fork 老库（已应用 143，models_list_config 承载「仅影响 /v1/models 展示」的本地
// 定制数据）升级到合并版本时，上游 235 会把 143 的列原样改名为 model_allowlist
// （语义升级为「准入白名单」），fork 用户的展示数据被静默变成准入约束。
//
// 复现路径：独立容器 → 只应用 143 并记账（模拟 fork 0.2.6 老库）→ 写入真实展示
// 配置 → 按真实 runner 顺序应用全部迁移（模拟升级）→ 断言展示数据是否被错误
// 挪进 model_allowlist。
func TestMigrationUpgradeLegacyForkDBKeepsModelsListConfigSemantics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	db := newLegacyForkDB(t, ctx)
	defer func() { _ = db.Close() }()

	// --- 阶段一：模拟 fork 0.2.6 老库（143 已应用并记账，235/236/239 未记账）。 ---
	legacySQL, err := dbmigrations.FS.ReadFile("143_group_models_list_config.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(legacySQL))
	require.NoError(t, err, "legacy fork db must already carry 143")
	_, err = db.ExecContext(ctx,
		"INSERT INTO schema_migrations (filename, checksum) VALUES ($1, $2)",
		"143_group_models_list_config.sql", migrationChecksum(string(legacySQL)))
	require.NoError(t, err, "record 143 as applied (fork 0.2.6 bookkeeping)")

	// fork 用户在 0.2.6 里配置的展示列表：宽松、仅用于 /v1/models 展示。
	// 展示列表往往刻意比真实可用模型宽（含别名、历史模型），这正是升级后被误当
	// 准入白名单时杀伤力最大的形态。
	legacyDisplayConfig := `{"enabled":true,"models":["gpt-5.4","gpt-5.4-mini","o4-preview-alias","legacy-gpt-4.1"]}`
	_, err = db.ExecContext(ctx, `
INSERT INTO groups (name, platform, rate_multiplier, status, models_list_config)
VALUES ('legacy-fork-group', 'openai', 1, 'active', $1::jsonb)
`, legacyDisplayConfig)
	require.NoError(t, err, "seed fork display config")

	// --- 阶段二：真实升级 —— 按文件名顺序应用全部未记账迁移。 ---
	require.NoError(t, ApplyMigrations(ctx, db), "upgrade must apply cleanly")

	// 调试：检查 235 的记账时间与数据状态（复现定位用）。
	var applied235, applied240 sql.NullTime
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT applied_at FROM schema_migrations WHERE filename = '235_group_model_allowlist.sql'").Scan(&applied235))
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT applied_at FROM schema_migrations WHERE filename = '240_reclaim_renamed_fork_models_list_config.sql'").Scan(&applied240))
	t.Logf("235 applied_at=%s 240 applied_at=%s delta=%s",
		applied235.Time.Format(time.RFC3339Nano), applied240.Time.Format(time.RFC3339Nano), applied240.Time.Sub(applied235.Time))

	// --- 阶段三：断言升级后两列的语义边界。 ---
	var migratedAllowlist, restoredDisplay sql.NullString
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT model_allowlist::text, models_list_config::text FROM groups WHERE name = 'legacy-fork-group'",
	).Scan(&migratedAllowlist, &restoredDisplay))

	t.Logf("after upgrade: model_allowlist=%s models_list_config=%s",
		migratedAllowlist.String, restoredDisplay.String)

	require.JSONEq(t, legacyDisplayConfig, restoredDisplay.String,
		"DEFECT REPRODUCED: fork display config must survive the upgrade in models_list_config")
	require.JSONEq(t, `{}`, migratedAllowlist.String,
		"DEFECT REPRODUCED: legacy display data must NOT silently become an admission allowlist")
}

// newLegacyForkDB 起一个独立 Postgres 容器，构造「fork 0.2.6 老库」形态：
// 完整 fork 侧迁移历史已应用（含 143，不含上游 235/236/239）。
//
// 做法：先应用全部迁移得到完整 schema 与记账，再摘除 235/236/239 与 143 的
// 记账、还原两列为「仅 models_list_config 存在」的老结构，最后由测试主体重放
// 143（其列被下面删除，重放即恢复老库形态）。
func newLegacyForkDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()

	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:18.1-alpine3.23",
		tcpostgres.WithDatabase("sub2api_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err, "start isolated postgres container")
	t.Cleanup(func() { _ = pgContainer.Terminate(context.Background()) })

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable", "TimeZone=UTC")
	require.NoError(t, err)
	db, err := openSQLWithRetry(ctx, dsn, 60*time.Second)
	require.NoError(t, err)

	// 完整 schema + 全部记账。
	require.NoError(t, ApplyMigrations(ctx, db))

	// 摘除 143/235/236/239/240 的记账并还原列结构（fork 0.2.6 无这些上游迁移）。
	for _, name := range []string{
		"240_reclaim_renamed_fork_models_list_config.sql",
		"239_restore_group_models_list_config.sql",
		"236_group_model_allowlist_repair.sql",
		"235_group_model_allowlist.sql",
		"143_group_models_list_config.sql",
	} {
		_, err := db.ExecContext(ctx, "DELETE FROM schema_migrations WHERE filename = $1", name)
		require.NoError(t, err, "drop migration record "+name)
	}
	_, err = db.ExecContext(ctx, "ALTER TABLE groups DROP COLUMN IF EXISTS model_allowlist")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "ALTER TABLE groups DROP COLUMN IF EXISTS models_list_config")
	require.NoError(t, err)
	return db
}
