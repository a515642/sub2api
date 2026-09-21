-- 240: 回迁被 235 错误改名的 fork 展示配置（升级链数据语义修复）。
--
-- 背景：143（fork 与上游共同历史）建立的 models_list_config 在 fork 中是
-- 「仅影响 /v1/models 展示」的本地定制列；上游 235 把这一列改名为
-- model_allowlist，语义升级为「分组模型白名单，约束模型列表与请求准入」，
-- 并声明「数据原样保留」。对纯上游库这是有意的破坏性变更（数据本来就
-- 只有这一列）。但 fork 升级到合并版本时，235 首次执行会把 fork 用户的
-- 展示数据原样改名成准入白名单：宽松的展示列表（常含别名、历史模型）
-- 升级后立即开始拦截不在列表内的真实请求，同时 239 只能补回一个空列，
-- 展示配置「丢失」。
--
-- 判定：只有当 235 是「本次启动的迁移批次内」刚执行的（applied_at 与
-- 240 的执行时刻相差数秒以内），才说明这是 fork 老库升级、235 的改名
-- 刚吞掉了 fork 展示数据 —— 此时把改名挪走的数据回迁到 models_list_config，
-- 并把 model_allowlist 还原为空配置。纯上游库早已应用过 235（applied_at
-- 远早于本迁移执行时刻），不满足时间窗判定，数据保持原样。
DO $$
DECLARE
    applied_235 TIMESTAMPTZ;
    allowlist_json JSONB;
BEGIN
    SELECT applied_at INTO applied_235
      FROM schema_migrations
     WHERE filename = '235_group_model_allowlist.sql';

    -- 235 从未执行（理论不可达：迁移按文件名顺序，235 先于 240），跳过。
    IF applied_235 IS NULL THEN
        RETURN;
    END IF;

    -- 只有 235 与 240 在同一迁移批次内先后执行（差值 ≤ 60 秒）才认定为
    -- fork 老库升级场景；纯上游库两迁移的应用时间至少相隔一次完整发布周期。
    IF now() - applied_235 > INTERVAL '60 seconds' THEN
        RETURN;
    END IF;

    SELECT model_allowlist INTO allowlist_json FROM groups LIMIT 1;

    -- 235 改名后的典型形态：列存在且至少一个分组带非空配置（fork 展示数据）。
    -- 逐组回迁：非空白名单配置（enabled=true 或 models 非空）视为被改名的
    -- fork 展示数据，搬回 models_list_config；model_allowlist 还原为空。
    UPDATE groups
       SET models_list_config = model_allowlist,
           model_allowlist = '{}'::jsonb
     WHERE (model_allowlist ->> 'enabled')::boolean
        OR COALESCE(jsonb_array_length(COALESCE(model_allowlist -> 'models', '[]'::jsonb)), 0) > 0;
END
$$;

-- 幂等收尾：保证两列都存在且形状正确（与 236/239 一致的防御性收口）。
ALTER TABLE groups
    ADD COLUMN IF NOT EXISTS models_list_config JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE groups
    ADD COLUMN IF NOT EXISTS model_allowlist JSONB NOT NULL DEFAULT '{}'::jsonb;
