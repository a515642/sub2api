-- 239: 并存恢复 groups.models_list_config（本地定制列）。
--
-- 背景：本地定制 f597c1581 用 143 引入 models_list_config（仅影响 /v1/models
-- 展示列表）；上游 cff3f8985 用 235 将同名列更名为 model_allowlist（语义升级
-- 为分组模型白名单，约束模型列表与请求准入），236 负责兜底收敛。
-- 合并 0.2.7 后，本地 ent schema 同时保留两列：models_list_config 承载展示
-- 配置，model_allowlist 承载白名单准入，二者是独立功能，不是同一份数据。
-- 但 235 的重命名会把 143 建出来的本地定制列一并改名，导致全新库上
-- models_list_config 不存在、ent 每次写 groups 都报
--     pq: column "models_list_config" of relation "groups" does not exist
--
-- 本迁移可重放，负责在 235/236 之后把本地定制列补回来：
--   1) models_list_config 不存在 -> 按默认值补建（新库、或已被 235 改名的库）；
--   2) 已存在                   -> 不动（0.2.6 老库若列还在，数据原样保留）。
-- model_allowlist 交给 235/236 保证，本迁移不碰。
ALTER TABLE groups
    ADD COLUMN IF NOT EXISTS models_list_config JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE groups SET models_list_config = '{}'::jsonb WHERE models_list_config IS NULL;

ALTER TABLE groups ALTER COLUMN models_list_config SET DEFAULT '{}'::jsonb;
ALTER TABLE groups ALTER COLUMN models_list_config SET NOT NULL;

COMMENT ON COLUMN groups.models_list_config IS
    '自定义 /v1/models 展示列表配置；仅影响模型列表响应，不影响调度（本地定制，与 model_allowlist 相互独立）';
