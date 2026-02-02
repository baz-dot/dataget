-- 将 15:00 的 media_user_revenue 减少 5% 后赋值给 14:00 的数据
-- 使用方法：在 BigQuery 控制台中执行此 SQL

-- 步骤1: 先查看 14:00 和 15:00 的 batch_id
-- 你需要先确认具体的 batch_id
SELECT
  batch_id,
  COUNT(*) as record_count,
  MIN(fetched_at) as fetched_time
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
WHERE stat_date = '2025-12-24'
  AND (batch_id LIKE '20251224_14%' OR batch_id LIKE '20251224_15%')
GROUP BY batch_id
ORDER BY batch_id;

-- 步骤2: 更新 14:00 的数据（使用 15:00 的数据 * 0.95）
-- 方法1: 使用 MERGE 语句（使用具体的 batch_id，避免多对多匹配）
-- 请先运行步骤1，确认具体的 batch_id，然后替换下面的 batch_id 值

-- 根据查询结果，使用以下 batch_id：
-- 14:00 数据: 20251224_140037 (1652条，没有 media_user_revenue)
-- 15:00 数据: 20251224_154454 (1726条，有 media_user_revenue)
-- 使用更多字段确保一对一匹配

MERGE `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns` AS target
USING (
  SELECT
    stat_date,
    campaign_id,
    country,
    channel,
    language,
    device,
    media_user_revenue * 0.95 AS adjusted_revenue
  FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
  WHERE stat_date = '2025-12-24'
    AND batch_id = '20251224_154454'
) AS source
ON target.stat_date = source.stat_date
  AND target.campaign_id = source.campaign_id
  AND target.country = source.country
  AND target.channel = source.channel
  AND target.language = source.language
  AND target.device = source.device
  AND target.batch_id = '20251224_140037'
WHEN MATCHED THEN
  UPDATE SET media_user_revenue = source.adjusted_revenue;

-- 方法2: 使用 UPDATE 子查询（添加更多维度确保一对一匹配）
UPDATE `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns` AS target
SET media_user_revenue = (
  SELECT media_user_revenue * 0.95
  FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns` AS source
  WHERE source.stat_date = target.stat_date
    AND source.campaign_id = target.campaign_id
    AND source.country = target.country
    AND source.channel = target.channel
    AND source.language = target.language
    AND source.device = target.device
    AND source.drama_id = target.drama_id
    AND source.batch_id LIKE '20251224_15%'
  LIMIT 1
)
WHERE target.stat_date = '2025-12-24'
  AND target.batch_id LIKE '20251224_14%'
  AND EXISTS (
    SELECT 1
    FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns` AS source
    WHERE source.stat_date = target.stat_date
      AND source.campaign_id = target.campaign_id
      AND source.country = target.country
      AND source.channel = target.channel
      AND source.language = target.language
      AND source.device = target.device
      AND source.drama_id = target.drama_id
      AND source.batch_id LIKE '20251224_15%'
  );

-- 步骤3: 验证更新结果
-- 对比 14:00 和 15:00 的数据
SELECT
  '14:00' AS time_slot,
  batch_id,
  campaign_id,
  country,
  channel,
  media_user_revenue
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
WHERE stat_date = '2025-12-24'
  AND batch_id LIKE '20251224_14%'
  AND campaign_id = 'YOUR_CAMPAIGN_ID'  -- 替换为实际的 campaign_id 进行验证

UNION ALL

SELECT
  '15:00' AS time_slot,
  batch_id,
  campaign_id,
  country,
  channel,
  media_user_revenue
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
WHERE stat_date = '2025-12-24'
  AND batch_id LIKE '20251224_15%'
  AND campaign_id = 'YOUR_CAMPAIGN_ID'  -- 替换为实际的 campaign_id 进行验证
ORDER BY time_slot, campaign_id;
