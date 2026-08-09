"""
数据源配置

当前唯一数据源: Quick BI dbt 数据服务 (Campaign_Ad_dataset_dbt, vigloo_dm)
与看板 [DBT] Marketer Performance 同源同口径。

历史说明:
- 旧 quickbi 源 (quickbi_campaigns 表, 2-1. campaign_dataset) 已于 2026-08 迁移下线, 表留档只读
- XMP 数据源已废弃 (平台不可用), 相关配置已移除
"""

# 数据源选择: 目前仅 "quickbi_dbt"
DATA_SOURCE = "quickbi_dbt"

# Quick BI dbt 配置 (字段命名与 dbt 数据集一致: kst_date/mmp_total_revenue/new_users_mkt/...)
QUICKBI_DBT_CONFIG = {
    "dataset_id": "quickbi_data",
    "table_id": "dbt_campaigns",
    "overview_table_id": "dbt_overview",
    "label": "QuickBI-dbt",
}


def get_data_source_config():
    """获取当前数据源配置"""
    return QUICKBI_DBT_CONFIG
