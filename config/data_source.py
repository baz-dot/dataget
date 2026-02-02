"""
数据源配置
在这里切换使用 QuickBI 还是 XMP 数据
"""

# 数据源选择: "quickbi" 或 "xmp" 或 "xmp_internal"
DATA_SOURCE = "xmp_internal"

# QuickBI 配置
QUICKBI_CONFIG = {
    "dataset_id": "quickbi_data",
    "table_id": "quickbi_campaigns",
}

# XMP 配置 (Open API)
XMP_CONFIG = {
    "dataset_id": "xmp_data",
    "table_id": "xmp_campaigns_view",
}

# XMP 内部 API 配置
XMP_INTERNAL_CONFIG = {
    "dataset_id": "xmp_data",
    "table_id": "xmp_internal_campaigns_view",  # 视图已包含 media_user_revenue 字段
}


def get_data_source_config():
    """获取当前数据源配置"""
    if DATA_SOURCE == "xmp_internal":
        return XMP_INTERNAL_CONFIG
    if DATA_SOURCE == "xmp":
        return XMP_CONFIG
    return QUICKBI_CONFIG
