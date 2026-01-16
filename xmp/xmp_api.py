"""
XMP 广告报表 API 数据抓取脚本
从 XMP Open API 获取广告投放数据并落库到 BigQuery

API 文档: https://xmp-open.mobvista.com
"""

import os
import sys
import json
import hashlib
import time
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any

# 北京时区 (UTC+8)
BEIJING_TZ = timezone(timedelta(hours=8))

# 设置控制台编码为 UTF-8
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# XMP API 配置
XMP_CLIENT_ID = os.getenv('XMP_CLIENT_ID')
XMP_CLIENT_SECRET = os.getenv('XMP_CLIENT_SECRET')

# API 端点
XMP_REPORT_URL = "https://xmp-open.mobvista.com/v2/media/account/report"
XMP_FIELDS_URL = "https://xmp-open.mobvista.com/v1/media/report/fields"
XMP_ACCOUNT_LIST_URL = "https://xmp-open.mobvista.com/v2/media/account/list"
XMP_USER_LIST_URL = "https://xmp-open.mobvista.com/v1/media/user/list"

# 媒体渠道枚举值
CHANNELS = {
    "facebook": "Facebook",
    "google": "Google",
    "tiktok": "TikTok",
    "kwai": "Kwai",
    "apple": "Apple",
    "huawei": "华为(海外)",
    "mintegral": "Mintegral",
    "applovin": "AppLovin",
    "unity": "Unity",
    "ironsource": "ironSource",
    "vungle": "Vungle",
}

# 各渠道数据时区
CHANNEL_TIMEZONE = {
    "facebook": "广告账户时区",
    "google": "广告账户时区",
    "tiktok": "广告账户时区",
    "kwai": "UTC+0",
    "apple": "广告账户时区",
    "huawei": "广告账户时区",
    "mintegral": "广告账户时区",
    "applovin": "UTC+0",
    "unity": "UTC+0",
    "ironsource": "UTC+0",
    "vungle": "UTC+0",
}


class XMPApiClient:
    """XMP Open API 客户端"""

    def __init__(self, client_id: str = None, client_secret: str = None):
        self.client_id = client_id or XMP_CLIENT_ID
        self.client_secret = client_secret or XMP_CLIENT_SECRET

        if not self.client_id or not self.client_secret:
            raise ValueError("请配置 XMP_CLIENT_ID 和 XMP_CLIENT_SECRET 环境变量")

        # 重试配置
        self.max_retries = 3
        self.retry_delays = [10, 30, 60]

        # 请求超时 (秒)
        self.timeout = 60

    def _generate_sign(self, timestamp: int) -> str:
        """
        生成签名
        sign = md5(client_secret + timestamp)
        """
        sign_str = f"{self.client_secret}{timestamp}"
        return hashlib.md5(sign_str.encode()).hexdigest()

    def _make_request(self, url: str, payload: dict, headers: dict = None) -> Optional[dict]:
        """
        发送 API 请求，带重试机制
        """
        default_headers = {"Content-Type": "application/json"}
        if headers:
            default_headers.update(headers)

        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    url,
                    json=payload,
                    headers=default_headers,
                    timeout=self.timeout
                )

                result = response.json()

                if result.get("code") == 0:
                    return result.get("data")

                error_msg = result.get("msg", "Unknown error")

                # 检查是否是限频错误
                if "频繁" in error_msg or "too many" in error_msg.lower():
                    if attempt < self.max_retries - 1:
                        delay = self.retry_delays[attempt]
                        print(f"[限频] 请求太频繁，{delay}秒后重试 ({attempt + 1}/{self.max_retries})...")
                        time.sleep(delay)
                        continue

                print(f"[API错误] code={result.get('code')}, msg={error_msg}")
                return None

            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    delay = self.retry_delays[attempt]
                    print(f"[超时] {delay}秒后重试 ({attempt + 1}/{self.max_retries})...")
                    time.sleep(delay)
                    continue
                print(f"[错误] 请求超时")
                return None

            except Exception as e:
                if attempt < self.max_retries - 1:
                    delay = self.retry_delays[attempt]
                    print(f"[错误] {str(e)[:100]}，{delay}秒后重试...")
                    time.sleep(delay)
                    continue
                print(f"[错误] {e}")
                return None

        return None

    def get_available_fields(self, report_type: str = "ad", language: str = "zh-CN") -> Optional[List[dict]]:
        """
        获取可用指标列表

        Args:
            report_type: "ad" 广告报表 / "material" 素材报表
            language: "zh-CN" 中文 / "en-US" 英文

        Returns:
            指标列表
        """
        timestamp = int(time.time())
        sign = self._generate_sign(timestamp)

        payload = {
            "client_id": self.client_id,
            "timestamp": timestamp,
            "sign": sign,
            "report_type": report_type
        }

        headers = {
            "Content-Type": "application/json",
            "Accept-Language": language
        }

        print(f"[XMP] 获取可用指标 (report_type={report_type})...")
        data = self._make_request(XMP_FIELDS_URL, payload, headers)

        if data and "fields" in data:
            fields = data["fields"]
            print(f"[XMP] 获取到 {len(fields)} 个可用指标")
            return fields

        return None

    def get_account_list(
        self,
        channel: List[str],
        account_id: List[str] = None,
        page_size: int = 200
    ) -> List[dict]:
        """
        获取广告账户列表

        Args:
            channel: 媒体渠道列表，如 ["facebook", "google", "tiktok"]
            account_id: 广告账户ID列表 (可选，最多500个)
            page_size: 每页大小 (1-1000)

        Returns:
            账户列表
        """
        all_accounts = []
        page = 1

        print(f"[XMP] 获取广告账户列表 (渠道: {channel})...")

        while True:
            timestamp = int(time.time())
            sign = self._generate_sign(timestamp)

            payload = {
                "client_id": self.client_id,
                "timestamp": timestamp,
                "sign": sign,
                "channel": channel,
                "page": page,
                "page_size": page_size
            }

            if account_id:
                payload["account_id"] = account_id[:500]

            data = self._make_request(XMP_ACCOUNT_LIST_URL, payload)

            if not data:
                break

            accounts = data.get("list", [])
            if not accounts:
                break

            all_accounts.extend(accounts)
            print(f"[XMP] 第 {page} 页: {len(accounts)} 个账户")

            if len(accounts) < page_size:
                break

            page += 1
            time.sleep(6)

        print(f"[XMP] 共获取 {len(all_accounts)} 个广告账户")
        return all_accounts

    def get_user_list(self, team_id: List[int] = None, user_id: List[int] = None) -> List[dict]:
        """
        获取用户列表（投手列表）

        Args:
            team_id: 部门ID列表
            user_id: 用户ID列表

        Returns:
            用户列表，包含 user_name, role_id, team_name 等
        """
        timestamp = int(time.time())
        sign = self._generate_sign(timestamp)

        payload = {
            "client_id": self.client_id,
            "timestamp": timestamp,
            "sign": sign
        }

        if team_id:
            payload["team_id"] = team_id
        if user_id:
            payload["user_id"] = user_id

        print("[XMP] 获取用户列表...")
        data = self._make_request(XMP_USER_LIST_URL, payload)

        if data:
            print(f"[XMP] 获取到 {len(data)} 个用户")
            return data

        return []

    def fetch_report_data(
        self,
        start_date: str,
        end_date: str,
        metrics: List[str],
        dimension: List[str] = None,
        module: str = None,
        account_id: List[str] = None,
        campaign_id: List[str] = None,
        geo: List[str] = None,
        product_id: List[int] = None,
        currency: str = "USD",
        page_size: int = 500
    ) -> List[dict]:
        """
        获取广告报表数据（自动处理分页）

        Args:
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            metrics: 指标列表，如 ["click", "cost", "impression"]
            dimension: 维度列表，如 ["date", "campaign_id", "geo"]
            module: 媒体渠道 (facebook/google/tiktok/applovin等)
            account_id: 广告账户ID列表
            campaign_id: 广告系列ID列表
            geo: 地区码列表，如 ["US", "JP"]
            product_id: XMP产品ID列表
            currency: 币种 USD/CNY/EUR
            page_size: 每页大小 (1-1000)

        Returns:
            数据列表
        """
        all_data = []
        page = 1

        print(f"[XMP] 拉取报表数据: {start_date} ~ {end_date}")
        print(f"[XMP] 维度: {dimension}, 指标数: {len(metrics)}")

        while True:
            timestamp = int(time.time())
            sign = self._generate_sign(timestamp)

            payload = {
                "client_id": self.client_id,
                "timestamp": timestamp,
                "sign": sign,
                "start_date": start_date,
                "end_date": end_date,
                "metrics": metrics,
                "page": page,
                "page_size": page_size,
                "currency": currency
            }

            if dimension:
                payload["dimension"] = dimension
            if module:
                payload["module"] = module
            if account_id:
                payload["account_id"] = account_id
            if campaign_id:
                payload["campaign_id"] = campaign_id
            if geo:
                payload["geo"] = geo
            if product_id:
                payload["product_id"] = product_id

            data = self._make_request(XMP_REPORT_URL, payload)

            if not data:
                break

            records = data.get("list", [])
            if not records:
                break

            all_data.extend(records)
            print(f"[XMP] 第 {page} 页: {len(records)} 条，累计 {len(all_data)} 条")

            if len(records) < page_size:
                break

            page += 1
            time.sleep(6)  # QPM 限频保护

        print(f"[XMP] 完成，共 {len(all_data)} 条记录")
        return all_data

    def fetch_today_data(self, metrics: List[str] = None, dimension: List[str] = None) -> List[dict]:
        """获取今日数据"""
        today = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')
        if metrics is None:
            metrics = ["impression", "click", "cost"]
        return self.fetch_report_data(today, today, metrics, dimension)

    def fetch_yesterday_data(self, metrics: List[str] = None, dimension: List[str] = None) -> List[dict]:
        """获取昨日数据"""
        yesterday = (datetime.now(BEIJING_TZ) - timedelta(days=1)).strftime('%Y-%m-%d')
        if metrics is None:
            metrics = ["impression", "click", "cost"]
        return self.fetch_report_data(yesterday, yesterday, metrics, dimension)


# 默认常用指标
DEFAULT_METRICS = [
    "impression",
    "click",
    "cost",
    "postback_install",
]


def upload_to_bigquery(data: list, project_id: str, dataset_id: str, batch_id: str = None):
    """上传数据到 BigQuery"""
    try:
        from bigquery_storage import BigQueryUploader

        uploader = BigQueryUploader(project_id, dataset_id)
        count = uploader.upload_xmp_report(data, batch_id=batch_id)
        print(f"[BQ] 已上传 {count} 条记录到 BigQuery")
        return count

    except ImportError:
        print("[BQ] 未安装 google-cloud-bigquery，跳过上传")
    except Exception as e:
        print(f"[BQ] 上传失败: {e}")

    return 0


def main():
    """主函数"""
    load_dotenv()

    # 初始化客户端
    try:
        client = XMPApiClient()
    except ValueError as e:
        print(f"[错误] {e}")
        return

    # 获取可用指标
    fields = client.get_available_fields("ad")
    if fields:
        print("\n=== 可用指标示例 ===")
        for f in fields[:5]:
            print(f"  {f['field']}: {f['title']} ({f['source']})")

    # 获取查询日期
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')

    # 拉取数据
    data = client.fetch_report_data(
        start_date=date_str,
        end_date=date_str,
        metrics=DEFAULT_METRICS,
        dimension=["date", "campaign_id", "geo"],
        currency="USD"
    )

    if not data:
        print("未获取到数据")
        return

    # 生成批次 ID
    batch_id = datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M%S')
    print(f"\n=== 批次 ID: {batch_id} ===")

    # BigQuery 上传
    bq_project = os.getenv('BQ_PROJECT_ID')
    bq_dataset = os.getenv('XMP_BQ_DATASET_ID', 'xmp_data')

    if bq_project:
        upload_to_bigquery(data, bq_project, bq_dataset, batch_id)
    else:
        print("[BQ] 未配置 BQ_PROJECT_ID，跳过上传")


if __name__ == '__main__':
    main()

