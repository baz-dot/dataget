"""
XMP API 测试脚本
用于验证 API 调用是否正常工作
"""

import os
import sys
import json
import hashlib
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# API 配置
CLIENT_ID = os.getenv('XMP_CLIENT_ID')
CLIENT_SECRET = os.getenv('XMP_CLIENT_SECRET')

def generate_sign(client_secret: str, timestamp: int) -> str:
    """生成签名: md5(client_secret + timestamp)"""
    sign_str = f"{client_secret}{timestamp}"
    return hashlib.md5(sign_str.encode()).hexdigest()


def test_get_fields():
    """测试获取可用指标接口"""
    print("\n" + "=" * 50)
    print("测试 1: 获取可用指标")
    print("=" * 50)

    timestamp = int(time.time())
    sign = generate_sign(CLIENT_SECRET, timestamp)

    url = "https://xmp-open.mobvista.com/v1/media/report/fields"
    payload = {
        "client_id": CLIENT_ID,
        "timestamp": timestamp,
        "sign": sign,
        "report_type": "ad"
    }

    print(f"URL: {url}")
    print(f"Payload: {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(url, json=payload, timeout=30)
        result = response.json()

        print(f"\n响应状态码: {response.status_code}")
        print(f"响应 code: {result.get('code')}")
        print(f"响应 msg: {result.get('msg')}")

        if result.get("code") == 0:
            fields = result.get("data", {}).get("fields", [])
            print(f"\n获取到 {len(fields)} 个可用指标")
            print("\n前 10 个指标:")
            for f in fields[:10]:
                print(f"  - {f['field']}: {f['title']} ({f.get('source', 'N/A')})")
            return True
        else:
            print(f"\n错误: {result}")
            return False

    except Exception as e:
        print(f"\n请求失败: {e}")
        return False


def test_get_accounts():
    """测试获取广告账户列表"""
    print("\n" + "=" * 50)
    print("测试 2: 获取广告账户列表")
    print("=" * 50)

    timestamp = int(time.time())
    sign = generate_sign(CLIENT_SECRET, timestamp)

    url = "https://xmp-open.mobvista.com/v2/media/account/list"
    payload = {
        "client_id": CLIENT_ID,
        "timestamp": timestamp,
        "sign": sign,
        "channel": ["facebook"],
        "page": 1,
        "page_size": 10
    }

    print(f"URL: {url}")
    print(f"Payload: {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(url, json=payload, timeout=30)
        result = response.json()

        print(f"\n响应状态码: {response.status_code}")
        print(f"响应 code: {result.get('code')}")
        print(f"响应 msg: {result.get('msg')}")

        if result.get("code") == 0:
            data = result.get("data", {})
            accounts = data.get("list", [])
            print(f"\n获取到 {len(accounts)} 个账户")
            if accounts:
                print("\n账户示例:")
                acc = accounts[0]
                print(f"  account_id: {acc.get('account_id')}")
                print(f"  account_name: {acc.get('account_name')}")
                print(f"  channel: {acc.get('channel')}")
                print(f"  auth_status: {acc.get('auth_status')}")
            return True
        else:
            print(f"\n错误: {result}")
            return False

    except Exception as e:
        print(f"\n请求失败: {e}")
        return False


def test_get_report():
    """测试获取报表数据接口"""
    print("\n" + "=" * 50)
    print("测试 2: 获取报表数据")
    print("=" * 50)

    timestamp = int(time.time())
    sign = generate_sign(CLIENT_SECRET, timestamp)

    # 查询昨天的数据
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    url = "https://xmp-open.mobvista.com/v2/media/account/report"
    payload = {
        "client_id": CLIENT_ID,
        "timestamp": timestamp,
        "sign": sign,
        "start_date": yesterday,
        "end_date": yesterday,
        "dimension": ["date"],
        "metrics": ["impression", "click", "cost"],
        "currency": "USD",
        "page": 1,
        "page_size": 10
    }

    print(f"URL: {url}")
    print(f"查询日期: {yesterday}")
    print(f"Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")

    try:
        response = requests.post(url, json=payload, timeout=60)
        result = response.json()

        print(f"\n响应状态码: {response.status_code}")
        print(f"响应 code: {result.get('code')}")
        print(f"响应 msg: {result.get('msg')}")

        if result.get("code") == 0:
            data = result.get("data", {})
            records = data.get("list", [])
            print(f"\n获取到 {len(records)} 条记录")

            if records:
                print("\n数据示例:")
                print(json.dumps(records[0], indent=2, ensure_ascii=False))
            return True
        else:
            print(f"\n错误响应: {json.dumps(result, indent=2, ensure_ascii=False)}")
            return False

    except Exception as e:
        print(f"\n请求失败: {e}")
        return False


def main():
    print("XMP API 测试")
    print("=" * 50)

    # 检查配置
    if not CLIENT_ID or not CLIENT_SECRET:
        print("\n错误: 请在 .env 文件中配置:")
        print("  XMP_CLIENT_ID=your_client_id")
        print("  XMP_CLIENT_SECRET=your_client_secret")
        return

    print(f"Client ID: {CLIENT_ID[:8]}...")
    print(f"Client Secret: {CLIENT_SECRET[:8]}...")

    # 测试 1: 获取可用指标
    test1_ok = test_get_fields()

    if test1_ok:
        print("\n等待 6 秒避免限频...")
        time.sleep(6)

        # 测试 2: 获取广告账户
        test2_ok = test_get_accounts()

        if test2_ok:
            print("\n等待 6 秒避免限频...")
            time.sleep(6)

            # 测试 3: 获取报表数据
            test_get_report()


if __name__ == "__main__":
    main()
