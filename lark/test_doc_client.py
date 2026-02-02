"""
测试飞书文档客户端
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from lark.lark_doc_client import create_doc_client

print("="*60)
print("飞书文档客户端测试")
print("="*60)

# 创建客户端
try:
    client = create_doc_client()
    print("[OK] 客户端创建成功")
except Exception as e:
    print(f"[FAIL] 客户端创建失败: {e}")
    sys.exit(1)

# 测试获取 access_token
print("\n测试获取 access_token...")
try:
    token = client._get_tenant_access_token()
    print(f"[OK] 获取 token 成功: {token[:20]}...")
except Exception as e:
    print(f"[FAIL] 获取 token 失败: {e}")
    sys.exit(1)

# 测试文档操作 (需要配置 doc_token)
doc_token = os.getenv("WEEKLY_REPORT_DOC_TOKEN")
if doc_token:
    print(f"\n测试文档操作 (doc_token: {doc_token})...")
    try:
        info = client.get_document_info(doc_token)
        print(f"文档信息: {info}")
    except Exception as e:
        print(f"[FAIL] 获取文档信息失败: {e}")
else:
    print("\n[SKIP] 未配置 WEEKLY_REPORT_DOC_TOKEN，跳过文档操作测试")
    print("请在飞书创建一个文档，从 URL 获取 token 并配置到 .env")

print("\n" + "="*60)
print("测试完成")
