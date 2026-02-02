"""
测试 XMP 报表写入飞书文档
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv()

from lark.lark_doc_client import create_doc_client


def get_mock_optimizer_data():
    """模拟投手数据"""
    return [
        {
            'name': '张三',
            'meta_spend': 15000,
            'meta_revenue': 8500,
            'meta_roas': 0.567,
            'tt_spend': 8000,
            'tt_revenue': 5200,
            'tt_roas': 0.65,
            'total_spend': 23000,
            'total_revenue': 13700,
            'total_roas': 0.596,
            'label': 'Spend Top1'
        },
        {
            'name': '李四',
            'meta_spend': 12000,
            'meta_revenue': 7800,
            'meta_roas': 0.65,
            'tt_spend': 5000,
            'tt_revenue': 3800,
            'tt_roas': 0.76,
            'total_spend': 17000,
            'total_revenue': 11600,
            'total_roas': 0.682,
            'label': 'ROAS Top1'
        },
        {
            'name': '王五',
            'meta_spend': 8000,
            'meta_revenue': 4000,
            'meta_roas': 0.50,
            'tt_spend': 3000,
            'tt_revenue': 1500,
            'tt_roas': 0.50,
            'total_spend': 11000,
            'total_revenue': 5500,
            'total_roas': 0.50,
            'label': ''
        },
    ]


def get_mock_editor_data():
    """模拟剪辑师数据"""
    return [
        {
            'name': '剪辑A',
            'meta_spend': 20000,
            'meta_revenue': 12000,
            'meta_roas': 0.60,
            'tt_spend': 10000,
            'tt_revenue': 7000,
            'tt_roas': 0.70,
            'total_spend': 30000,
            'total_revenue': 19000,
            'total_roas': 0.633,
            'label': 'Spend Top1 | ROAS Top1'
        },
        {
            'name': '剪辑B',
            'meta_spend': 15000,
            'meta_revenue': 7500,
            'meta_roas': 0.50,
            'tt_spend': 8000,
            'tt_revenue': 4000,
            'tt_roas': 0.50,
            'total_spend': 23000,
            'total_revenue': 11500,
            'total_roas': 0.50,
            'label': ''
        },
    ]


def test_daily_report():
    """测试日报写入"""
    # 默认使用指定的 wiki token
    wiki_token = os.getenv("LARK_DOC_TOKEN", "U0tywFaZriyKUHkhwGUlPr6jgYg")

    client = create_doc_client()

    # 先获取 Wiki 节点的实际 doc_token
    print(f"[测试] 获取 Wiki 节点信息: {wiki_token}")
    node_info = client.get_wiki_node_info(wiki_token)
    print(f"[节点信息] {node_info}")

    if node_info.get("code") != 0:
        print(f"[错误] 获取 Wiki 节点失败: {node_info}")
        return

    node = node_info.get("data", {}).get("node", {})
    doc_token = node.get("obj_token")
    obj_type = node.get("obj_type")
    print(f"[文档] obj_token={doc_token}, obj_type={obj_type}")

    if obj_type != "docx":
        print(f"[错误] Wiki 节点类型不是文档: {obj_type}")
        return

    optimizer_data = get_mock_optimizer_data()
    editor_data = get_mock_editor_data()

    print("[测试] 写入 XMP 日报...")
    result = client.write_xmp_daily_report(
        doc_token=doc_token,
        date_str="2026-01-12",
        optimizer_data=optimizer_data,
        editor_data=editor_data
    )

    print(f"[结果] {result}")


def test_weekly_report():
    """测试周报写入"""
    doc_token = os.getenv("LARK_DOC_TOKEN")
    if not doc_token:
        print("[错误] 请设置 LARK_DOC_TOKEN 环境变量")
        return

    client = create_doc_client()

    optimizer_data = get_mock_optimizer_data()
    editor_data = get_mock_editor_data()

    # 最佳表现者
    best_optimizer = optimizer_data[1]  # 李四 ROAS 最高
    best_editor = editor_data[0]  # 剪辑A

    print("[测试] 写入 XMP 周报...")
    result = client.write_xmp_weekly_report(
        doc_token=doc_token,
        start_date="2026-01-06",
        end_date="2026-01-12",
        optimizer_data=optimizer_data,
        editor_data=editor_data,
        best_optimizer=best_optimizer,
        best_editor=best_editor
    )

    print(f"[结果] {result}")


def test_direct_doc():
    """测试直接写入普通文档（非 Wiki）"""
    # 普通文档的 doc_token 可以从 URL 获取
    # 例如: https://xxx.larksuite.com/docx/ABC123 中的 ABC123
    doc_token = os.getenv("LARK_DIRECT_DOC_TOKEN")

    if not doc_token:
        print("[提示] 请设置 LARK_DIRECT_DOC_TOKEN 环境变量")
        print("       或在飞书中创建普通文档，从 URL 获取 token")
        print("       URL 格式: https://xxx.larksuite.com/docx/{doc_token}")
        return

    client = create_doc_client()

    # 先测试获取文档信息
    print(f"[测试] 获取文档信息: {doc_token}")
    doc_info = client.get_document_info(doc_token)
    print(f"[文档信息] {doc_info}")

    if doc_info.get("code") != 0:
        print(f"[错误] 获取文档失败: {doc_info}")
        return

    optimizer_data = get_mock_optimizer_data()
    editor_data = get_mock_editor_data()

    print("[测试] 写入 XMP 日报到普通文档...")
    result = client.write_xmp_daily_report(
        doc_token=doc_token,
        date_str="2026-01-12",
        optimizer_data=optimizer_data,
        editor_data=editor_data
    )

    print(f"[结果] {result}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='测试 XMP 报表写入飞书文档')
    parser.add_argument('--daily', action='store_true', help='测试日报 (Wiki)')
    parser.add_argument('--weekly', action='store_true', help='测试周报 (Wiki)')
    parser.add_argument('--direct', action='store_true', help='测试普通文档 (非 Wiki)')
    args = parser.parse_args()

    if args.daily:
        test_daily_report()
    elif args.weekly:
        test_weekly_report()
    elif args.direct:
        test_direct_doc()
    else:
        print("用法:")
        print("  python test_xmp_doc_report.py --daily   # 测试日报 (Wiki)")
        print("  python test_xmp_doc_report.py --weekly  # 测试周报 (Wiki)")
        print("  python test_xmp_doc_report.py --direct  # 测试普通文档")