"""
从 Excel 文件读取日报数据并上传到飞书文档
"""
import sys
sys.path.insert(0, '.')

import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from lark.lark_doc_client import create_doc_client

load_dotenv()

# Excel 文件路径
EXCEL_FILE = "daily_report_2026-01-05_to_2026-01-18.xlsx"

# 飞书 Wiki token
WIKI_TOKEN = "XMYlw4GkDicYhwk3VWglkF0DgZb"


def add_labels(data_list, spend_key='total_spend', roas_key='total_roas'):
    """添加 ROAS Top1 和 Spend Top1 标注"""
    if not data_list:
        return data_list

    max_spend = max(d.get(spend_key, 0) for d in data_list)
    max_roas = max(d.get(roas_key, 0) for d in data_list)

    for d in data_list:
        parts = []
        if d.get(spend_key, 0) == max_spend and max_spend > 0:
            parts.append('Spend Top1')
        if d.get(roas_key, 0) == max_roas and max_roas > 0:
            parts.append('ROAS Top1')
        d['label'] = ', '.join(parts) if parts else ''

    return data_list


def read_excel_data(excel_file):
    """从 Excel 读取数据"""
    # 读取两个 sheet
    optimizer_df = pd.read_excel(excel_file, sheet_name='投手日报')
    editor_df = pd.read_excel(excel_file, sheet_name='剪辑师日报')

    print(f"读取投手数据: {len(optimizer_df)} 条")
    print(f"读取剪辑师数据: {len(editor_df)} 条")

    return optimizer_df, editor_df


def convert_df_to_dict(df, name_col, is_optimizer=True):
    """将 DataFrame 转换为字典列表"""
    result = []
    for _, row in df.iterrows():
        item = {
            'name': row[name_col],
            'meta_spend': row['Meta Spend'] if pd.notna(row['Meta Spend']) else 0,
            'meta_roas': row['Meta ROAS'] if pd.notna(row['Meta ROAS']) else 0,
            'tt_spend': row['TT Spend'] if pd.notna(row['TT Spend']) else 0,
            'tt_roas': row['TT ROAS'] if pd.notna(row['TT ROAS']) else 0,
            'total_spend': row['总 Spend'] if pd.notna(row['总 Spend']) else 0,
            'total_roas': row['总 ROAS'] if pd.notna(row['总 ROAS']) else 0,
            'label': row['标注'] if pd.notna(row['标注']) else ''
        }
        result.append(item)
    return result


def main():
    # 创建飞书文档客户端
    try:
        client = create_doc_client()
    except ValueError as e:
        print(f"错误: {e}")
        print("请配置 LARK_APP_ID 和 LARK_APP_SECRET 环境变量")
        return

    # 获取 Wiki 节点的实际文档 token
    print("获取 Wiki 节点信息...")
    node_info = client.get_wiki_node_info(WIKI_TOKEN)
    if node_info.get("code") != 0:
        print(f"获取 Wiki 节点失败: {node_info}")
        return

    node = node_info.get("data", {}).get("node", {})
    doc_token = node.get("obj_token")
    obj_type = node.get("obj_type")
    print(f"Wiki 节点: obj_token={doc_token}, obj_type={obj_type}")

    if obj_type != "docx":
        print(f"Wiki 节点类型不是文档: {obj_type}")
        return

    # 读取 Excel 数据
    print(f"\n读取 Excel 文件: {EXCEL_FILE}")
    optimizer_df, editor_df = read_excel_data(EXCEL_FILE)

    # 获取所有日期
    dates = sorted(optimizer_df['日期'].unique())
    print(f"\n日期范围: {dates[0]} 到 {dates[-1]}")
    print(f"共 {len(dates)} 天")
    print("=" * 60)

    # 逐天上传
    for date_str in dates:
        print(f"\n--- 处理 {date_str} ---")

        # 筛选当天数据
        opt_day = optimizer_df[optimizer_df['日期'] == date_str]
        ed_day = editor_df[editor_df['日期'] == date_str]

        # 转换为字典列表
        optimizer_data = convert_df_to_dict(opt_day, '投手', is_optimizer=True)
        editor_data = convert_df_to_dict(ed_day, '剪辑师', is_optimizer=False)

        # 重新计算标注
        optimizer_data = add_labels(optimizer_data)
        editor_data = add_labels(editor_data)

        print(f"[{date_str}] 投手: {len(optimizer_data)} 人, 剪辑师: {len(editor_data)} 人")

        # 上传到飞书
        print(f"[{date_str}] 上传到飞书...")
        result = client.write_xmp_daily_report(
            doc_token,
            str(date_str)[:10],  # 确保日期格式正确
            optimizer_data,
            editor_data
        )

        if result.get("code") == 0:
            print(f"[{date_str}] 上传成功!")
        else:
            print(f"[{date_str}] 上传失败: {result}")

    print(f"\n{'=' * 60}")
    print("全部上传完成!")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
