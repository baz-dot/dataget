"""
生成投手和剪辑师日报 Excel
- 投手: 2026-01-05 到 2026-01-18
- 剪辑师: 2026-01-11 到 2026-01-18
- 字段: Meta Spend, Meta ROAS, TT Spend, TT ROAS, 总 Spend, 总 ROAS, 标注
- 标注: ROAS Top1, Spend Top1
"""
import os
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd

load_dotenv()

PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
client = bigquery.Client(project=PROJECT_ID)


def query_optimizer_daily_data():
    """查询投手每日数据"""
    query = f"""
    WITH daily_data AS (
        SELECT
            stat_date,
            optimizer,
            -- Meta 数据
            SUM(CASE WHEN LOWER(channel) IN ('meta', 'facebook') THEN spend ELSE 0 END) as meta_spend,
            SUM(CASE WHEN LOWER(channel) IN ('meta', 'facebook') THEN new_user_revenue ELSE 0 END) as meta_revenue,
            -- TT 数据
            SUM(CASE WHEN LOWER(channel) = 'tiktok' THEN spend ELSE 0 END) as tt_spend,
            SUM(CASE WHEN LOWER(channel) = 'tiktok' THEN new_user_revenue ELSE 0 END) as tt_revenue,
            -- 总计
            SUM(spend) as total_spend,
            SUM(new_user_revenue) as total_revenue
        FROM `{PROJECT_ID}.quickbi_data.quickbi_campaigns` t1
        WHERE stat_date BETWEEN '2026-01-05' AND '2026-01-18'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM `{PROJECT_ID}.quickbi_data.quickbi_campaigns` t2
              WHERE t2.stat_date = t1.stat_date
          )
        GROUP BY stat_date, optimizer
    ),
    daily_ranks AS (
        SELECT
            stat_date,
            optimizer,
            meta_spend,
            SAFE_DIVIDE(meta_revenue, meta_spend) as meta_roas,
            tt_spend,
            SAFE_DIVIDE(tt_revenue, tt_spend) as tt_roas,
            total_spend,
            SAFE_DIVIDE(total_revenue, total_spend) as total_roas,
            -- 排名
            RANK() OVER (PARTITION BY stat_date ORDER BY total_spend DESC) as spend_rank,
            RANK() OVER (PARTITION BY stat_date ORDER BY SAFE_DIVIDE(total_revenue, total_spend) DESC) as roas_rank
        FROM daily_data
        WHERE total_spend > 0
    )
    SELECT
        stat_date,
        optimizer,
        meta_spend,
        meta_roas,
        tt_spend,
        tt_roas,
        total_spend,
        total_roas,
        spend_rank,
        roas_rank
    FROM daily_ranks
    ORDER BY stat_date, total_spend DESC
    """
    return client.query(query).to_dataframe()


def query_editor_daily_data():
    """查询剪辑师每日数据"""
    query = f"""
    WITH daily_data AS (
        SELECT
            stat_date,
            editor_name,
            -- Meta 数据
            SUM(CASE WHEN LOWER(channel) IN ('meta', 'facebook') THEN spend ELSE 0 END) as meta_spend,
            SUM(CASE WHEN LOWER(channel) IN ('meta', 'facebook') THEN revenue ELSE 0 END) as meta_revenue,
            -- TT 数据
            SUM(CASE WHEN LOWER(channel) = 'tiktok' THEN spend ELSE 0 END) as tt_spend,
            SUM(CASE WHEN LOWER(channel) = 'tiktok' THEN revenue ELSE 0 END) as tt_revenue,
            -- 总计
            SUM(spend) as total_spend,
            SUM(revenue) as total_revenue
        FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats` t1
        WHERE stat_date BETWEEN '2026-01-11' AND '2026-01-18'
          AND batch_id = (
              SELECT MAX(batch_id)
              FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats` t2
              WHERE t2.stat_date = t1.stat_date
          )
        GROUP BY stat_date, editor_name
    ),
    daily_ranks AS (
        SELECT
            stat_date,
            editor_name,
            meta_spend,
            SAFE_DIVIDE(meta_revenue, meta_spend) as meta_roas,
            tt_spend,
            SAFE_DIVIDE(tt_revenue, tt_spend) as tt_roas,
            total_spend,
            SAFE_DIVIDE(total_revenue, total_spend) as total_roas,
            -- 排名
            RANK() OVER (PARTITION BY stat_date ORDER BY total_spend DESC) as spend_rank,
            RANK() OVER (PARTITION BY stat_date ORDER BY SAFE_DIVIDE(total_revenue, total_spend) DESC) as roas_rank
        FROM daily_data
        WHERE total_spend > 0
    )
    SELECT
        stat_date,
        editor_name,
        meta_spend,
        meta_roas,
        tt_spend,
        tt_roas,
        total_spend,
        total_roas,
        spend_rank,
        roas_rank
    FROM daily_ranks
    ORDER BY stat_date, total_spend DESC
    """
    return client.query(query).to_dataframe()


def add_labels(df, name_col):
    """添加标注列"""
    labels = []
    for _, row in df.iterrows():
        label_parts = []
        if row['spend_rank'] == 1:
            label_parts.append('Spend Top1')
        if row['roas_rank'] == 1:
            label_parts.append('ROAS Top1')
        labels.append(', '.join(label_parts) if label_parts else '')
    df['标注'] = labels
    return df


def format_dataframe(df, name_col):
    """格式化 DataFrame"""
    # 添加标注
    df = add_labels(df, name_col)

    # 重命名列
    df = df.rename(columns={
        'stat_date': '日期',
        name_col: '姓名',
        'meta_spend': 'Meta Spend',
        'meta_roas': 'Meta ROAS',
        'tt_spend': 'TT Spend',
        'tt_roas': 'TT ROAS',
        'total_spend': '总 Spend',
        'total_roas': '总 ROAS'
    })

    # 选择需要的列
    columns = ['日期', '姓名', 'Meta Spend', 'Meta ROAS', 'TT Spend', 'TT ROAS', '总 Spend', '总 ROAS', '标注']
    df = df[columns]

    return df


def main():
    print("=" * 60)
    print("生成投手和剪辑师日报")
    print("=" * 60)

    # 查询投手数据
    print("\n[1] 查询投手数据 (2026-01-05 ~ 2026-01-18)...")
    optimizer_df = query_optimizer_daily_data()
    print(f"    获取 {len(optimizer_df)} 条记录")

    # 查询剪辑师数据
    print("\n[2] 查询剪辑师数据 (2026-01-11 ~ 2026-01-18)...")
    editor_df = query_editor_daily_data()
    print(f"    获取 {len(editor_df)} 条记录")

    # 格式化数据
    print("\n[3] 格式化数据...")
    optimizer_df = format_dataframe(optimizer_df, 'optimizer')
    editor_df = format_dataframe(editor_df, 'editor_name')

    # 生成 Excel
    output_file = f"daily_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    print(f"\n[4] 生成 Excel: {output_file}")

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        optimizer_df.to_excel(writer, sheet_name='投手日报', index=False)
        editor_df.to_excel(writer, sheet_name='剪辑师日报', index=False)

    print(f"\n完成! 文件已保存: {output_file}")
    print("=" * 60)


if __name__ == '__main__':
    main()
