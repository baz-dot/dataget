"""
列出 BigQuery 项目中的所有表
"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

project_id = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
client = bigquery.Client(project=project_id)

print(f"=== BigQuery 项目中的所有数据集和表 ===\n")

# 列出所有数据集
datasets = list(client.list_datasets())

if not datasets:
    print("项目中没有数据集")
else:
    for dataset in datasets:
        dataset_id = dataset.dataset_id
        print(f"\n数据集: {dataset_id}")
        print("-" * 80)

        # 列出数据集中的所有表
        tables = client.list_tables(f"{project_id}.{dataset_id}")

        for table in tables:
            print(f"  - {table.table_id} ({table.table_type})")
