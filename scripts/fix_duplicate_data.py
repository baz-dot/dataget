"""
修复 2025-12-24 的重复数据问题
1. 删除重复的 20251224_130036 batch
2. 重新导入正确的 13:00 数据
"""
import os
from dotenv import load_dotenv
load_dotenv()

from bigquery_storage import BigQueryUploader

project_id = os.getenv('BQ_PROJECT_ID')
bq = BigQueryUploader(project_id, "quickbi_data")

print("=" * 60)
print("修复 2025-12-24 的重复数据")
print("=" * 60)

table_ref = f"{project_id}.quickbi_data.quickbi_campaigns"

# 1. 删除重复的 batch
print("\n[1] 删除重复的 20251224_130036 batch...")
delete_query = f"""
DELETE FROM `{table_ref}`
WHERE stat_date = '2025-12-24'
  AND batch_id = '20251224_130036'
"""

try:
    job = bq.client.query(delete_query)
    job.result()  # 等待完成
    print(f"[OK] 已删除 batch_id = 20251224_130036 的数据")
except Exception as e:
    print(f"[FAIL] 删除失败: {e}")
    exit(1)

# 2. 验证删除结果
print("\n[2] 验证删除结果...")
verify_query = f"""
SELECT COUNT(*) as count
FROM `{table_ref}`
WHERE stat_date = '2025-12-24'
  AND batch_id = '20251224_130036'
"""

result = list(bq.client.query(verify_query).result())
if result[0].count == 0:
    print("[OK] 数据已成功删除")
else:
    print(f"[WARNING] 仍有 {result[0].count} 条数据")

print("\n" + "=" * 60)
print("修复完成！")
print("=" * 60)
print("\n下一步：运行 import_historical_data.py 重新导入正确的数据")
