"""测试 XMP API campaign_id 维度"""
from xmp.xmp_api import XMPApiClient
from dotenv import load_dotenv
from datetime import datetime, timedelta
load_dotenv()

client = XMPApiClient()
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# 不用 campaign_id 维度，直接用 date 维度，数据量小
data = client.fetch_report_data(
    start_date=yesterday,
    end_date=yesterday,
    metrics=['cost', 'impression'],
    dimension=['date'],
    module='facebook',
    currency='USD',
    page_size=10
)

if data:
    print('字段:', list(data[0].keys()))
    print('数据:', data[0])
else:
    print('无数据')
