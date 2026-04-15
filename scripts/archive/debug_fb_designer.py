"""调试 Facebook designer API 返回数据"""
import asyncio
import aiohttp

BEARER_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3NjkwOTQwOTQsImV4cCI6MTc3MDM5MDA5NCwibmJmIjoxNzY5MDk0MDk0LCJkYXRhIjp7ImlkIjoiMjk0ODMifX0.-Gx41oXVvdNnr05pwsRrg-2KXc4abgTHjVYDIGwJ6Go"

async def main():
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json",
        "Origin": "https://xmp.mobvista.com",
        "Referer": "https://xmp.mobvista.com/"
    }

    payload = {
        "level": "designer",
        "channel": "facebook",
        "start_time": "2026-01-05",
        "end_time": "2026-01-05",
        "field": "cost,purchase_value,designer_name",
        "page": 1,
        "page_size": 1000,
        "report_timezone": "",
        "search": [
            {"item": "is_xmp", "val": "0", "op": "EQ"}
        ],
        "source": "report"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://xmp-api.mobvista.com/admanage/channel/list",
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            result = await response.json()

            print("API Response code:", result.get('code'))
            print("API Response msg:", result.get('msg'))

            data_list = result.get('data', {}).get('list', [])
            print(f"\n获取到 {len(data_list)} 条记录")

            # 打印前3条记录的所有字段
            print("\n前3条记录的字段:")
            for i, item in enumerate(data_list[:3]):
                print(f"\n--- 记录 {i+1} ---")
                for key, val in item.items():
                    print(f"  {key}: {val}")

if __name__ == '__main__':
    asyncio.run(main())
