"""调试 Facebook designer API - 查看所有剪辑师名字"""
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
            data_list = result.get('data', {}).get('list', [])

            print(f"获取到 {len(data_list)} 条记录")
            print("\n所有剪辑师名字:")
            for item in data_list:
                name = item.get('designer_name', '')
                cost = float(item.get('currency_cost', 0) or 0)
                print(f"  {name}: ${cost:,.2f}")

if __name__ == '__main__':
    asyncio.run(main())
