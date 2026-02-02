"""
检查 TikTok 广告命名格式
找出为什么从 14:33 开始无法识别剪辑师
"""
import asyncio
import os
from dotenv import load_dotenv
from xmp.xmp_scheduler import XMPMultiChannelScraper, extract_editor_from_ad_name

load_dotenv()

async def check_tiktok_ads():
    """检查 TikTok 广告数据和命名格式"""
    scraper = XMPMultiChannelScraper()

    print("=" * 80)
    print("获取 2026-01-16 TikTok 广告数据...")
    print("=" * 80)

    # 获取 TikTok 广告数据
    ads = await scraper.fetch_channel_ads('tiktok', '2026-01-16', '2026-01-16')

    if not ads:
        print("未获取到 TikTok 广告数据！")
        return

    print(f"\n总广告数: {len(ads)}")
    print(f"总消耗: ${sum(ad['cost'] for ad in ads):,.2f}")

    # 统计能识别和不能识别的广告
    recognized = []
    unrecognized = []

    for ad in ads:
        ad_name = ad.get('ad_name', '')
        editor = extract_editor_from_ad_name(ad_name)

        if editor:
            recognized.append({
                'ad_name': ad_name,
                'editor': editor,
                'cost': ad['cost']
            })
        else:
            unrecognized.append({
                'ad_name': ad_name,
                'cost': ad['cost']
            })

    print(f"\n可识别广告: {len(recognized)} 条 (${sum(a['cost'] for a in recognized):,.2f})")
    print(f"无法识别广告: {len(unrecognized)} 条 (${sum(a['cost'] for a in unrecognized):,.2f})")

    # 显示可识别的广告（按剪辑师分组）
    print("\n" + "=" * 80)
    print("可识别的广告（按剪辑师分组）")
    print("=" * 80)

    editor_ads = {}
    for ad in recognized:
        editor = ad['editor']
        if editor not in editor_ads:
            editor_ads[editor] = []
        editor_ads[editor].append(ad)

    for editor, ads_list in sorted(editor_ads.items()):
        total_cost = sum(a['cost'] for a in ads_list)
        print(f"\n{editor}: {len(ads_list)} 条广告, ${total_cost:,.2f}")
        # 显示前 3 条
        for ad in ads_list[:3]:
            print(f"  - {ad['ad_name']} (${ad['cost']:.2f})")
        if len(ads_list) > 3:
            print(f"  ... 还有 {len(ads_list) - 3} 条")

    # 显示无法识别的广告（重点！）
    print("\n" + "=" * 80)
    print("无法识别的广告（前 30 条）")
    print("=" * 80)

    # 按消耗排序
    unrecognized.sort(key=lambda x: x['cost'], reverse=True)

    for i, ad in enumerate(unrecognized[:30]):
        print(f"\n{i+1}. {ad['ad_name']}")
        print(f"   消耗: ${ad['cost']:.2f}")

if __name__ == '__main__':
    asyncio.run(check_tiktok_ads())
