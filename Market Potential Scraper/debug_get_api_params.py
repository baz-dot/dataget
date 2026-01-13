"""
调试工具：手动获取 API 请求参数
打开浏览器，手动搜索一次，然后从控制台复制参数
"""
import os
import json
from dotenv import load_dotenv
from dataeye_scraper import DataEyeScraper

load_dotenv()

def debug_api_params():
    """调试：手动获取 API 参数"""
    print("="*60)
    print("调试工具：获取 API 请求参数")
    print("="*60)

    with DataEyeScraper(headless=False) as scraper:
        # 登录
        print("\n步骤 1: 登录...")
        if not scraper.login():
            print("登录失败")
            return

        print("✓ 登录成功")

        # 访问页面
        print("\n步骤 2: 访问素材页面...")
        scraper.page.goto(scraper.target_url, timeout=60000)

        print("\n" + "="*60)
        print("请在浏览器中执行以下操作：")
        print("="*60)
        print("1. 按 F12 打开开发者工具")
        print("2. 切换到 Network（网络）标签")
        print("3. 在页面上搜索一个短剧（如：天降萌宝老祖）")
        print("4. 在 Network 中找到 'searchCreative' 请求")
        print("5. 点击该请求，查看 Payload（请求负载）")
        print("6. 复制完整的 JSON 参数")
        print("="*60)

        input("\n完成后按 Enter 继续...")

        print("\n请粘贴复制的 JSON 参数（输入完成后按 Enter，然后输入 'END' 并按 Enter）：")
        lines = []
        while True:
            line = input()
            if line.strip() == 'END':
                break
            lines.append(line)

        json_str = '\n'.join(lines)

        try:
            params = json.loads(json_str)
            print("\n✓ 参数解析成功！")
            print(json.dumps(params, indent=2, ensure_ascii=False))

            # 保存到文件
            with open('api_params_template.json', 'w', encoding='utf-8') as f:
                json.dump(params, f, indent=2, ensure_ascii=False)
            print("\n✓ 参数已保存到 api_params_template.json")

        except Exception as e:
            print(f"\n✗ 解析失败: {e}")

if __name__ == '__main__':
    debug_api_params()
