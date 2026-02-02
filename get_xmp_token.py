"""
自动登录 XMP 并获取 Bearer Token
"""
import os
import time
import json
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()

XMP_USERNAME = os.getenv('XMP_USERNAME')
XMP_PASSWORD = os.getenv('XMP_PASSWORD')


def get_bearer_token():
    """自动登录 XMP 并获取 Bearer Token"""

    if not XMP_USERNAME or not XMP_PASSWORD:
        print("错误: 请配置 XMP_USERNAME 和 XMP_PASSWORD 环境变量")
        return None

    print(f"使用账号: {XMP_USERNAME}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # 显示浏览器方便调试
        context = browser.new_context()
        page = context.new_page()

        # 监听网络请求，捕获 Bearer Token
        bearer_token = None

        def handle_request(request):
            nonlocal bearer_token
            auth = request.headers.get('authorization', '')
            if auth.startswith('Bearer ') and len(auth) > 50:
                bearer_token = auth.replace('Bearer ', '')
                print(f"[捕获] 从请求头获取到 Token: {bearer_token[:30]}...")
            # 也检查小写的 header
            auth2 = request.headers.get('Authorization', '')
            if auth2.startswith('Bearer ') and len(auth2) > 50:
                bearer_token = auth2.replace('Bearer ', '')
                print(f"[捕获] 从 Authorization 获取到 Token: {bearer_token[:30]}...")

        page.on('request', handle_request)

        try:
            # 访问登录页面
            print("访问登录页面...")
            page.goto("https://xmp.mobvista.com/m/login", timeout=60000)
            time.sleep(2)

            # 输入用户名
            print("输入用户名...")
            page.fill('input[type="text"]', XMP_USERNAME)
            time.sleep(0.5)

            # 输入密码
            print("输入密码...")
            page.fill('input[type="password"]', XMP_PASSWORD)
            time.sleep(0.5)

            # 点击登录按钮
            print("点击登录...")
            login_selectors = [
                'button:has-text("Log in")',
                'button.el-button--primary',
                'button[type="submit"]',
            ]

            for selector in login_selectors:
                try:
                    page.click(selector, timeout=3000)
                    print(f"点击: {selector}")
                    break
                except:
                    continue

            # 等待登录完成
            print("等待登录完成...")
            time.sleep(5)

            # 访问一个需要认证的页面来触发 API 请求
            print("访问数据页面...")
            page.goto("https://xmp.mobvista.com/ads_manage/summary/account", timeout=60000)
            time.sleep(3)

            # 刷新页面触发更多 API 请求
            if not bearer_token:
                print("刷新页面捕获 Token...")
                page.reload()
                time.sleep(3)

            # 如果还没获取到 Token，尝试从 localStorage 获取
            if not bearer_token:
                print("尝试从 localStorage 获取 Token...")
                # 打印所有 localStorage 内容
                all_storage = page.evaluate('''() => {
                    const result = {};
                    for (let i = 0; i < localStorage.length; i++) {
                        const key = localStorage.key(i);
                        result[key] = localStorage.getItem(key);
                    }
                    return result;
                }''')
                print(f"localStorage 内容: {list(all_storage.keys())}")

                # 查找 token
                for key, val in all_storage.items():
                    if val and (val.startswith('eyJ') or 'token' in key.lower()):
                        print(f"  找到可能的 Token: {key} = {val[:50]}...")
                        if val.startswith('eyJ'):
                            bearer_token = val
                            break

            if bearer_token:
                print(f"\n获取到 Bearer Token!")
                print(f"Token 长度: {len(bearer_token)}")
                print(f"Token 前50字符: {bearer_token[:50]}...")
                return bearer_token
            else:
                print("未能获取到 Bearer Token")
                page.screenshot(path="xmp_login_debug.png")
                print("已保存截图: xmp_login_debug.png")
                return None

        except Exception as e:
            print(f"登录失败: {e}")
            page.screenshot(path="xmp_login_error.png")
            return None
        finally:
            browser.close()


if __name__ == '__main__':
    token = get_bearer_token()
    if token:
        print(f"\n完整 Token:\n{token}")
