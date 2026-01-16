"""
XMP TikTok 支付完成总价值 抓取脚本
通过 XMP 内部 API 获取 TikTok 渠道的 total_complete_payment_rate 字段

保障措施:
- 失败告警: API 调用失败时发飞书通知
- Token 提前刷新: 不等过期，提前刷新
- 重试机制: 失败自动重试 3 次
- 数据校验: 返回数据异常时告警
"""

import os
import sys
import json
import asyncio
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

# 飞书告警配置
LARK_ALERT_WEBHOOK = os.getenv('LARK_ALERT_WEBHOOK') or os.getenv('LARK_WEBHOOK_URL')
LARK_ALERT_SECRET = os.getenv('LARK_ALERT_SECRET') or os.getenv('LARK_SECRET')

# Token 有效期配置 (天)
TOKEN_VALID_DAYS = 15
TOKEN_REFRESH_BEFORE_DAYS = 3  # 提前 3 天刷新

# 重试配置
MAX_RETRIES = 3
RETRY_DELAYS = [5, 15, 30]  # 重试间隔 (秒)

# XMP 配置
XMP_USERNAME = os.getenv('XMP_USERNAME')
XMP_PASSWORD = os.getenv('XMP_PASSWORD')
XMP_COOKIES_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'xmp_cookies.json')
XMP_TOKEN_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'xmp_token.json')

# API 端点
XMP_SUMMARY_URL = "https://xmp-api.mobvista.com/admanage/channel/summary"
XMP_LOGIN_URL = "https://xmp.mobvista.com/"
XMP_TIKTOK_PAGE = "https://xmp.mobvista.com/ads_manage/tiktok/account"


def send_lark_alert(title: str, content: str, level: str = "warning"):
    """
    发送飞书告警通知

    Args:
        title: 告警标题
        content: 告警内容
        level: 告警级别 (info/warning/error)
    """
    if not LARK_ALERT_WEBHOOK:
        print(f"[告警] 未配置飞书 Webhook，跳过告警: {title}")
        return False

    # 告警级别对应的颜色和图标
    level_config = {
        "info": {"color": "blue", "icon": "ℹ️"},
        "warning": {"color": "orange", "icon": "⚠️"},
        "error": {"color": "red", "icon": "🚨"},
    }
    config = level_config.get(level, level_config["warning"])

    # 构建消息
    msg = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"{config['icon']} {title}"},
                "template": config["color"]
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": content}},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}}
            ]
        }
    }

    try:
        resp = requests.post(LARK_ALERT_WEBHOOK, json=msg, timeout=10)
        if resp.status_code == 200:
            print(f"[告警] 已发送飞书通知: {title}")
            return True
        else:
            print(f"[告警] 发送失败: {resp.text}")
            return False
    except Exception as e:
        print(f"[告警] 发送异常: {e}")
        return False


class XMPTikTokScraper:
    """XMP TikTok 数据抓取器 (带保障措施)"""

    def __init__(self):
        self.bearer_token = None
        self.token_updated_at = None
        self.cookies = None
        self._load_token()

    def _load_token(self) -> bool:
        """从文件加载 Token，检查是否需要刷新"""
        if not os.path.exists(XMP_TOKEN_FILE):
            return False

        try:
            with open(XMP_TOKEN_FILE, 'r') as f:
                data = json.load(f)

            self.bearer_token = data.get('token')
            updated_str = data.get('updated')

            if updated_str:
                self.token_updated_at = datetime.fromisoformat(updated_str)

            if self.bearer_token:
                # 检查 Token 是否需要刷新
                if self._should_refresh_token():
                    print(f"[XMP] Token 即将过期，需要刷新")
                    return False
                print(f"[XMP] 已加载保存的 Token")
                return True

        except Exception as e:
            print(f"[XMP] 加载 Token 失败: {e}")

        return False

    def _should_refresh_token(self) -> bool:
        """检查 Token 是否需要刷新 (提前 N 天刷新)"""
        if not self.token_updated_at:
            return True

        days_since_update = (datetime.now() - self.token_updated_at).days
        refresh_threshold = TOKEN_VALID_DAYS - TOKEN_REFRESH_BEFORE_DAYS

        if days_since_update >= refresh_threshold:
            print(f"[XMP] Token 已使用 {days_since_update} 天，超过刷新阈值 {refresh_threshold} 天")
            return True

        print(f"[XMP] Token 已使用 {days_since_update} 天，有效期内")
        return False

    def _save_token(self, token: str):
        """保存 Token 到文件"""
        try:
            data = {
                'token': token,
                'updated': datetime.now().isoformat()
            }
            with open(XMP_TOKEN_FILE, 'w') as f:
                json.dump(data, f)
            self.token_updated_at = datetime.now()
            print(f"[XMP] Token 已保存")
        except Exception as e:
            print(f"[XMP] 保存 Token 失败: {e}")
            send_lark_alert(
                "XMP Token 保存失败",
                f"**错误**: {e}",
                level="error"
            )

    async def login_and_get_token(self, headless: bool = False) -> Optional[str]:
        """
        登录 XMP 并获取 Bearer Token
        """
        from playwright.async_api import async_playwright

        print("[XMP] 启动浏览器登录...")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )

            # 加载已保存的 cookies
            if os.path.exists(XMP_COOKIES_FILE):
                try:
                    with open(XMP_COOKIES_FILE, 'r') as f:
                        cookies = json.load(f)
                    await context.add_cookies(cookies)
                    print(f"[XMP] 已加载 {len(cookies)} 个 cookies")
                except Exception as e:
                    print(f"[XMP] 加载 cookies 失败: {e}")

            page = await context.new_page()

            # 用于捕获 Bearer Token
            captured_token = None

            async def capture_request(request):
                nonlocal captured_token
                auth_header = request.headers.get('authorization', '')
                if auth_header.startswith('Bearer ') and not captured_token:
                    captured_token = auth_header
                    print(f"[XMP] 捕获到 Bearer Token (长度: {len(auth_header)})")

            page.on('request', capture_request)

            # 直接访问 TikTok 广告账户页面
            print("[XMP] 访问 TikTok 广告账户页面...")
            try:
                await page.goto(XMP_TIKTOK_PAGE, wait_until='domcontentloaded', timeout=60000)
            except Exception as e:
                print(f"[XMP] 页面加载超时，继续等待: {e}")

            await asyncio.sleep(3)

            # 检查是否需要登录
            current_url = page.url
            print(f"[XMP] 当前 URL: {current_url}")

            if 'login' in current_url.lower():
                print("[XMP] 需要登录，输入凭证...")
                await asyncio.sleep(2)

                # 等待登录表单
                try:
                    await page.wait_for_selector('input[type="password"]', timeout=10000)
                except:
                    pass

                # 输入用户名
                email_input = page.locator('input[type="text"]').first
                await email_input.fill(XMP_USERNAME)
                await asyncio.sleep(0.5)

                # 输入密码
                pwd_input = page.locator('input[type="password"]').first
                await pwd_input.fill(XMP_PASSWORD)
                await asyncio.sleep(0.5)

                # 点击登录按钮 - 尝试多种选择器
                print("[XMP] 尝试点击登录按钮...")
                try:
                    # 方式1: 按钮文本
                    await page.click('button:has-text("登录")', timeout=5000)
                except:
                    try:
                        # 方式2: submit 按钮
                        await page.click('button[type="submit"]', timeout=5000)
                    except:
                        try:
                            # 方式3: 任意按钮
                            await page.click('button.ant-btn-primary', timeout=5000)
                        except:
                            # 方式4: 回车提交
                            await pwd_input.press('Enter')

                print("[XMP] 已提交登录，等待跳转...")
                await asyncio.sleep(5)

                # 登录后再次访问 TikTok 页面
                print("[XMP] 登录后访问 TikTok 广告账户页面...")
                await page.goto(XMP_TIKTOK_PAGE, wait_until='domcontentloaded', timeout=60000)

            # 等待页面加载和 API 请求
            print("[XMP] 等待页面数据加载...")
            await asyncio.sleep(8)

            # 如果还没捕获到，尝试刷新页面
            if not captured_token:
                print("[XMP] 未捕获到 Token，尝试刷新页面...")
                await page.reload(wait_until='domcontentloaded')
                await asyncio.sleep(5)

            # 保存 cookies
            cookies = await context.cookies()
            with open(XMP_COOKIES_FILE, 'w') as f:
                json.dump(cookies, f, indent=2)
            print(f"[XMP] 已保存 {len(cookies)} 个 cookies")

            # 保存 Token
            if captured_token:
                self._save_token(captured_token)
                self.bearer_token = captured_token
            else:
                print("[XMP] 警告: 未能捕获到 Bearer Token")

            await browser.close()
            return captured_token

    async def fetch_tiktok_summary(
        self,
        start_date: str = None,
        end_date: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        获取 TikTok 渠道汇总数据 (带重试和数据校验)

        Args:
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD

        Returns:
            包含 total_complete_payment_rate 的汇总数据
        """
        import aiohttp

        # 检查 Token，必要时刷新
        if not self.bearer_token or self._should_refresh_token():
            print("[XMP] 需要获取/刷新 Token...")
            await self.login_and_get_token()

        if not self.bearer_token:
            error_msg = "登录失败，无法获取数据"
            print(f"[XMP] {error_msg}")
            send_lark_alert("XMP 登录失败", f"**错误**: {error_msg}", level="error")
            return None

        # 默认查询今天
        if not start_date:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date

        print(f"[XMP] 查询 TikTok 汇总数据: {start_date} ~ {end_date}")

        # 请求参数
        payload = {
            "level": "account",
            "channel": "tiktok",
            "start_time": start_date,
            "end_time": end_date,
            "field": "account_name,account_id,cost,impression,cpm,cpc,ctr,conversion,cpi,total_complete_payment_rate,total_purchase_value",
            "page": 1,
            "page_size": 100,
            "report_timezone": ""
        }

        headers = {
            "Authorization": self.bearer_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Language": "zh-CN",
            "Origin": "https://xmp.mobvista.com",
            "Referer": "https://xmp.mobvista.com/"
        }

        # 带重试的 API 请求
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        XMP_SUMMARY_URL,
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        result = await response.json()

                        # Token 过期检测
                        if result.get('code') in [401, 403, -1]:
                            error_msg = result.get('msg', 'Token 无效')
                            print(f"[XMP] Token 可能过期: {error_msg}")

                            # 尝试重新登录
                            if attempt < MAX_RETRIES - 1:
                                print(f"[XMP] 尝试重新登录 ({attempt + 1}/{MAX_RETRIES})...")
                                self.bearer_token = None
                                await self.login_and_get_token()
                                if self.bearer_token:
                                    headers["Authorization"] = self.bearer_token
                                    await asyncio.sleep(RETRY_DELAYS[attempt])
                                    continue

                            send_lark_alert(
                                "XMP Token 失效",
                                f"**错误**: {error_msg}\n**重试次数**: {attempt + 1}",
                                level="error"
                            )
                            return None

                        if result.get('code') == 0:
                            data = result.get('data', {})
                            sum_data = data.get('sum', {})

                            # 数据校验
                            validation_result = self._validate_data(sum_data, start_date)
                            if not validation_result['valid']:
                                send_lark_alert(
                                    "XMP 数据异常",
                                    validation_result['message'],
                                    level="warning"
                                )

                            cost = float(sum_data.get('cost', 0))
                            revenue = float(sum_data.get('total_complete_payment_rate', 0))

                            print(f"[XMP] 获取成功!")
                            print(f"  - 总消耗: ${cost:,.2f}")
                            print(f"  - 支付完成总价值: ${revenue:,.2f}")

                            return {
                                'date': start_date,
                                'cost': cost,
                                'total_complete_payment_rate': revenue,
                                'total_purchase_value': float(sum_data.get('total_purchase_value', 0)),
                                'impression': int(sum_data.get('impression', 0)),
                                'click': int(sum_data.get('click', 0)),
                                'conversion': float(sum_data.get('conversion', 0)),
                                'raw_data': sum_data
                            }
                        else:
                            last_error = result.get('msg', 'Unknown error')
                            print(f"[XMP] API 错误: {last_error}")

            except asyncio.TimeoutError:
                last_error = "请求超时"
                print(f"[XMP] {last_error} ({attempt + 1}/{MAX_RETRIES})")
            except Exception as e:
                last_error = str(e)
                print(f"[XMP] 请求失败: {last_error} ({attempt + 1}/{MAX_RETRIES})")

            # 重试等待
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                print(f"[XMP] {delay} 秒后重试...")
                await asyncio.sleep(delay)

        # 所有重试都失败，发送告警
        send_lark_alert(
            "XMP API 调用失败",
            f"**日期**: {start_date}\n**错误**: {last_error}\n**重试次数**: {MAX_RETRIES}",
            level="error"
        )
        return None

    def _validate_data(self, data: Dict, date: str) -> Dict[str, Any]:
        """
        校验返回数据是否正常

        Returns:
            {'valid': bool, 'message': str}
        """
        issues = []

        cost = float(data.get('cost', 0))
        revenue = float(data.get('total_complete_payment_rate', 0))
        impression = int(data.get('impression', 0))

        # 校验1: 消耗为 0 但有展示
        if cost == 0 and impression > 1000:
            issues.append(f"消耗为 0 但展示数 {impression:,}")

        # 校验2: ROAS 异常高 (>500%)
        if cost > 0:
            roas = revenue / cost
            if roas > 5:
                issues.append(f"ROAS 异常高: {roas*100:.1f}%")

        # 校验3: 消耗异常大 (单日 > $100,000)
        if cost > 100000:
            issues.append(f"单日消耗异常: ${cost:,.2f}")

        if issues:
            return {
                'valid': False,
                'message': f"**日期**: {date}\n**异常**: " + "、".join(issues)
            }

        return {'valid': True, 'message': ''}

    async def fetch_tiktok_campaigns(
        self,
        start_date: str = None,
        end_date: str = None,
        page_size: int = 100
    ) -> Optional[List[Dict[str, Any]]]:
        """
        获取 TikTok campaign 维度明细数据 (带分页)

        Args:
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            page_size: 每页数量

        Returns:
            campaign 列表，每条包含 cost 和 revenue
        """
        import aiohttp

        # 检查 Token
        if not self.bearer_token or self._should_refresh_token():
            print("[XMP] 需要获取/刷新 Token...")
            await self.login_and_get_token()

        if not self.bearer_token:
            send_lark_alert("XMP 登录失败", "无法获取 campaign 数据", level="error")
            return None

        if not start_date:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date

        print(f"[XMP] 拉取 TikTok campaign 明细: {start_date} ~ {end_date}")

        all_campaigns = []
        page = 1

        headers = {
            "Authorization": self.bearer_token,
            "Content-Type": "application/json",
            "Origin": "https://xmp.mobvista.com",
            "Referer": "https://xmp.mobvista.com/"
        }

        while True:
            payload = {
                "level": "campaign",
                "channel": "tiktok",
                "start_time": start_date,
                "end_time": end_date,
                "field": "campaign_id,campaign_name,cost,total_complete_payment_rate,impression,click",
                "page": page,
                "page_size": page_size
            }

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        "https://xmp-api.mobvista.com/admanage/channel/list",
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        result = await response.json()

                        if result.get('code') != 0:
                            print(f"[XMP] API 错误: {result.get('msg')}")
                            break

                        data = result.get('data', {})
                        campaigns = data.get('list', [])

                        if not campaigns:
                            break

                        # 提取关键字段
                        for c in campaigns:
                            cost = float(c.get('cost', 0))
                            revenue = float(c.get('total_complete_payment_rate', 0))
                            all_campaigns.append({
                                'campaign_id': c.get('campaign_id'),
                                'campaign_name': c.get('campaign_name'),
                                'cost': cost,
                                'revenue': revenue,
                                'roas': revenue / cost if cost > 0 else 0,
                                'impression': int(c.get('impression', 0)),
                                'click': int(c.get('click', 0)),
                            })

                        print(f"  第 {page} 页: {len(campaigns)} 条")

                        if len(campaigns) < page_size:
                            break

                        page += 1
                        await asyncio.sleep(0.5)

            except Exception as e:
                print(f"[XMP] 请求失败: {e}")
                break

        print(f"[XMP] 共获取 {len(all_campaigns)} 个 campaign")
        return all_campaigns


async def main():
    """主函数"""
    scraper = XMPTikTokScraper()

    # 获取日期参数
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')

    # 如果没有 Token，才需要登录
    if not scraper.bearer_token:
        print("[XMP] 没有保存的 Token，需要登录...")
        await scraper.login_and_get_token(headless=False)

    # 获取数据
    result = await scraper.fetch_tiktok_summary(date_str, date_str)

    if result:
        print("\n" + "="*50)
        print(f"TikTok 渠道汇总 ({date_str})")
        print("="*50)
        print(f"总消耗:           ${result['cost']:,.2f}")
        print(f"支付完成总价值:   ${result['total_complete_payment_rate']:,.2f}")
        print(f"总付费价值(App):  ${result['total_purchase_value']:,.2f}")
        print(f"ROAS (网页):      {result['total_complete_payment_rate']/result['cost']*100:.1f}%" if result['cost'] > 0 else "ROAS: N/A")
        print("="*50)


if __name__ == '__main__':
    asyncio.run(main())
