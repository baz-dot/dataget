"""
XMP 多渠道数据定时抓取脚本
支持 TikTok 和 Meta (Facebook) 渠道

功能:
- 定时抓取 campaign 维度数据
- 支持多渠道 (tiktok, facebook)
- 数据存储到 BigQuery
- 失败告警通知
"""

import os
import sys
import json
import re
import asyncio
import aiohttp
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

# 北京时区 (UTC+8)
BEIJING_TZ = timezone(timedelta(hours=8))


def get_last_week_workdays(end_date: datetime) -> List[str]:
    """
    获取上周或本周的完整工作周（周一到周五）

    规则:
    - 周一到周五运行: 统计上周的周一到周五
    - 周六到周日运行: 统计本周的周一到周五

    Args:
        end_date: 参考日期（通常是当前日期）

    Returns:
        工作日列表，格式 ['YYYY-MM-DD', ...]，按时间升序
    """
    current_weekday = end_date.weekday()  # 0=Monday, 6=Sunday

    # 判断是周末还是工作日
    if current_weekday >= 5:  # 周六(5)或周日(6)
        # 周末: 统计本周的周一到周五
        days_to_this_monday = current_weekday
        target_monday = end_date - timedelta(days=days_to_this_monday)
    else:
        # 工作日: 统计上周的周一到周五
        days_to_last_monday = current_weekday + 7
        target_monday = end_date - timedelta(days=days_to_last_monday)

    # 生成周一到周五的日期列表
    workdays = []
    for i in range(5):  # 周一到周五，共5天
        day = target_monday + timedelta(days=i)
        workdays.append(day.strftime('%Y-%m-%d'))

    return workdays


# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

# ============ 配置 ============
LARK_ALERT_WEBHOOK = os.getenv('LARK_ALERT_WEBHOOK') or os.getenv('LARK_WEBHOOK_URL')
XMP_USERNAME = os.getenv('XMP_USERNAME')
XMP_PASSWORD = os.getenv('XMP_PASSWORD')
XMP_COOKIES_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'xmp_cookies.json')
XMP_TOKEN_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'xmp_token.json')

TOKEN_VALID_DAYS = 15
TOKEN_REFRESH_BEFORE_DAYS = 3

# ============ 投手/剪辑师名单配置 ============
# 投手名单 (英文名用于匹配 campaign_name 中的 optimizer-xxx)
# 注: lyla, juria, jade 是韩国人不统计; eason 是剪辑师不是投手
OPTIMIZER_LIST = [
    "kimi", "silas", "zane", "kino", "hannibal", "bigzo", "echo", "felix", "jocelyn", "ponyo", "lulu", "alvin", "troy", "kendon"
]

# 剪辑师名单 (中文名 -> 可能的别名/英文名/姓氏)
EDITOR_NAME_MAP = {
    "谢奕俊": ["eason", "谢"],
    "樊凯翱": ["kyrie", "樊"],
    "吴泽鑫": ["beita", "吴"],
    "宋涵妍": ["helen", "宋"],
    "聂佳欢": ["maggie", "聂"],
    "许丹晨": ["dancey", "许"],
    "李文政": ["curry", "李"],
    "邓玮": ["dorris", "邓"],
    "王俊喜": ["ethan", "王"],
    "陶佳凝": ["lynn", "陶"],
}

# 反向映射: 所有可能的名字 -> 标准中文名
EDITOR_ALIAS_MAP = {}
for cn_name, aliases in EDITOR_NAME_MAP.items():
    EDITOR_ALIAS_MAP[cn_name] = cn_name
    for alias in aliases:
        EDITOR_ALIAS_MAP[alias] = cn_name
        EDITOR_ALIAS_MAP[alias.lower()] = cn_name


def extract_editor_from_material_name(material_name: str) -> Optional[str]:
    """
    从素材名称中提取剪辑师名

    素材名称格式: 日期-剪辑师名-剧名-语言-序号.mp4
    例如:
    - 1.4-聂佳欢-Eldest Daughter's Marriage Life-ko-12.mp4
    - 12.25-樊-Eldest Daughter's Marriage Life-ko-4.mp4
    - 12月1日-宋涵妍-xxx-5.mp4
    """
    if not material_name:
        return None

    # 尝试用 - 分割，第二部分是剪辑师名
    parts = material_name.split('-')
    if len(parts) >= 2:
        editor_part = parts[1].strip()
        # 匹配完整中文名
        if editor_part in EDITOR_ALIAS_MAP:
            return EDITOR_ALIAS_MAP[editor_part]
        # 匹配单姓
        for cn_name, aliases in EDITOR_NAME_MAP.items():
            for alias in aliases:
                if alias == editor_part or alias.lower() == editor_part.lower():
                    return cn_name

    return None


def safe_float(val) -> float:
    """安全地将值转换为 float，处理 None、空字符串、'-' 等情况"""
    if val is None or val == '' or val == '-':
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def safe_int(val) -> int:
    """安全地将值转换为 int"""
    if val is None or val == '' or val == '-':
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def send_lark_alert(title: str, content: str, level: str = "warning"):
    """发送飞书告警通知"""
    if not LARK_ALERT_WEBHOOK:
        print(f"[告警] 未配置飞书 Webhook: {title}")
        return False
    level_config = {
        "info": {"color": "blue", "icon": "ℹ️"},
        "warning": {"color": "orange", "icon": "⚠️"},
        "error": {"color": "red", "icon": "🚨"},
    }
    config = level_config.get(level, level_config["warning"])
    msg = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"{config['icon']} {title}"}, "template": config["color"]},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": content}},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}}
            ]
        }
    }
    try:
        resp = requests.post(LARK_ALERT_WEBHOOK, json=msg, timeout=10)
        return resp.status_code == 200
    except:
        return False


class XMPBaseScraper:
    """XMP 基础抓取器 (Token 管理)"""

    def __init__(self):
        self.bearer_token = None
        self.tri = None
        self.token_updated_at = None
        self._load_token()

    def _load_token(self) -> bool:
        if not os.path.exists(XMP_TOKEN_FILE):
            return False
        try:
            with open(XMP_TOKEN_FILE, 'r') as f:
                data = json.load(f)
            self.bearer_token = data.get('token')
            self.tri = data.get('tri')
            updated_str = data.get('updated')
            if updated_str:
                self.token_updated_at = datetime.fromisoformat(updated_str)
            if self.bearer_token and not self._should_refresh_token():
                print(f"[XMP] 已加载保存的 Token")
                return True
        except Exception as e:
            print(f"[XMP] 加载 Token 失败: {e}")
        return False

    def _should_refresh_token(self) -> bool:
        if not self.tri:
            print(f"[XMP] Token 缺少 tri，需要刷新")
            return True
        if not self.token_updated_at:
            return True
        days_since_update = (datetime.now() - self.token_updated_at).days
        refresh_threshold = TOKEN_VALID_DAYS - TOKEN_REFRESH_BEFORE_DAYS
        if days_since_update >= refresh_threshold:
            print(f"[XMP] Token 已使用 {days_since_update} 天，需要刷新")
            return True
        print(f"[XMP] Token 已使用 {days_since_update} 天，有效期内")
        return False

    def _save_token(self, token: str, tri: str = None):
        tri = tri if tri is not None else self.tri
        try:
            with open(XMP_TOKEN_FILE, 'w') as f:
                json.dump({'token': token, 'tri': tri, 'updated': datetime.now().isoformat()}, f)
            self.token_updated_at = datetime.now()
            print(f"[XMP] Token 已保存")
        except Exception as e:
            print(f"[XMP] 保存 Token 失败: {e}")

    def _api_headers(self) -> Dict[str, str]:
        headers = {
            "Authorization": self.bearer_token,
            "Content-Type": "application/json",
            "Origin": "https://xmp.mobvista.com",
            "Referer": "https://xmp.mobvista.com/"
        }
        if self.tri:
            headers["tri"] = self.tri
        return headers

    async def _save_login_diagnostic_screenshot(self, page, screenshot_path: str):
        try:
            await page.screenshot(path=screenshot_path, full_page=True)
            print(f"[XMP] 登录诊断页面截图已保存: {screenshot_path}")

            from gcs_storage import GCSUploader
            blob_path = f"xmp/login_diagnostics/{screenshot_path}"
            uri = GCSUploader("xmp_raw_data_storage").upload_file(screenshot_path, blob_path)
            print(f"[XMP] 登录诊断页面截图已上传: {uri}")
        except Exception as screenshot_error:
            print(f"[XMP] 保存或上传登录诊断截图失败: {screenshot_error}")

    async def login_and_get_token(self, headless: bool = True) -> Optional[str]:
        """登录 XMP 获取 Token"""
        from playwright.async_api import async_playwright
        print("[XMP] 启动浏览器登录...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            try:
                context = await browser.new_context(viewport={'width': 1920, 'height': 1080})
                if os.path.exists(XMP_COOKIES_FILE):
                    try:
                        with open(XMP_COOKIES_FILE, 'r') as f:
                            await context.add_cookies(json.load(f))
                    except Exception as e:
                        print(f"[XMP] 加载 cookies 失败，将继续走登录流程: {e}")
                page = await context.new_page()
                captured_token = None
                captured_tri = None

                async def capture_request(request):
                    nonlocal captured_token, captured_tri
                    auth = request.headers.get('authorization', '')
                    tri = request.headers.get('tri', '')
                    if auth.startswith('Bearer ') and not captured_token:
                        captured_token = auth
                        print(f"[XMP] 捕获到 Token")
                    if tri and not captured_tri:
                        captured_tri = tri
                        print(f"[XMP] 捕获到 tri")

                page.on('request', capture_request)
                try:
                    await page.goto("https://xmp.mobvista.com/ads_manage/tiktok/account", wait_until='domcontentloaded', timeout=60000)
                except Exception as e:
                    print(f"[XMP] 打开 XMP 账号页面失败，尝试继续检查当前页面状态: {e}")
                    await self._save_login_diagnostic_screenshot(page, "xmp_open_error.png")
                await asyncio.sleep(3)

                if 'login' in page.url.lower():
                    print("[XMP] 需要登录...")
                    max_login_retries = 3
                    for attempt in range(1, max_login_retries + 1):
                        try:
                            if attempt > 1:
                                await asyncio.sleep(1)
                                await page.goto("https://xmp.mobvista.com/ads_manage/tiktok/account", wait_until='domcontentloaded', timeout=60000)
                            try:
                                await page.wait_for_selector('input[type="password"]', timeout=10000)
                            except Exception as e:
                                print(f"[XMP] 等待登录表单超时或失败，继续尝试填写: {e}")
                            print(f"[XMP] XMP_USERNAME:{XMP_USERNAME}")
                            await page.locator('input[type="text"]').first.fill(XMP_USERNAME)
                            await page.locator('input[type="password"]').first.fill(XMP_PASSWORD)
                            try:
                                # 触发登录按钮
                                await page.locator('button.el-button--primary, button.ant-btn-primary').first.click(timeout=5000)
                            except Exception as e:
                                print(f"[XMP] 点击登录按钮失败，尝试回车提交: {e}")
                                await page.locator('input[type="password"]').first.press('Enter')
                            await asyncio.sleep(10)

                            for i in range(3):
                                if 'login' not in page.url.lower():
                                    break

                                print(f"[XMP] 登陆中，再等一会 ({i + 1}/3)")
                                await asyncio.sleep(10)
                            else:
                                if 'login' in page.url.lower():
                                    print("[XMP] 登录等待超过 3 次，放弃")
                                    # return None / break / raise，看你外层流程怎么处理
                            if attempt == max_login_retries:
                                await self._save_login_diagnostic_screenshot(page, "xmp_login_error.png")
                            await page.goto("https://xmp.mobvista.com/ads_manage/tiktok/account", wait_until='domcontentloaded', timeout=60000)
                            if 'login' not in page.url.lower():
                                break
                            raise RuntimeError(f"登录后仍停留在登录页: {page.url}")
                        except Exception as e:
                            print(f"[XMP] 登录 XMP 失败, 重试 ({attempt}/{max_login_retries}): {e}")
                            if attempt == max_login_retries:
                                return None

                await asyncio.sleep(8)
                if not captured_token:
                    try:
                        await page.reload(wait_until='domcontentloaded')
                        await asyncio.sleep(5)
                    except Exception as e:
                        print(f"[XMP] 刷新页面以捕获 Token 失败: {e}")

                try:
                    cookies = await context.cookies()
                    with open(XMP_COOKIES_FILE, 'w') as f:
                        json.dump(cookies, f)
                except Exception as e:
                    print(f"[XMP] 保存 cookies 失败: {e}")

                if captured_token:
                    captured_tri = captured_tri or self.tri
                    self._save_token(captured_token, captured_tri)
                    self.bearer_token = captured_token
                    self.tri = captured_tri

                return captured_token
            finally:
                await browser.close()

# 支持的渠道及其收入字段
CHANNEL_CONFIG = {
    'tiktok': {
        'revenue_fields': ['total_complete_payment_rate'],  # TK收入 = 支付完成总价值
        'name': 'TikTok'
    },
    'facebook': {
        'revenue_fields': ['purchase_value'],  # Meta收入 = 购物转化价值
        'name': 'Meta/Facebook'
    }
}

SUPPORTED_CHANNELS = list(CHANNEL_CONFIG.keys())

# API 端点
XMP_LIST_URL = "https://xmp-api.mobvista.com/admanage/channel/list"
XMP_SUMMARY_URL = "https://xmp-api.mobvista.com/admanage/channel/summary"

# 全局 API 并发限制，防止触发 XMP 限流
XMP_API_SEM = asyncio.Semaphore(3)
XMP_MATERIAL_LIST_URL = "https://xmp-api.mobvista.com/mediacenter/material/list"

# XMP Open API 端点 (素材库/素材报表)
XMP_OPEN_MATERIAL_LIST_URL = "https://xmp-open.mobvista.com/v2/media/material/list"
XMP_OPEN_MATERIAL_REPORT_URL = "https://xmp-open.mobvista.com/v2/media/material_report/list"


class XMPMultiChannelScraper(XMPBaseScraper):
    """XMP 多渠道数据抓取器"""

    async def fetch_channel_campaigns(
        self,
        channel: str,
        start_date: str = None,
        end_date: str = None,
        page_size: int = 1000
    ) -> Optional[List[Dict[str, Any]]]:
        """
        获取指定渠道的 campaign 明细数据

        Args:
            channel: 渠道名 (tiktok/facebook)
            start_date: 开始日期
            end_date: 结束日期
            page_size: 每页数量

        Returns:
            campaign 列表
        """
        if channel not in SUPPORTED_CHANNELS:
            print(f"[XMP] 不支持的渠道: {channel}")
            return None

        # 检查 Token
        if not self.bearer_token or self._should_refresh_token():
            print("[XMP] 需要获取/刷新 Token..")
            await self.login_and_get_token()

        if not self.bearer_token:
            send_lark_alert("XMP 登录失败", f"无法获取 {channel} 数据", level="error")
            return None

        if not start_date:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date

        print(f"[XMP] 拉取 {channel} campaign 明细: {start_date} ~ {end_date}")

        # 获取渠道配置
        channel_cfg = CHANNEL_CONFIG.get(channel, {})
        revenue_fields = channel_cfg.get('revenue_fields', ['purchase_value'])

        all_campaigns = []
        page = 1
        page_retries = 0
        MAX_PAGE_RETRIES = 2

        headers = self._api_headers()

        # 构建字段列表
        base_fields = "campaign_id,campaign_name,cost,impression,click,status,geo"
        revenue_fields_str = ",".join(revenue_fields)
        field_str = f"{base_fields},{revenue_fields_str}"

        while True:
            # TikTok 按 geo 分组时收入字段返回 0，所以只按 campaign_id 分组
            group_by = "campaign_id" if channel == "tiktok" else "geo,campaign_id"
            payload = {
                "level": "report",
                "channel": channel,
                "start_time": start_date,
                "end_time": end_date,
                "field": field_str,
                "group_by": group_by,
                "page": page,
                "page_size": page_size
            }

            try:
                async with XMP_API_SEM:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            XMP_LIST_URL,
                            json=payload,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=90)
                        ) as response:
                            result = await response.json()

                if result.get('code') != 0:
                    msg = result.get('msg', '')
                    if '繁忙' in msg or 'busy' in msg.lower():
                        print(f"[XMP] {channel} campaigns 限流，10s 后重试 (第{page}页)...")
                        await asyncio.sleep(10)
                        continue  # 重试当前页
                    print(f"[XMP] API 错误: {msg}")
                    break

                page_retries = 0  # 成功则重置重试计数
                data = result.get('data', {})
                campaigns = data.get('list', [])

                if not campaigns:
                    break

                for c in campaigns:
                    cost = float(c.get('cost', 0))
                    # 收入 = 所有收入字段之和
                    revenue = sum(float(c.get(f, 0)) for f in revenue_fields)
                    # TikTok 收入明细
                    tk_complete = float(c.get('total_complete_payment_rate', 0)) if channel == 'tiktok' else 0
                    tk_purchase = float(c.get('total_purchase_value', 0)) if channel == 'tiktok' else 0
                    all_campaigns.append({
                        'channel': channel,
                        'campaign_id': c.get('campaign_id'),
                        'campaign_name': c.get('campaign_name'),
                        'cost': cost,
                        'revenue': revenue,
                        'tk_complete_payment': tk_complete,
                        'tk_purchase_value': tk_purchase,
                        'roas': revenue / cost if cost > 0 else 0,
                        'impression': int(c.get('impression', 0)),
                        'click': int(c.get('click', 0)),
                        'status': c.get('status', ''),
                        'geo': c.get('geo', ''),
                        'date': start_date,
                    })

                print(f" {channel} 第 {page} 页: {len(campaigns)} 条")

                if len(campaigns) < page_size:
                    break

                page += 1
                await asyncio.sleep(0.5)

            except (asyncio.TimeoutError, aiohttp.ClientError, Exception) as e:
                page_retries += 1
                if page_retries <= MAX_PAGE_RETRIES:
                    print(f"[XMP] {channel} campaigns 第{page}页失败: {type(e).__name__}，5s 后重试 ({page_retries}/{MAX_PAGE_RETRIES})...")
                    await asyncio.sleep(5)
                    continue
                print(f"[XMP] {channel} campaigns 第{page}页重试用尽，已获取 {len(all_campaigns)} 条")
                break

        print(f"[XMP] {channel} 共获取 {len(all_campaigns)} 个 campaign")
        return all_campaigns

    async def fetch_channel_ads(
        self,
        channel: str,
        start_date: str = None,
        end_date: str = None,
        page_size: int = 1000
    ) -> Optional[List[Dict[str, Any]]]:
        """
        获取指定渠道的广告维度数据 (用于剪辑师分渠道统计)

        Args:
            channel: 渠道名 (tiktok/facebook)
            start_date: 开始日期
            end_date: 结束日期
            page_size: 每页数量

        Returns:
            广告列表，包含 ad_name, cost, drill_channel 等
        """
        if channel not in SUPPORTED_CHANNELS:
            print(f"[XMP] 不支持的渠道: {channel}")
            return None

        # 检查 Token
        if not self.bearer_token or self._should_refresh_token():
            print("[XMP] 需要获取/刷新 Token....")
            await self.login_and_get_token()

        if not self.bearer_token:
            send_lark_alert("XMP 登录失败", f"无法获取 {channel} 广告数据", level="error")
            return None

        if not start_date:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date

        print(f"[XMP] 拉取 {channel} 广告维度数据: {start_date} ~ {end_date}")

        all_ads = []
        page = 1
        page_retries = 0
        MAX_PAGE_RETRIES = 2

        headers = self._api_headers()

        # 获取渠道配置的收入字段
        channel_cfg = CHANNEL_CONFIG.get(channel, {})
        revenue_fields = channel_cfg.get('revenue_fields', ['purchase_value'])
        revenue_fields_str = ",".join(revenue_fields)

        while True:
            payload = {
                "level": "ad",
                "channel": channel,
                "start_time": start_date,
                "end_time": end_date,
                "field": f"ad_name,ad_id,status,cost,impression,click,cpm,cpc,ctr,conversion,cpi,{revenue_fields_str}",
                "page": page,
                "page_size": page_size,
                "report_timezone": "",
                "search": [{"item": "cost", "val": "0", "op": "GT"}]
            }

            try:
                async with XMP_API_SEM:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            XMP_LIST_URL,
                            json=payload,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=30)
                        ) as response:
                            result = await response.json()

                if result.get('code') != 0:
                    msg = result.get('msg', '')
                    if '繁忙' in msg or 'busy' in msg.lower():
                        print(f"[XMP] {channel} ads 限流，10s 后重试 (第{page}页)...")
                        await asyncio.sleep(10)
                        continue
                    print(f"[XMP] API 错误: {msg}")
                    break

                page_retries = 0
                data = result.get('data', {})
                ads = data.get('list', [])

                if not ads:
                    break

                for ad in ads:
                    cost = float(ad.get('cost') or 0)
                    # 计算收入: 累加所有收入字段
                    revenue = 0
                    for rf in revenue_fields:
                        revenue += float(ad.get(rf) or 0)

                    all_ads.append({
                        'channel': channel,
                        'ad_id': ad.get('ad_id'),
                        'ad_name': ad.get('ad_name'),
                        'cost': cost,
                        'revenue': revenue,
                        'impression': int(ad.get('impression') or 0),
                        'click': int(ad.get('click') or 0),
                        'conversion': float(ad.get('conversion') or 0),
                        'cpm': float(ad.get('cpm') or 0),
                        'cpc': float(ad.get('cpc') or 0),
                        'ctr': float(ad.get('ctr') or 0),
                        'status': ad.get('status', ''),
                        'date': start_date,
                    })

                print(f"  第 {page} 页: {len(ads)} 条广告")

                if len(ads) < page_size:
                    break

                page += 1
                await asyncio.sleep(0.5)

            except (asyncio.TimeoutError, aiohttp.ClientError, Exception) as e:
                page_retries += 1
                if page_retries <= MAX_PAGE_RETRIES:
                    print(f"[XMP] {channel} ads 第{page}页失败: {type(e).__name__}，5s 后重试 ({page_retries}/{MAX_PAGE_RETRIES})...")
                    await asyncio.sleep(5)
                    continue
                print(f"[XMP] {channel} ads 第{page}页重试用尽，已获取 {len(all_ads)} 条")
                break

        print(f"[XMP] {channel} 共获取 {len(all_ads)} 条广告")
        return all_ads

    async def fetch_channel_designers(
        self,
        channel: str,
        start_date: str = None,
        end_date: str = None,
        page_size: int = 1000,
        is_xmp: str = "0"
    ) -> Optional[List[Dict[str, Any]]]:
        """
        获取指定渠道的剪辑师(designer)维度数据

        Args:
            channel: 渠道名 (facebook)
            start_date: 开始日期
            end_date: 结束日期
            page_size: 每页数量
            is_xmp: 是否为 XMP 剪辑师 ("0" 或 "1")

        Returns:
            剪辑师列表，包含 designer_name, cost, revenue 等
        """
        # 检查 Token
        if not self.bearer_token or self._should_refresh_token():
            print("[XMP] 需要获取/刷新 Token...")
            await self.login_and_get_token()

        if not self.bearer_token:
            send_lark_alert("XMP 登录失败", f"无法获取 {channel} designer 数据", level="error")
            return None

        if not start_date:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date

        print(f"[XMP] 拉取 {channel} designer 维度数据 (is_xmp={is_xmp}): {start_date} ~ {end_date}")

        all_designers = []
        page = 1
        page_retries = 0
        MAX_PAGE_RETRIES = 2

        headers = self._api_headers()

        # 获取渠道配置的收入字段
        channel_cfg = CHANNEL_CONFIG.get(channel, {})
        revenue_fields = channel_cfg.get('revenue_fields', ['purchase_value'])
        revenue_fields_str = ",".join(revenue_fields)

        while True:
            payload = {
                "level": "designer",
                "channel": channel,
                "start_time": start_date,
                "end_time": end_date,
                "field": f"designer_name,currency_cost,impression,cpm,click,cpc,ctr,conversion_rate,{revenue_fields_str}",
                "page": page,
                "page_size": page_size,
                "report_timezone": "",
                "source": "report",
                "score_by": "avg",
                "search": [{"item": "is_xmp", "val": is_xmp, "op": "EQ"}]
            }

            try:
                async with XMP_API_SEM:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            XMP_LIST_URL,
                            json=payload,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=30)
                        ) as response:
                            result = await response.json()

                if result.get('code') != 0:
                    msg = result.get('msg', '')
                    if '繁忙' in msg or 'busy' in msg.lower():
                        print(f"[XMP] {channel} designers 限流，10s 后重试 (第{page}页)...")
                        await asyncio.sleep(10)
                        continue
                    print(f"[XMP] API 错误: {msg}")
                    break

                page_retries = 0
                data = result.get('data', {})
                designers = data.get('list', [])

                if not designers:
                    break

                for d in designers:
                    # 使用 safe_float 处理可能的 '-' 值
                    cost = safe_float(d.get('currency_cost')) or safe_float(d.get('cost'))
                    # 计算收入
                    revenue = 0
                    for rf in revenue_fields:
                        revenue += safe_float(d.get(rf))

                    all_designers.append({
                        'channel': channel,
                        'designer_name': d.get('designer_name'),
                        'cost': cost,
                        'revenue': revenue,
                        'impression': safe_int(d.get('impression')),
                        'click': safe_int(d.get('click')),
                        'cpm': safe_float(d.get('cpm')),
                        'cpc': safe_float(d.get('cpc')),
                        'ctr': safe_float(d.get('ctr')),
                        'date': start_date,
                    })

                print(f"  第 {page} 页: {len(designers)} 条")

                if len(designers) < page_size:
                    break

                page += 1
                await asyncio.sleep(0.5)

            except (asyncio.TimeoutError, aiohttp.ClientError, Exception) as e:
                page_retries += 1
                if page_retries <= MAX_PAGE_RETRIES:
                    print(f"[XMP] {channel} designers 第{page}页失败: {type(e).__name__}，5s 后重试 ({page_retries}/{MAX_PAGE_RETRIES})...")
                    await asyncio.sleep(5)
                    continue
                print(f"[XMP] {channel} designers 第{page}页重试用尽，已获取 {len(all_designers)} 条")
                break

        print(f"[XMP] {channel} 共获取 {len(all_designers)} 条 designer 数据")
        return all_designers

    async def fetch_channel_summary(
        self,
        channel: str,
        start_date: str = None,
        end_date: str = None
    ) -> Optional[Dict[str, Any]]:
        """获取指定渠道的汇总数据"""
        if not self.bearer_token or self._should_refresh_token():
            await self.login_and_get_token()

        if not self.bearer_token:
            return None

        if not start_date:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date

        # 获取渠道配置
        channel_cfg = CHANNEL_CONFIG.get(channel, {})
        revenue_fields = channel_cfg.get('revenue_fields', ['purchase_value'])
        revenue_fields_str = ",".join(revenue_fields)

        payload = {
            "level": "account",
            "channel": channel,
            "start_time": start_date,
            "end_time": end_date,
            "field": f"cost,{revenue_fields_str},impression,click",
            "page": 1,
            "page_size": 100
        }

        headers = self._api_headers()

        try:
            async with XMP_API_SEM:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        XMP_SUMMARY_URL,
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        result = await response.json()

            if result.get('code') == 0:
                sum_data = result.get('data', {}).get('sum', {})
                cost = float(sum_data.get('cost', 0))
                # 收入 = 所有收入字段之和
                revenue = sum(float(sum_data.get(f, 0)) for f in revenue_fields)

                return {
                    'channel': channel,
                    'date': start_date,
                    'cost': cost,
                    'revenue': revenue,
                    'roas': revenue / cost if cost > 0 else 0,
                    'impression': int(sum_data.get('impression', 0)),
                    'click': int(sum_data.get('click', 0)),
                }
        except Exception as e:
            print(f"[XMP] {channel} 汇总请求失败: {e}")

        return None

    async def fetch_all_channels(
        self,
        start_date: str = None,
        end_date: str = None
    ) -> Dict[str, Any]:
        """
        抓取所有渠道数据

        Returns:
            {
                'summary': {channel: summary_data},
                'campaigns': [all campaigns],
                'timestamp': datetime
            }
        """
        if not start_date:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date

        print(f"\n{'='*50}")
        print(f"[XMP] 开始抓取所有渠道数据: {start_date}")
        print(f"{'='*50}")

        result = {
            'summary': {},
            'campaigns': [],
            'timestamp': datetime.now().isoformat(),
            'date': start_date
        }

        # 确保 Token 已获取（避免并行时重复登录）
        if not self.bearer_token or self._should_refresh_token():
            print("[XMP] 需要获取/刷新 Token.....")
            await self.login_and_get_token()

        # 并行拉取所有渠道
        async def _fetch_single_channel(channel):
            print(f"\n--- {channel.upper()} ---")
            summary = await self.fetch_channel_summary(channel, start_date, end_date)
            if summary:
                print(f"  汇总: cost=${summary['cost']:,.2f}, revenue=${summary['revenue']:,.2f}, ROAS={summary['roas']*100:.1f}%")
            campaigns = await self.fetch_channel_campaigns(channel, start_date, end_date)
            return channel, summary, campaigns

        channel_results = await asyncio.gather(
            *[_fetch_single_channel(ch) for ch in SUPPORTED_CHANNELS]
        )

        for channel, summary, campaigns in channel_results:
            if summary:
                result['summary'][channel] = summary
            if campaigns:
                result['campaigns'].extend(campaigns)

        # 打印总汇总
        total_cost = sum(s['cost'] for s in result['summary'].values())
        total_rev = sum(s['revenue'] for s in result['summary'].values())

        print(f"\n{'='*50}")
        print(f"[XMP] 抓取完成")
        print(f"  总 campaign 数: {len(result['campaigns'])}")
        print(f"  总消耗: ${total_cost:,.2f}")
        print(f"  总收入: ${total_rev:,.2f}")
        print(f"  整体 ROAS: {total_rev/total_cost*100:.1f}%" if total_cost > 0 else "  ROAS: N/A")
        print(f"{'='*50}\n")

        return result


class XMPEditorStatsScraper(XMPBaseScraper):
    """XMP 剪辑师统计数据抓取器 (使用 Open API)"""

    def __init__(self):
        super().__init__()
        # Open API 配置
        self.client_id = os.getenv('XMP_CLIENT_ID')
        self.client_secret = os.getenv('XMP_CLIENT_SECRET')

    def _generate_sign(self, timestamp: int) -> str:
        """生成签名: sign = md5(client_secret + timestamp)"""
        import hashlib
        sign_str = f"{self.client_secret}{timestamp}"
        return hashlib.md5(sign_str.encode()).hexdigest()

    def _make_request(self, url: str, payload: dict) -> Optional[dict]:
        """发送 API 请求"""
        import time
        headers = {"Content-Type": "application/json"}
        max_retries = 3
        retry_delays = [10, 30, 60]

        for attempt in range(max_retries):
            try:
                response = requests.post(url, json=payload, headers=headers, timeout=60)
                result = response.json()

                if result.get("code") == 0:
                    return result.get("data")

                error_msg = result.get("msg", "Unknown error")
                if "频繁" in error_msg or "too many" in error_msg.lower():
                    if attempt < max_retries - 1:
                        delay = retry_delays[attempt]
                        print(f"[限频] {delay}秒后重试...")
                        time.sleep(delay)
                        continue

                print(f"[API错误] code={result.get('code')}, msg={error_msg}")
                return None

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delays[attempt])
                    continue
                print(f"[错误] {e}")
                return None
        return None

    def fetch_material_details(
        self,
        start_date: str = None,
        end_date: str = None,
        folder_id: List[int] = None,
        user_material_id: List[str] = None,
        md5_file_id: List[str] = None,
        is_deleted: int = 0,
        page_size: int = 500
    ) -> List[dict]:
        """
        获取素材详情列表 (v2 API)

        Args:
            start_date: 创建开始日期 YYYY-MM-DD
            end_date: 创建结束日期 YYYY-MM-DD (最长跨度30天)
            folder_id: 文件夹ID列表
            user_material_id: 素材local id列表
            md5_file_id: 素材md5列表
            is_deleted: 是否删除 (0未删除, 1已删除, None全部)
            page_size: 每页数量 (1-1000)

        Returns:
            素材详情列表，包含:
            - material_name: 素材名称
            - designer: 设计师/剪辑师
            - shape: 形状 (竖版/横版/方形)
            - duration: 时长(秒)
            - file_url: 文件地址
            - folder_name: 文件夹名称
            - created_time: 创建时间
        """
        import time
        if not self.client_id or not self.client_secret:
            print("[XMP] 未配置 XMP_CLIENT_ID/XMP_CLIENT_SECRET")
            return []

        # 至少需要一个查询条件
        if not any([start_date, folder_id, user_material_id, md5_file_id]):
            print("[XMP] 错误: 至少需要传入一个查询条件 (start_date/folder_id/user_material_id/md5_file_id)")
            return []

        all_materials = []
        page = 1

        date_range = f"{start_date} ~ {end_date}" if start_date else "无日期限制"
        print(f"[XMP] 获取素材详情: {date_range}")

        while True:
            timestamp = int(time.time())
            payload = {
                "client_id": self.client_id,
                "timestamp": timestamp,
                "sign": self._generate_sign(timestamp),
                "page": page,
                "page_size": page_size
            }

            # 添加可选参数
            if start_date:
                payload["start_date"] = start_date
            if end_date:
                payload["end_date"] = end_date
            if folder_id:
                payload["folder_id"] = folder_id
            if user_material_id:
                payload["user_material_id"] = user_material_id
            if md5_file_id:
                payload["md5_file_id"] = md5_file_id
            if is_deleted is not None:
                payload["is_deleted"] = is_deleted

            data = self._make_request(XMP_OPEN_MATERIAL_LIST_URL, payload)
            if not data:
                break

            # API 返回列表或 {list: [...]} 结构
            if isinstance(data, list):
                materials = data
            else:
                materials = data if isinstance(data, list) else []

            if not materials:
                break

            all_materials.extend(materials)
            print(f"[XMP] 第 {page} 页: {len(materials)} 个素材，累计 {len(all_materials)} 个")

            if len(materials) < page_size:
                break

            page += 1
            time.sleep(0.5)  # QPM 限频保护

        print(f"[XMP] 素材详情共 {len(all_materials)} 条记录")
        return all_materials

    def fetch_material_list(self, md5_file_id: List[str] = None) -> List[dict]:
        """获取素材库列表 (含剪辑师信息)"""
        import time
        if not self.client_id or not self.client_secret:
            print("[XMP] 未配置 XMP_CLIENT_ID/XMP_CLIENT_SECRET")
            return []

        all_materials = []
        page = 1
        page_size = 100

        print(f"[XMP] 获取素材库列表...")

        while True:
            timestamp = int(time.time())
            payload = {
                "client_id": self.client_id,
                "timestamp": timestamp,
                "sign": self._generate_sign(timestamp),
                "page": page,
                "page_size": page_size
            }
            if md5_file_id:
                payload["md5_file_id"] = md5_file_id

            data = self._make_request(XMP_OPEN_MATERIAL_LIST_URL, payload)
            if not data:
                break

            # API 可能直接返回列表或 {list: [...]} 结构
            if isinstance(data, list):
                materials = data
            else:
                materials = data.get("list", [])
            if not materials:
                break

            all_materials.extend(materials)
            print(f"[XMP] 第 {page} 页: {len(materials)} 个素材")

            if len(materials) < page_size:
                break
            page += 1
            time.sleep(0.5)

        print(f"[XMP] 共获取 {len(all_materials)} 个素材")
        return all_materials

    def fetch_material_report(
        self,
        start_date: str,
        end_date: str,
        dimension: List[str] = None,
        metrics: List[str] = None
    ) -> List[dict]:
        """
        获取素材报表数据 (含消耗和收入信息)

        Args:
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            dimension: 维度列表，默认 ["md5_file_id", "material_name"]
            metrics: 指标列表，默认包含花费、收入等核心指标

        Returns:
            素材报表数据列表
        """
        import time
        if not self.client_id or not self.client_secret:
            print("[XMP] 未配置 XMP_CLIENT_ID/XMP_CLIENT_SECRET")
            return []

        all_data = []
        page = 1
        page_size = 500

        # 默认维度和指标
        if dimension is None:
            dimension = ["md5_file_id", "material_name"]
        if metrics is None:
            # 核心指标: 花费、收入、展示、点击
            metrics = [
                "currency_cost",      # 花费 (USD)
                "total_purchase_value",  # 付费金额/收入
                "impression",         # 展示数
                "click",              # 点击数
            ]

        print(f"[XMP] 拉取素材报表: {start_date} ~ {end_date}")
        print(f"[XMP] 维度: {dimension}, 指标: {metrics}")

        while True:
            timestamp = int(time.time())
            payload = {
                "client_id": self.client_id,
                "timestamp": timestamp,
                "sign": self._generate_sign(timestamp),
                "start_date": start_date,
                "end_date": end_date,
                "metrics": metrics,
                "dimension": dimension,
                "page": page,
                "page_size": page_size,
                "cost_currency": "usd"
            }

            data = self._make_request(XMP_OPEN_MATERIAL_REPORT_URL, payload)
            if not data:
                break

            records = data.get("list", [])
            if not records:
                break

            all_data.extend(records)
            print(f"[XMP] 第 {page} 页: {len(records)} 条")

            if len(records) < page_size:
                break
            page += 1
            time.sleep(6)  # QPM 限频保护 (10 QPM)

        print(f"[XMP] 素材报表共 {len(all_data)} 条记录")
        return all_data

    def _extract_designer_name(self, designer_raw) -> str:
        """从 designer 字段提取设计师名称"""
        if isinstance(designer_raw, dict):
            return designer_raw.get("name") or designer_raw.get("designer_name") or "未知"
        elif isinstance(designer_raw, list):
            first = designer_raw[0] if designer_raw else None
            if isinstance(first, dict):
                return first.get("name") or first.get("designer_name") or "未知"
            else:
                return first or "未知"
        else:
            return designer_raw or "未知"

    def _extract_optimizer_from_name(self, name: str) -> str:
        """从 campaign_name 中提取投手英文名"""
        if not name:
            return "未知"
        name_lower = name.lower()
        for optimizer in OPTIMIZER_LIST:
            if optimizer.lower() in name_lower:
                return optimizer.lower()
        return "未知"

    def _extract_editor_from_name(self, name: str) -> str:
        """从广告名称中搜索剪辑师 (中英文名)"""
        if not name:
            return "未知"
        # 先尝试精确匹配中文名
        for cn_name in EDITOR_NAME_MAP.keys():
            if cn_name in name:
                return cn_name
        # 再尝试匹配别名
        name_lower = name.lower()
        for alias, cn_name in EDITOR_ALIAS_MAP.items():
            if alias.lower() in name_lower:
                return cn_name
        return "未知"

    def fetch_editor_stats(
        self,
        start_date: str,
        end_date: str,
        hot_threshold: float = 500.0,
        roas_threshold: float = 0.45
    ) -> List[dict]:
        """
        获取剪辑师统计数据 (周报用)

        统计指标:
        - material_count: 上新素材数
        - total_cost: 消耗贡献
        - total_revenue: 总收入
        - d0_roas: D0 ROAS (收入/花费)
        - hot_count: 爆款数 (>$500 且 ROI 达标)
        - hot_rate: 爆款率
        - top_material: Top 素材名称

        Args:
            start_date: 开始日期
            end_date: 结束日期
            hot_threshold: 爆款消耗阈值 (默认 $500)
            roas_threshold: 爆款 ROAS 阈值 (默认 45%)
        """
        print(f"[XMP] 获取剪辑师统计: {start_date} ~ {end_date}")

        # 1. 获取素材报表 (消耗+收入数据)
        material_report = self.fetch_material_report(start_date, end_date)
        if not material_report:
            print("[XMP] 素材报表为空")
            return []

        # 构建 md5 -> {cost, revenue, name} 映射
        md5_data_map = {}
        for item in material_report:
            md5 = item.get("md5_file_id")
            if not md5:
                continue
            cost = float(item.get("currency_cost", 0))
            revenue = float(item.get("total_purchase_value", 0))
            name = item.get("material_name", "")

            if md5 not in md5_data_map:
                md5_data_map[md5] = {"cost": 0, "revenue": 0, "name": name}

            md5_data_map[md5]["cost"] += cost
            md5_data_map[md5]["revenue"] += revenue
            if not md5_data_map[md5]["name"] and name:
                md5_data_map[md5]["name"] = name

        # 2. 获取素材库 (剪辑师信息)
        md5_list = list(md5_data_map.keys())
        materials = self.fetch_material_list(md5_file_id=md5_list)
        if not materials:
            print("[XMP] 素材库为空")
            return []

        # 构建 md5 -> designer 映射
        md5_designer_map = {}
        for mat in materials:
            md5 = mat.get("md5_file_id")
            designer = self._extract_designer_name(mat.get("designer"))
            md5_designer_map[md5] = designer

        # 3. 按剪辑师聚合
        editor_stats = {}
        for md5, data in md5_data_map.items():
            designer = md5_designer_map.get(md5, "未知")
            cost = data["cost"]
            revenue = data["revenue"]
            name = data["name"]
            roas = revenue / cost if cost > 0 else 0

            if designer not in editor_stats:
                editor_stats[designer] = {
                    "name": designer,
                    "material_count": 0,
                    "total_cost": 0,
                    "total_revenue": 0,
                    "hot_count": 0,
                    "materials": []  # 用于找 Top 素材
                }

            editor_stats[designer]["material_count"] += 1
            editor_stats[designer]["total_cost"] += cost
            editor_stats[designer]["total_revenue"] += revenue

            # 爆款判定: 消耗 > 阈值 且 ROAS 达标
            if cost >= hot_threshold and roas >= roas_threshold:
                editor_stats[designer]["hot_count"] += 1

            # 记录素材用于找 Top
            editor_stats[designer]["materials"].append({
                "name": name,
                "md5": md5,
                "cost": cost,
                "revenue": revenue,
                "roas": roas
            })

        # 4. 计算衍生指标
        result = []
        for editor in editor_stats.values():
            count = editor["material_count"]
            cost = editor["total_cost"]
            revenue = editor["total_revenue"]

            # 找 Top 素材 (按消耗排序)
            materials_sorted = sorted(
                editor["materials"],
                key=lambda x: x["cost"],
                reverse=True
            )
            top_mat = materials_sorted[0] if materials_sorted else None

            result.append({
                "name": editor["name"],
                "material_count": count,
                "total_cost": cost,
                "total_revenue": revenue,
                "d0_roas": revenue / cost if cost > 0 else 0,
                "hot_count": editor["hot_count"],
                "hot_rate": editor["hot_count"] / count if count > 0 else 0,
                "top_material": top_mat["name"] if top_mat else "",
                "top_material_cost": top_mat["cost"] if top_mat else 0,
                "top_material_roas": top_mat["roas"] if top_mat else 0,
            })

        # 按消耗排序
        result.sort(key=lambda x: x["total_cost"], reverse=True)

        print(f"[XMP] 剪辑师统计: {len(result)} 人")
        return result

    def fetch_editor_performance(
        self,
        start_date: str,
        end_date: str,
        hot_threshold: float = 500.0,
        roas_threshold: float = 0.45
    ) -> Dict[str, Any]:
        """
        获取剪辑师产出与质量报表 (Editor Performance)

        对应图表字段:
        - 剪辑师: name
        - 上新素材数: material_count
        - 消耗贡献: total_cost
        - D0 Roas: d0_roas
        - 爆款率: hot_rate (>$500 且 ROI 达标)
        - Top 素材: top_material

        Returns:
            {
                'start_date': 开始日期,
                'end_date': 结束日期,
                'summary': {total_editors, total_materials, total_cost, avg_roas},
                'editors': [剪辑师数据列表]
            }
        """
        print(f"[XMP] 生成剪辑师产出报表: {start_date} ~ {end_date}")

        # 获取剪辑师统计数据
        editors = self.fetch_editor_stats(
            start_date=start_date,
            end_date=end_date,
            hot_threshold=hot_threshold,
            roas_threshold=roas_threshold
        )

        if not editors:
            return {
                'start_date': start_date,
                'end_date': end_date,
                'summary': {},
                'editors': []
            }

        # 计算汇总数据
        total_cost = sum(e['total_cost'] for e in editors)
        total_revenue = sum(e['total_revenue'] for e in editors)
        total_materials = sum(e['material_count'] for e in editors)

        summary = {
            'total_editors': len(editors),
            'total_materials': total_materials,
            'total_cost': total_cost,
            'total_revenue': total_revenue,
            'avg_roas': total_revenue / total_cost if total_cost > 0 else 0
        }

        # 格式化输出
        print(f"\n{'='*60}")
        print(f"剪辑师产出与质量 (Editor Performance)")
        print(f"日期范围: {start_date} ~ {end_date}")
        print(f"{'='*60}")
        print(f"{'剪辑师':<12} {'上新素材数':>10} {'消耗贡献':>12} {'D0 ROAS':>10} {'爆款率':>10} {'Top 素材':<20}")
        print(f"{'-'*60}")

        for e in editors[:10]:  # 只打印前10
            name = e['name'][:10] if len(e['name']) > 10 else e['name']
            top_mat = e['top_material'][:18] if len(e['top_material']) > 18 else e['top_material']
            print(f"{name:<12} {e['material_count']:>10} ${e['total_cost']:>10,.0f} {e['d0_roas']*100:>9.1f}% {e['hot_rate']*100:>9.1f}% {top_mat:<20}")

        print(f"{'='*60}")
        print(f"汇总: {summary['total_editors']} 位剪辑师, {total_materials} 个素材, 总消耗 ${total_cost:,.0f}, 平均 ROAS {summary['avg_roas']*100:.1f}%")

        return {
            'start_date': start_date,
            'end_date': end_date,
            'summary': summary,
            'editors': editors
        }

    def fetch_daily_editor_output(self, date: str) -> Dict[str, Any]:
        """
        获取某天的剪辑师产出统计 (日报用)

        Args:
            date: 日期 YYYY-MM-DD

        Returns:
            {
                'date': 日期,
                'total_materials': 总素材数,
                'editors': [{name, count, materials}]
            }
        """
        print(f"[XMP] 获取剪辑师日产出: {date}")

        # 获取当天创建的素材
        materials = self.fetch_material_details(
            start_date=date,
            end_date=date,
            is_deleted=0
        )

        if not materials:
            return {'date': date, 'total_materials': 0, 'editors': []}

        # 按剪辑师聚合
        editor_output = {}
        for mat in materials:
            designer_raw = mat.get("designer", [])
            if isinstance(designer_raw, list) and designer_raw:
                designer = designer_raw[0].get("name", "未知")
            elif isinstance(designer_raw, dict):
                designer = designer_raw.get("name", "未知")
            else:
                designer = "未知"

            if designer not in editor_output:
                editor_output[designer] = {
                    "name": designer,
                    "count": 0,
                    "materials": []
                }

            editor_output[designer]["count"] += 1
            editor_output[designer]["materials"].append({
                "name": mat.get("material_name"),
                "shape": mat.get("shape"),
                "duration": mat.get("duration"),
                "folder": mat.get("folder_name")
            })

        # 转为列表并排序
        editors = list(editor_output.values())
        editors.sort(key=lambda x: x["count"], reverse=True)

        result = {
            'date': date,
            'total_materials': len(materials),
            'editors': editors
        }

        print(f"[XMP] 日产出统计: {len(materials)} 个素材, {len(editors)} 位剪辑师")
        return result


async def run_once(date_str: str = None, upload_bq: bool = False):
    """执行一次抓取（包含投手/剪辑师统计）"""
    import time as _time
    _t0 = _time.time()

    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    scraper = XMPMultiChannelScraper()
    result = await scraper.fetch_all_channels(date_str, date_str)

    campaigns = result.get('campaigns', [])
    if not campaigns:
        print("[XMP] 无 campaign 数据")
        return result

    # 并行获取: 投手统计 + 剪辑师数据 (Meta designers × 2 + TikTok ads)
    optimizer_task = fetch_optimizer_summary_stats(scraper.bearer_token, date_str, tri=scraper.tri)
    designers_0_task = scraper.fetch_channel_designers('facebook', date_str, date_str, is_xmp="0")
    designers_1_task = scraper.fetch_channel_designers('facebook', date_str, date_str, is_xmp="1")
    tk_ads_task = scraper.fetch_channel_ads('tiktok', date_str, date_str)

    optimizer_stats, designers_0, designers_1, tk_ads = await asyncio.gather(
        optimizer_task, designers_0_task, designers_1_task, tk_ads_task
    )

    # 汇总剪辑师数据
    editor_stats = []

    meta_designers = []
    if designers_0:
        meta_designers.extend(designers_0)
    if designers_1:
        meta_designers.extend(designers_1)

    if meta_designers:
        for d in meta_designers:
            cost = d['cost']
            revenue = d['revenue']
            editor_stats.append({
                'stat_date': date_str,
                'channel': 'facebook',
                'name': d['designer_name'],
                'total_cost': cost,
                'total_revenue': revenue,
                'd0_roas': revenue / cost if cost > 0 else 0,
                'impressions': d['impression'],
                'clicks': d['click'],
                'material_count': 0,
                'hot_count': 0,
                'hot_rate': 0,
                'top_material': '',
                'top_material_cost': 0,
                'top_material_roas': 0,
            })

    if tk_ads:
        tk_editor_stats = aggregate_editor_stats_from_ads(tk_ads, date_str)
        editor_stats.extend(tk_editor_stats)

    # 按消耗排序
    editor_stats.sort(key=lambda x: x['total_cost'], reverse=True)

    # 计算去重后的人数
    unique_optimizers = len(set(o['name'] for o in optimizer_stats))
    print(f"[XMP] 投手统计: {unique_optimizers} 人 ({len(optimizer_stats)} 条记录), 剪辑师统计: {len(editor_stats)} 人")

    result['optimizer_stats'] = optimizer_stats
    result['editor_stats'] = editor_stats

    # 后台上传到 BigQuery（不阻塞返回）
    if upload_bq:
        async def _bg_upload():
            try:
                from bigquery_storage import BigQueryUploader

                project_id = os.getenv('BQ_PROJECT_ID')
                dataset_id = os.getenv('BQ_DATASET_ID', 'xmp_data')

                if project_id:
                    batch_id = datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M%S')
                    uploader = BigQueryUploader(project_id, dataset_id)
                    print(f"[BQ] 后台上传开始: {len(campaigns)} campaigns + {len(optimizer_stats)} 投手 + {len(editor_stats)} 剪辑师")

                    count_c = uploader.upload_xmp_internal_campaigns(campaigns, batch_id=batch_id)
                    print(f"[BQ] 已上传 {count_c} 条 campaign 记录")

                    count_o = uploader.upload_optimizer_stats(optimizer_stats, batch_id=batch_id)
                    print(f"[BQ] 已上传 {count_o} 条投手统计")

                    count_e = uploader.upload_editor_stats(editor_stats, batch_id=batch_id)
                    print(f"[BQ] 已上传 {count_e} 条剪辑师统计")

                    print(f"[BQ] 后台上传完成: batch_id={batch_id}")
                else:
                    print("[BQ] 未配置 BQ_PROJECT_ID，跳过上传")
            except Exception as e:
                print(f"[BQ] 后台上传失败: {e}")
                send_lark_alert("XMP 数据上传失败", str(e), level="error")

        asyncio.create_task(_bg_upload())

    _elapsed = _time.time() - _t0
    print(f"\n[XMP] run_once 总耗时: {_elapsed:.1f}秒 ({_elapsed/60:.1f}分钟)")
    return result


async def fetch_optimizer_summary_stats(
    bearer_token: str,
    date_str: str,
    optimizer_list: List[str] = None,
    tri: str = None
) -> List[Dict]:
    """
    使用 channel/summary API 获取投手汇总数据

    Args:
        bearer_token: Bearer Token
        date_str: 日期 YYYY-MM-DD
        optimizer_list: 投手列表，默认使用 OPTIMIZER_LIST

    Returns:
        投手统计列表
    """
    if optimizer_list is None:
        optimizer_list = OPTIMIZER_LIST

    channels = ['tiktok', 'facebook']
    results = []

    headers = {
        "Authorization": bearer_token,  # bearer_token 已包含 "Bearer " 前缀
        "Content-Type": "application/json",
        "Origin": "https://xmp.mobvista.com",
        "Referer": "https://xmp.mobvista.com/"
    }
    if tri:
        headers["tri"] = tri

    async def _fetch_one_optimizer(optimizer):
        """并行获取单个投手的所有渠道数据"""
        optimizer_data = {
            'stat_date': date_str,
            'name': optimizer,
            'tiktok_cost': 0,
            'tiktok_revenue': 0,
            'tiktok_roas': 0,
            'facebook_cost': 0,
            'facebook_revenue': 0,
            'facebook_roas': 0,
            'total_cost': 0,
            'total_revenue': 0,
            'roas': 0
        }

        for channel in channels:
            payload = {
                "level": "campaign",
                "channel": channel,
                "start_time": date_str,
                "end_time": date_str,
                "field": "cost,purchase_value,total_complete_payment_rate",
                "page": 1,
                "page_size": 1000,
                "report_timezone": "",
                "search": [
                    {"item": "campaign", "val": optimizer, "op": "LIKE", "op_type": "OR"}
                ]
            }

            try:
                async with XMP_API_SEM:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            XMP_SUMMARY_URL,
                            json=payload,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=30)
                        ) as response:
                            result = await response.json()

                if result.get('code') != 0:
                    msg = result.get('msg', '')
                    if '繁忙' in msg or 'busy' in msg.lower():
                        print(f"[XMP] {optimizer}/{channel} 限流，10s 后重试...")
                        await asyncio.sleep(10)
                        # 重试
                        try:
                            async with XMP_API_SEM:
                                async with aiohttp.ClientSession() as session:
                                    async with session.post(
                                        XMP_SUMMARY_URL,
                                        json=payload,
                                        headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=30)
                                    ) as response:
                                        result = await response.json()
                            if result.get('code') != 0:
                                print(f"[XMP] API 重试仍失败 ({optimizer}/{channel}): {result.get('msg')}")
                                continue
                        except Exception as e:
                            print(f"[XMP] 重试失败 ({optimizer}/{channel}): {e}")
                            continue
                    else:
                        print(f"[XMP] API 错误 ({optimizer}/{channel}): {msg}")
                        continue

                sum_data = result.get('data', {}).get('sum', {})
                cost = float(sum_data.get('cost', 0) or 0)
                # TikTok 用 total_complete_payment_rate，Facebook 用 purchase_value
                if channel == 'tiktok':
                    revenue = float(sum_data.get('total_complete_payment_rate', 0) or 0)
                else:
                    revenue = float(sum_data.get('purchase_value', 0) or 0)
                roas = revenue / cost if cost > 0 else 0

                if channel == 'tiktok':
                    optimizer_data['tiktok_cost'] = cost
                    optimizer_data['tiktok_revenue'] = revenue
                    optimizer_data['tiktok_roas'] = roas
                else:
                    optimizer_data['facebook_cost'] = cost
                    optimizer_data['facebook_revenue'] = revenue
                    optimizer_data['facebook_roas'] = roas

            except Exception as e:
                print(f"[XMP] 请求失败 ({optimizer}/{channel}): {e}")

        print(f"[XMP] {optimizer}: TT ${optimizer_data['tiktok_cost']:,.2f}, Meta ${optimizer_data['facebook_cost']:,.2f}")
        return optimizer_data

    # 并行获取所有投手数据
    optimizer_data_list = await asyncio.gather(
        *[_fetch_one_optimizer(opt) for opt in optimizer_list]
    )

    # 汇总结果
    for optimizer_data in optimizer_data_list:
        # 分渠道添加记录 (export_stats_to_lark_doc 需要按 channel 区分)
        # TikTok 记录
        if optimizer_data['tiktok_cost'] > 0 or optimizer_data['tiktok_revenue'] > 0:
            results.append({
                'stat_date': date_str,
                'name': optimizer_data['name'],
                'channel': 'tiktok',
                'total_cost': optimizer_data['tiktok_cost'],
                'total_revenue': optimizer_data['tiktok_revenue'],
                'roas': optimizer_data['tiktok_roas'],
            })

        # Facebook 记录
        if optimizer_data['facebook_cost'] > 0 or optimizer_data['facebook_revenue'] > 0:
            results.append({
                'stat_date': date_str,
                'name': optimizer_data['name'],
                'channel': 'facebook',
                'total_cost': optimizer_data['facebook_cost'],
                'total_revenue': optimizer_data['facebook_revenue'],
                'roas': optimizer_data['facebook_roas'],
            })

    return results


async def fetch_editor_tiktok_stats(
    bearer_token: str,
    date_str: str,
    tri: str = None
) -> List[Dict]:
    """
    使用 channel/summary API 获取剪辑师 TikTok 数据

    Args:
        bearer_token: Bearer Token
        date_str: 日期 YYYY-MM-DD

    Returns:
        剪辑师 TikTok 统计列表
    """
    results = []

    headers = {
        "Authorization": bearer_token,  # bearer_token 已包含 "Bearer " 前缀
        "Content-Type": "application/json",
        "Origin": "https://xmp.mobvista.com",
        "Referer": "https://xmp.mobvista.com/"
    }
    if tri:
        headers["tri"] = tri

    for cn_name, aliases in EDITOR_NAME_MAP.items():
        # 搜索所有可能的名字（英文名 + 中文名 + 姓氏）
        search_names = [cn_name] + aliases  # 中文全名 + 所有别名

        total_cost = 0
        total_revenue = 0

        for search_name in search_names:
            payload = {
                "level": "ad",
                "channel": "tiktok",
                "start_time": date_str,
                "end_time": date_str,
                "field": "cost,total_complete_payment_rate",
                "page": 1,
                "page_size": 1000,
                "report_timezone": "",
                "search": [
                    {"item": "ad", "val": search_name, "op": "LIKE", "op_type": "OR"}
                ]
            }

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        XMP_SUMMARY_URL,
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        result = await response.json()

                        if result.get('code') != 0:
                            continue

                        sum_data = result.get('data', {}).get('sum', {})
                        cost = float(sum_data.get('cost', 0) or 0)
                        revenue = float(sum_data.get('total_complete_payment_rate', 0) or 0)

                        total_cost += cost
                        total_revenue += revenue

            except Exception as e:
                print(f"[XMP] 请求失败 ({cn_name}/{search_name}): {e}")

        # 汇总该剪辑师所有名字的数据
        roas = total_revenue / total_cost if total_cost > 0 else 0
        results.append({
            'editor_name': cn_name,
            'channel': 'tiktok',
            'stat_date': date_str,
            'spend': total_cost,
            'revenue': total_revenue,
            'roas': roas
        })
        print(f"[XMP] {cn_name} (TT): ${total_cost:,.2f}, Rev ${total_revenue:,.2f}")

    return results


async def fetch_editor_facebook_stats(
    bearer_token: str,
    date_str: str,
    tri: str = None
) -> List[Dict]:
    """
    使用 channel/list API 获取剪辑师 Facebook 数据

    Args:
        bearer_token: Bearer Token
        date_str: 日期 YYYY-MM-DD

    Returns:
        剪辑师 Facebook 统计列表
    """
    results = []

    headers = {
        "Authorization": bearer_token,  # bearer_token 已包含 "Bearer " 前缀
        "Content-Type": "application/json",
        "Origin": "https://xmp.mobvista.com",
        "Referer": "https://xmp.mobvista.com/"
    }
    if tri:
        headers["tri"] = tri

    # 使用 designer level 获取所有剪辑师数据
    payload = {
        "level": "designer",
        "channel": "facebook",
        "start_time": date_str,
        "end_time": date_str,
        "field": "cost,purchase_value,designer_name",
        "page": 1,
        "page_size": 1000,
        "report_timezone": "",
        "search": [
            {"item": "is_xmp", "val": "0", "op": "EQ"}
        ],
        "source": "report"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                XMP_LIST_URL,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                result = await response.json()

                if result.get('code') != 0:
                    print(f"[XMP] API 错误: {result.get('msg')}")
                    return results

                data_list = result.get('data', {}).get('list', [])

                # 构建剪辑师中文名到英文名的反向映射
                name_to_cn = {}
                for cn_name, aliases in EDITOR_NAME_MAP.items():
                    name_to_cn[cn_name] = cn_name
                    for alias in aliases:
                        name_to_cn[alias.lower()] = cn_name

                # 安全转换为 float
                def safe_float(val):
                    if val is None or val == '' or val == '-':
                        return 0.0
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return 0.0

                # 按剪辑师聚合数据
                editor_data = {}
                for item in data_list:
                    designer_name = item.get('designer_name', '')
                    # 使用 currency_cost 而不是 cost（cost 返回 '-'）
                    cost = safe_float(item.get('currency_cost', 0))
                    revenue = safe_float(item.get('purchase_value', 0))

                    # 查找对应的中文名
                    cn_name = name_to_cn.get(designer_name.lower())
                    if not cn_name:
                        cn_name = name_to_cn.get(designer_name)

                    if cn_name:
                        if cn_name not in editor_data:
                            editor_data[cn_name] = {'cost': 0, 'revenue': 0}
                        editor_data[cn_name]['cost'] += cost
                        editor_data[cn_name]['revenue'] += revenue

                # 转换为结果列表
                for cn_name in EDITOR_NAME_MAP.keys():
                    data = editor_data.get(cn_name, {'cost': 0, 'revenue': 0})
                    cost = data['cost']
                    revenue = data['revenue']
                    roas = revenue / cost if cost > 0 else 0

                    results.append({
                        'editor_name': cn_name,
                        'channel': 'facebook',
                        'stat_date': date_str,
                        'spend': cost,
                        'revenue': revenue,
                        'roas': roas
                    })
                    print(f"[XMP] {cn_name} (Meta): ${cost:,.2f}, Rev ${revenue:,.2f}")

    except Exception as e:
        print(f"[XMP] 请求失败 (Facebook designer): {e}")

    return results


async def fetch_editor_combined_stats(
    bearer_token: str,
    date_str: str,
    tri: str = None
) -> List[Dict]:
    """
    获取剪辑师完整数据（TikTok + Facebook 合并）

    Args:
        bearer_token: Bearer Token
        date_str: 日期 YYYY-MM-DD

    Returns:
        剪辑师完整统计列表
    """
    # 并行获取 TikTok 和 Facebook 数据
    tt_data = await fetch_editor_tiktok_stats(bearer_token, date_str, tri=tri)
    fb_data = await fetch_editor_facebook_stats(bearer_token, date_str, tri=tri)

    # 合并数据
    results = []
    tt_map = {d['editor_name']: d for d in tt_data}
    fb_map = {d['editor_name']: d for d in fb_data}

    for cn_name in EDITOR_NAME_MAP.keys():
        tt = tt_map.get(cn_name, {'spend': 0, 'revenue': 0})
        fb = fb_map.get(cn_name, {'spend': 0, 'revenue': 0})

        tt_spend = tt.get('spend', 0)
        tt_revenue = tt.get('revenue', 0)
        fb_spend = fb.get('spend', 0)
        fb_revenue = fb.get('revenue', 0)

        total_spend = tt_spend + fb_spend
        total_revenue = tt_revenue + fb_revenue

        results.append({
            'editor_name': cn_name,
            'stat_date': date_str,
            'tt_spend': tt_spend,
            'tt_revenue': tt_revenue,
            'tt_roas': tt_revenue / tt_spend if tt_spend > 0 else 0,
            'meta_spend': fb_spend,
            'meta_revenue': fb_revenue,
            'meta_roas': fb_revenue / fb_spend if fb_spend > 0 else 0,
            'total_spend': total_spend,
            'total_revenue': total_revenue,
            'total_roas': total_revenue / total_spend if total_spend > 0 else 0
        })

    return results


def aggregate_optimizer_stats(campaigns: List[Dict], date_str: str) -> List[Dict]:
    """
    从 campaign 数据聚合投手统计

    Args:
        campaigns: campaign 列表
        date_str: 统计日期

    Returns:
        投手统计列表
    """
    optimizer_data = {}

    for c in campaigns:
        channel = c.get('channel', '')
        campaign_name = c.get('campaign_name', '')

        # 从 campaign_name 提取投手名 (格式: optimizer-xxx)
        optimizer = None
        match = re.search(r'optimizer-([a-zA-Z]+)', campaign_name, re.IGNORECASE)
        if match:
            optimizer = match.group(1).lower()

        if not optimizer:
            continue

        key = (channel, optimizer)
        cost = float(c.get('cost', 0))
        revenue = float(c.get('revenue', 0))

        if key not in optimizer_data:
            optimizer_data[key] = {
                'channel': channel,
                'name': optimizer,
                'stat_date': date_str,
                'campaign_count': 0,
                'total_cost': 0,
                'total_revenue': 0,
                'impressions': 0,
                'clicks': 0,
                'conversions': 0,
                'campaigns': []
            }

        optimizer_data[key]['campaign_count'] += 1
        optimizer_data[key]['total_cost'] += cost
        optimizer_data[key]['total_revenue'] += revenue
        optimizer_data[key]['impressions'] += float(c.get('impression', 0))
        optimizer_data[key]['clicks'] += float(c.get('click', 0))
        optimizer_data[key]['campaigns'].append({
            'name': campaign_name,
            'cost': cost,
            'revenue': revenue,
            'roas': revenue / cost if cost > 0 else 0
        })

    # 计算衍生指标
    result = []
    for key, data in optimizer_data.items():
        cost = data['total_cost']
        revenue = data['total_revenue']

        # 找 Top campaign
        campaigns_sorted = sorted(data['campaigns'], key=lambda x: x['cost'], reverse=True)
        top = campaigns_sorted[0] if campaigns_sorted else None

        result.append({
            'stat_date': date_str,
            'channel': data['channel'],
            'name': data['name'],
            'campaign_count': data['campaign_count'],
            'total_cost': cost,
            'total_revenue': revenue,
            'roas': revenue / cost if cost > 0 else 0,
            'impressions': data['impressions'],
            'clicks': data['clicks'],
            'conversions': data['conversions'],
            'top_campaign': top['name'] if top else '',
            'top_campaign_spend': top['cost'] if top else 0,
            'top_campaign_roas': top['roas'] if top else 0,
        })

    result.sort(key=lambda x: x['total_cost'], reverse=True)
    return result


def aggregate_editor_stats_from_campaigns(campaigns: List[Dict], date_str: str) -> List[Dict]:
    """
    从 campaign 数据聚合剪辑师统计 (TikTok 从广告名称解析)

    Args:
        campaigns: campaign 列表
        date_str: 统计日期

    Returns:
        剪辑师统计列表
    """
    editor_data = {}

    for c in campaigns:
        channel = c.get('channel', '')
        campaign_name = c.get('campaign_name', '')

        # 从 campaign_name 提取剪辑师名
        editor = None
        # 先匹配中文名
        for cn_name in EDITOR_NAME_MAP.keys():
            if cn_name in campaign_name:
                editor = cn_name
                break
        # 再匹配别名
        if not editor:
            name_lower = campaign_name.lower()
            for alias, cn_name in EDITOR_ALIAS_MAP.items():
                if alias.lower() in name_lower:
                    editor = cn_name
                    break

        if not editor:
            continue

        key = (channel, editor)
        cost = float(c.get('cost', 0))
        revenue = float(c.get('revenue', 0))
        roas = revenue / cost if cost > 0 else 0

        if key not in editor_data:
            editor_data[key] = {
                'channel': channel,
                'name': editor,
                'stat_date': date_str,
                'material_count': 0,
                'total_cost': 0,
                'total_revenue': 0,
                'impressions': 0,
                'clicks': 0,
                'hot_count': 0,
                'materials': []
            }

        editor_data[key]['material_count'] += 1
        editor_data[key]['total_cost'] += cost
        editor_data[key]['total_revenue'] += revenue
        editor_data[key]['impressions'] += float(c.get('impression', 0))
        editor_data[key]['clicks'] += float(c.get('click', 0))

        # 爆款判定: 消耗 > $500 且 ROAS >= 45%
        if cost >= 500 and roas >= 0.45:
            editor_data[key]['hot_count'] += 1

        editor_data[key]['materials'].append({
            'name': campaign_name,
            'cost': cost,
            'revenue': revenue,
            'roas': roas
        })

    # 计算衍生指标
    result = []
    for key, data in editor_data.items():
        cost = data['total_cost']
        revenue = data['total_revenue']
        count = data['material_count']

        # 找 Top 素材
        materials_sorted = sorted(data['materials'], key=lambda x: x['cost'], reverse=True)
        top = materials_sorted[0] if materials_sorted else None

        result.append({
            'stat_date': date_str,
            'channel': data['channel'],
            'name': data['name'],
            'material_count': count,
            'total_cost': cost,
            'total_revenue': revenue,
            'd0_roas': revenue / cost if cost > 0 else 0,
            'impressions': data['impressions'],
            'clicks': data['clicks'],
            'hot_count': data['hot_count'],
            'hot_rate': data['hot_count'] / count if count > 0 else 0,
            'top_material': top['name'] if top else '',
            'top_material_cost': top['cost'] if top else 0,
            'top_material_roas': top['roas'] if top else 0,
        })

    result.sort(key=lambda x: x['total_cost'], reverse=True)
    return result


def aggregate_editor_stats_from_ads(ads: List[Dict], date_str: str) -> List[Dict]:
    """
    从广告维度数据聚合剪辑师统计 (分渠道)

    广告名称格式:
    - 12.25-宋-Eldest Daughter's Marriage Life-ko-4.mp4
    - 15000696_ja_vc_beita_1229_hilight_3.mp4

    Args:
        ads: 广告列表 (包含 channel, ad_name, cost 等)
        date_str: 统计日期

    Returns:
        剪辑师统计列表 (分渠道)
    """
    editor_data = {}

    for ad in ads:
        channel = ad.get('channel', '')
        ad_name = ad.get('ad_name', '')

        # 从 ad_name 提取剪辑师名
        editor = extract_editor_from_ad_name(ad_name)

        if not editor:
            continue

        key = (channel, editor)
        cost = float(ad.get('cost', 0))
        revenue = float(ad.get('revenue', 0))

        if key not in editor_data:
            editor_data[key] = {
                'channel': channel,
                'name': editor,
                'stat_date': date_str,
                'material_count': 0,
                'total_cost': 0,
                'total_revenue': 0,
                'impressions': 0,
                'clicks': 0,
                'conversions': 0,
                'ads': []
            }

        editor_data[key]['material_count'] += 1
        editor_data[key]['total_cost'] += cost
        editor_data[key]['total_revenue'] += revenue
        editor_data[key]['impressions'] += float(ad.get('impression', 0))
        editor_data[key]['clicks'] += float(ad.get('click', 0))
        editor_data[key]['conversions'] += float(ad.get('conversion', 0))
        editor_data[key]['ads'].append({
            'name': ad_name,
            'cost': cost,
            'revenue': revenue,
        })

    # 计算衍生指标
    result = []
    for key, data in editor_data.items():
        cost = data['total_cost']
        revenue = data['total_revenue']
        count = data['material_count']
        d0_roas = revenue / cost if cost > 0 else 0

        # 找 Top 素材 (按消耗排序)
        ads_sorted = sorted(data['ads'], key=lambda x: x['cost'], reverse=True)
        top = ads_sorted[0] if ads_sorted else None
        top_roas = top['revenue'] / top['cost'] if top and top['cost'] > 0 else 0

        result.append({
            'stat_date': date_str,
            'channel': data['channel'],
            'name': data['name'],
            'material_count': count,
            'total_cost': cost,
            'total_revenue': revenue,
            'd0_roas': d0_roas,
            'impressions': data['impressions'],
            'clicks': data['clicks'],
            'hot_count': 0,
            'hot_rate': 0,
            'top_material': top['name'] if top else '',
            'top_material_cost': top['cost'] if top else 0,
            'top_material_roas': top_roas,
        })

    result.sort(key=lambda x: x['total_cost'], reverse=True)
    print(f"[XMP] 从广告数据提取剪辑师: {len(result)} 条 (分渠道)")
    return result


def extract_editor_from_ad_name(ad_name: str) -> Optional[str]:
    """
    从广告名称中提取剪辑师名

    支持两种格式:
    1. 12.25-宋-Eldest Daughter's Marriage Life-ko-4.mp4
    2. 15000696_ja_vc_beita_1229_hilight_3.mp4
    """
    if not ad_name:
        return None

    # 格式1: 日期-剪辑师-剧名-语言-序号.mp4
    if '-' in ad_name:
        parts = ad_name.split('-')
        if len(parts) >= 2:
            editor_part = parts[1].strip()
            # 匹配中文名或别名
            if editor_part in EDITOR_ALIAS_MAP:
                return EDITOR_ALIAS_MAP[editor_part]
            # 匹配单姓
            for cn_name, aliases in EDITOR_NAME_MAP.items():
                if editor_part == cn_name:
                    return cn_name
                for alias in aliases:
                    if alias.lower() == editor_part.lower():
                        return cn_name

    # 格式2: dramaid_lang_vc_editor_date_xxx.mp4
    if '_' in ad_name:
        parts = ad_name.split('_')
        for part in parts:
            part_lower = part.lower()
            # 匹配英文名
            for cn_name, aliases in EDITOR_NAME_MAP.items():
                for alias in aliases:
                    if alias.lower() == part_lower:
                        return cn_name

    return None


def aggregate_editor_stats_from_material_report(date_str: str) -> List[Dict]:
    """
    从素材报表聚合剪辑师统计

    素材名称格式: 日期-剪辑师名-剧名-语言-序号.mp4
    """
    scraper = XMPEditorStatsScraper()
    material_data = scraper.fetch_material_report(date_str, date_str)

    if not material_data:
        print("[XMP] 素材报表为空")
        return []

    editor_data = {}

    for item in material_data:
        material_name = item.get('material_name', '')
        editor = extract_editor_from_material_name(material_name)

        if not editor:
            continue

        cost = float(item.get('currency_cost', 0))
        revenue = float(item.get('total_purchase_value', 0))
        roas = revenue / cost if cost > 0 else 0

        # 素材报表不区分渠道，统一归类
        channel = 'all'
        key = (channel, editor)

        if key not in editor_data:
            editor_data[key] = {
                'channel': channel,
                'name': editor,
                'stat_date': date_str,
                'material_count': 0,
                'total_cost': 0,
                'total_revenue': 0,
                'hot_count': 0,
                'materials': []
            }

        editor_data[key]['material_count'] += 1
        editor_data[key]['total_cost'] += cost
        editor_data[key]['total_revenue'] += revenue

        # 爆款判定: 消耗 > $500 且 ROAS >= 45%
        if cost >= 500 and roas >= 0.45:
            editor_data[key]['hot_count'] += 1

        editor_data[key]['materials'].append({
            'name': material_name,
            'cost': cost,
            'revenue': revenue,
            'roas': roas
        })

    # 计算衍生指标
    result = []
    for key, data in editor_data.items():
        cost = data['total_cost']
        revenue = data['total_revenue']
        count = data['material_count']

        # 找 Top 素材
        materials_sorted = sorted(data['materials'], key=lambda x: x['cost'], reverse=True)
        top = materials_sorted[0] if materials_sorted else None

        result.append({
            'stat_date': date_str,
            'channel': data['channel'],
            'name': data['name'],
            'material_count': count,
            'total_cost': cost,
            'total_revenue': revenue,
            'd0_roas': revenue / cost if cost > 0 else 0,
            'hot_count': data['hot_count'],
            'hot_rate': data['hot_count'] / count if count > 0 else 0,
            'top_material': top['name'] if top else '',
            'top_material_cost': top['cost'] if top else 0,
            'top_material_roas': top['roas'] if top else 0,
        })

    result.sort(key=lambda x: x['total_cost'], reverse=True)
    print(f"[XMP] 从素材报表提取剪辑师: {len(result)} 人")
    return result


async def run_with_stats(date_str: str = None, upload_bq: bool = False):
    """执行抓取并生成投手/剪辑师统计"""
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    # 1. 抓取 campaign 数据
    scraper = XMPMultiChannelScraper()
    result = await scraper.fetch_all_channels(date_str, date_str)

    campaigns = result.get('campaigns', [])
    if not campaigns:
        print("[XMP] 无 campaign 数据")
        return result

    # 2. 并行获取: 投手统计 + 剪辑师数据
    optimizer_task = fetch_optimizer_summary_stats(scraper.bearer_token, date_str, tri=scraper.tri)
    designers_0_task = scraper.fetch_channel_designers('facebook', date_str, date_str, is_xmp="0")
    designers_1_task = scraper.fetch_channel_designers('facebook', date_str, date_str, is_xmp="1")
    tk_ads_task = scraper.fetch_channel_ads('tiktok', date_str, date_str)

    optimizer_stats, designers_0, designers_1, tk_ads = await asyncio.gather(
        optimizer_task, designers_0_task, designers_1_task, tk_ads_task
    )

    print(f"[XMP] 投手统计: {len(optimizer_stats)} 人")

    # 3. 汇总剪辑师数据
    editor_stats = []

    meta_designers = []
    if designers_0:
        meta_designers.extend(designers_0)
    if designers_1:
        meta_designers.extend(designers_1)

    if meta_designers:
        for d in meta_designers:
            cost = d['cost']
            revenue = d['revenue']
            editor_stats.append({
                'stat_date': date_str,
                'channel': 'facebook',
                'name': d['designer_name'],
                'total_cost': cost,
                'total_revenue': revenue,
                'd0_roas': revenue / cost if cost > 0 else 0,
                'impressions': d['impression'],
                'clicks': d['click'],
                'material_count': 0,
                'hot_count': 0,
                'hot_rate': 0,
                'top_material': '',
                'top_material_cost': 0,
                'top_material_roas': 0,
            })

    if tk_ads:
        tk_editor_stats = aggregate_editor_stats_from_ads(tk_ads, date_str)
        editor_stats.extend(tk_editor_stats)

    # 按消耗排序
    editor_stats.sort(key=lambda x: x['total_cost'], reverse=True)
    print(f"[XMP] 剪辑师统计: {len(editor_stats)} 人")

    result['optimizer_stats'] = optimizer_stats
    result['editor_stats'] = editor_stats

    # 4. 并行上传到 BigQuery
    if upload_bq:
        try:
            from bigquery_storage import BigQueryUploader

            project_id = os.getenv('BQ_PROJECT_ID')
            dataset_id = os.getenv('BQ_DATASET_ID', 'xmp_data')

            if project_id:
                batch_id = datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M%S')
                uploader = BigQueryUploader(project_id, dataset_id)

                async def _upload_campaigns():
                    count = uploader.upload_xmp_internal_campaigns(campaigns, batch_id=batch_id)
                    print(f"[BQ] 已上传 {count} 条 campaign 记录")
                    return count

                async def _upload_optimizer():
                    count = uploader.upload_optimizer_stats(optimizer_stats, batch_id=batch_id)
                    print(f"[BQ] 已上传 {count} 条投手统计")
                    return count

                async def _upload_editor():
                    count = uploader.upload_editor_stats(editor_stats, batch_id=batch_id)
                    print(f"[BQ] 已上传 {count} 条剪辑师统计")
                    return count

                await asyncio.gather(
                    _upload_campaigns(), _upload_optimizer(), _upload_editor()
                )
            else:
                print("[BQ] 未配置 BQ_PROJECT_ID，跳过上传")
        except Exception as e:
            print(f"[BQ] 上传失败: {e}")
            send_lark_alert("XMP 统计数据上传失败", str(e), level="error")

    return result


def query_stats_from_bq(date_str: str) -> Dict[str, List[Dict]]:
    """
    从 BigQuery 查询投手/剪辑师统计数据

    Args:
        date_str: 日期 YYYY-MM-DD

    Returns:
        {'optimizer_stats': [...], 'editor_stats': [...], 'batch_id': str, 'batch_valid': bool}
    """
    from google.cloud import bigquery
    from datetime import datetime, timedelta

    project_id = os.getenv('BQ_PROJECT_ID')
    if not project_id:
        print("[BQ] 未配置 BQ_PROJECT_ID")
        return {'optimizer_stats': [], 'editor_stats': [], 'batch_id': None, 'batch_valid': False}

    client = bigquery.Client(project=project_id)

    # 直接使用最新的 batch_id（包含最完整的数据）
    batch_id = None
    latest_batch_query = f"""
    SELECT MAX(batch_id) as latest_batch_id
    FROM `{project_id}.xmp_data.xmp_optimizer_stats`
    WHERE stat_date = '{date_str}'
    """
    try:
        for row in client.query(latest_batch_query):
            batch_id = row.latest_batch_id
        if batch_id:
            print(f"[BQ] ✓ 找到最新数据 batch: {batch_id}")
    except Exception as e:
        print(f"[BQ] 查询 batch_id 失败: {e}")
        return {'optimizer_stats': [], 'editor_stats': [], 'batch_id': None, 'batch_valid': False}

    if not batch_id:
        print(f"[BQ] 未找到 {date_str} 的数据")
        return {'optimizer_stats': [], 'editor_stats': [], 'batch_id': None, 'batch_valid': False}

    # 2. 校验 batch_id 时效性
    # 对于 T-1 日报，batch_id 应该是 T 日（今天）抓取的
    # 如果 batch_id 的日期 <= stat_date，说明凌晨2点的任务没有执行成功
    batch_valid = True
    try:
        batch_date_str = batch_id[:8]  # 提取 YYYYMMDD
        batch_date = datetime.strptime(batch_date_str, '%Y%m%d').date()
        stat_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        today = datetime.now().date()

        # 如果是查询历史日期（T-1 或更早），batch 应该是 stat_date 之后抓取的
        if stat_date < today:
            if batch_date <= stat_date:
                print(f"[BQ] ⚠️ 数据时效性校验失败!")
                print(f"[BQ]   stat_date={date_str}, batch抓取日期={batch_date_str}")
                print(f"[BQ]   batch 应该是 {date_str} 之后抓取的，但实际是当天或更早抓取")
                print(f"[BQ]   可能原因: 凌晨2点的 T-1 数据采集任务未执行")
                batch_valid = False
            else:
                print(f"[BQ] ✓ 数据时效性校验通过: batch 在 {batch_date_str} 抓取")
    except Exception as e:
        print(f"[BQ] 校验 batch 时效性时出错: {e}")

    # 3. 查询投手统计（使用最新 batch_id）
    opt_query = f"""
    SELECT
        optimizer_name as name,
        channel,
        spend as total_cost,
        revenue as total_revenue,
        roas
    FROM `{project_id}.xmp_data.xmp_optimizer_stats`
    WHERE stat_date = '{date_str}' AND batch_id = '{batch_id}'
    ORDER BY spend DESC
    """

    optimizer_stats = []
    try:
        for row in client.query(opt_query):
            optimizer_stats.append({
                'name': row.name,
                'channel': row.channel,
                'total_cost': float(row.total_cost or 0),
                'total_revenue': float(row.total_revenue or 0),
                'roas': float(row.roas or 0),
            })
        print(f"[BQ] 查询到 {len(optimizer_stats)} 条投手数据")
    except Exception as e:
        print(f"[BQ] 查询投手数据失败: {e}")

    # 4. 查询剪辑师统计（使用最新 batch_id）
    editor_query = f"""
    SELECT
        editor_name as name,
        channel,
        spend as total_cost,
        revenue as total_revenue,
        roas
    FROM `{project_id}.xmp_data.xmp_editor_stats`
    WHERE stat_date = '{date_str}' AND batch_id = '{batch_id}'
    ORDER BY spend DESC
    """

    editor_stats = []
    try:
        for row in client.query(editor_query):
            editor_stats.append({
                'name': row.name,
                'channel': row.channel,
                'total_cost': float(row.total_cost or 0),
                'total_revenue': float(row.total_revenue or 0),
                'roas': float(row.roas or 0),
            })
        print(f"[BQ] 查询到 {len(editor_stats)} 条剪辑师数据")
    except Exception as e:
        print(f"[BQ] 查询剪辑师数据失败: {e}")

    return {
        'optimizer_stats': optimizer_stats,
        'editor_stats': editor_stats,
        'batch_id': batch_id,
        'batch_valid': batch_valid
    }


def query_weekly_stats_from_bq(start_date: str, end_date: str) -> Dict[str, Any]:
    """
    从 BigQuery 查询周报数据（汇总一周的投手/剪辑师统计）

    Args:
        start_date: 开始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD

    Returns:
        {
            'optimizer_stats': [...],
            'editor_stats': [...],
            'invalid_dates': [],  # batch 校验失败的日期
            'all_valid': bool
        }
    """
    from datetime import datetime, timedelta

    all_opt_stats = []
    all_editor_stats = []
    invalid_dates = []

    # 逐天查询数据
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        print(f"\n--- 查询 {date_str} ---")

        bq_data = query_stats_from_bq(date_str)

        if not bq_data.get('batch_valid', True):
            invalid_dates.append(date_str)
            print(f"[周报] ⚠️ {date_str} 数据校验失败，跳过")
        else:
            all_opt_stats.extend(bq_data.get('optimizer_stats', []))
            all_editor_stats.extend(bq_data.get('editor_stats', []))

        current += timedelta(days=1)

    return {
        'optimizer_stats': all_opt_stats,
        'editor_stats': all_editor_stats,
        'invalid_dates': invalid_dates,
        'all_valid': len(invalid_dates) == 0
    }


def export_stats_to_lark_doc(
    optimizer_stats: List[Dict],
    editor_stats: List[Dict],
    date_str: str,
    doc_token: str = None
) -> bool:
    """
    导出投手/剪辑师统计到飞书文档

    Args:
        optimizer_stats: 投手统计数据
        editor_stats: 剪辑师统计数据
        date_str: 日期
        doc_token: 飞书文档 token，不传则从环境变量 XMP_DOC_TOKEN 获取

    Returns:
        是否成功
    """
    try:
        from lark.lark_doc_client import create_doc_client
    except ImportError:
        print("[飞书] 无法导入 lark_doc_client 模块")
        return False

    doc_token = doc_token or os.getenv('XMP_DOC_TOKEN')
    if not doc_token:
        print("[飞书] 未配置 XMP_DOC_TOKEN，跳过文档写入")
        return False

    # 合并 Meta + TikTok 数据
    def merge_by_name(stats: List[Dict]) -> List[Dict]:
        merged = {}
        for s in stats:
            name = s.get('name')
            if name not in merged:
                merged[name] = {
                    'name': name,
                    'meta_spend': 0, 'meta_revenue': 0,
                    'tt_spend': 0, 'tt_revenue': 0,
                    # 剪辑师特有字段
                    'material_count': 0,
                    'hot_count': 0,
                    'top_materials': [],  # 存储所有素材，用于找 Top
                }
            channel = s.get('channel', '')
            spend = s.get('total_cost', 0)
            revenue = s.get('total_revenue', 0)

            if channel == 'facebook':
                merged[name]['meta_spend'] += spend
                merged[name]['meta_revenue'] += revenue
            elif channel == 'tiktok':
                merged[name]['tt_spend'] += spend
                merged[name]['tt_revenue'] += revenue

            # 合并剪辑师特有字段
            material_count = s.get('material_count', 0)
            hot_count = s.get('hot_count', 0)
            top_material = s.get('top_material', '')
            top_material_cost = s.get('top_material_cost', 0)
            top_material_roas = s.get('top_material_roas', 0)

            merged[name]['material_count'] += material_count
            merged[name]['hot_count'] += hot_count

            # 记录 Top 素材（用于后续选择消耗最高的）
            if top_material:
                merged[name]['top_materials'].append({
                    'name': top_material,
                    'cost': top_material_cost,
                    'roas': top_material_roas,
                    'channel': channel
                })

        result = []
        for name, d in merged.items():
            total_spend = d['meta_spend'] + d['tt_spend']
            total_revenue = d['meta_revenue'] + d['tt_revenue']
            material_count = d['material_count']
            hot_count = d['hot_count']

            # 选择消耗最高的 Top 素材
            top_materials = d['top_materials']
            if top_materials:
                top_mat = max(top_materials, key=lambda x: x['cost'])
                top_material = top_mat['name']
                top_material_cost = top_mat['cost']
                top_material_roas = top_mat['roas']
            else:
                top_material = ''
                top_material_cost = 0
                top_material_roas = 0

            result.append({
                'name': name,
                'meta_spend': d['meta_spend'],
                'meta_revenue': d['meta_revenue'],
                'meta_roas': d['meta_revenue'] / d['meta_spend'] if d['meta_spend'] > 0 else 0,
                'tt_spend': d['tt_spend'],
                'tt_revenue': d['tt_revenue'],
                'tt_roas': d['tt_revenue'] / d['tt_spend'] if d['tt_spend'] > 0 else 0,
                'total_spend': total_spend,
                'total_revenue': total_revenue,
                'total_roas': total_revenue / total_spend if total_spend > 0 else 0,
                # 剪辑师特有字段
                'material_count': material_count,
                'hot_count': hot_count,
                'hot_rate': hot_count / material_count if material_count > 0 else 0,
                'top_material': top_material,
                'top_material_cost': top_material_cost,
                'top_material_roas': top_material_roas,
            })
        result.sort(key=lambda x: x['total_spend'], reverse=True)
        return result

    # 标注 Spend/ROAS 第一
    def add_labels(data: List[Dict]) -> List[Dict]:
        if not data:
            return data
        spend_first = max(data, key=lambda x: x['total_spend'])
        qualified = [d for d in data if d['total_spend'] >= 100]
        roas_first = max(qualified, key=lambda x: x['total_roas']) if qualified else None
        for d in data:
            labels = []
            if d == spend_first:
                labels.append('Spend Top1')
            if d == roas_first:
                labels.append('ROAS Top1')
            d['label'] = ' | '.join(labels) if labels else ''
        return data

    opt_merged = merge_by_name(optimizer_stats)
    editor_merged = merge_by_name(editor_stats)

    # 先过滤掉韩国投手，再计算标签
    KR_OPTIMIZERS = ['juria', 'lyla', 'jade']
    opt_merged = [o for o in opt_merged if o.get('name', '').lower() not in KR_OPTIMIZERS]
    opt_merged = add_labels(opt_merged)

    # 先过滤掉韩国剪辑师，再计算标签
    KR_EDITORS = ['sydney', 'dia', 'gyeommy']
    editor_merged = [e for e in editor_merged if e.get('name', '').lower() not in KR_EDITORS]
    editor_merged = add_labels(editor_merged)

    try:
        client = create_doc_client()

        # 判断是否是 Wiki token (需要先获取实际的 doc_token)
        # Wiki token 通常以 wiki/ 开头或者直接是 wiki_token
        wiki_token = doc_token

        # 尝试获取 Wiki 节点信息
        node_info = client.get_wiki_node_info(wiki_token)
        if node_info.get("code") == 0:
            # 是 Wiki 页面，获取实际的 doc_token
            node = node_info.get("data", {}).get("node", {})
            actual_doc_token = node.get("obj_token")
            obj_type = node.get("obj_type")
            print(f"[飞书] Wiki 节点: obj_token={actual_doc_token}, obj_type={obj_type}")

            if obj_type != "docx":
                print(f"[飞书] Wiki 节点类型不是文档: {obj_type}")
                return False
            doc_token = actual_doc_token

        result = client.write_xmp_daily_report(
            doc_token=doc_token,
            date_str=date_str,
            optimizer_data=opt_merged,
            editor_data=editor_merged
        )
        if result.get('code') == 0:
            print(f"[飞书] 日报已写入文档: {doc_token}")
            return True
        else:
            print(f"[飞书] 写入失败: {result.get('msg')}")
            return False
    except Exception as e:
        print(f"[飞书] 写入异常: {e}")
        return False


def export_stats_to_excel(
    optimizer_stats: List[Dict],
    editor_stats: List[Dict],
    date_str: str,
    output_path: str = None
) -> str:
    """
    导出投手/剪辑师统计到 Excel

    Excel 包含两个 Sheet:
    - 投手日报: Meta/TT 分渠道 + 汇总 + Spend/ROAS Top1 标注
    - 剪辑师日报: 同上
    """
    try:
        import pandas as pd
    except ImportError:
        print("[Excel] 需要安装 pandas: pip install pandas openpyxl")
        return None

    filename = output_path or f"xmp_stats_{date_str.replace('-', '')}.xlsx"

    # 合并 Meta + TikTok 数据
    def merge_by_name(stats: List[Dict]) -> List[Dict]:
        merged = {}
        for s in stats:
            name = s.get('name')
            if name not in merged:
                merged[name] = {
                    'name': name,
                    'meta_spend': 0, 'meta_revenue': 0,
                    'tt_spend': 0, 'tt_revenue': 0,
                }
            channel = s.get('channel', '')
            spend = s.get('total_cost', 0)
            revenue = s.get('total_revenue', 0)

            if channel == 'facebook':
                merged[name]['meta_spend'] += spend
                merged[name]['meta_revenue'] += revenue
            elif channel == 'tiktok':
                merged[name]['tt_spend'] += spend
                merged[name]['tt_revenue'] += revenue

        result = []
        for name, d in merged.items():
            total_spend = d['meta_spend'] + d['tt_spend']
            total_revenue = d['meta_revenue'] + d['tt_revenue']
            result.append({
                'name': name,
                'meta_spend': d['meta_spend'],
                'meta_revenue': d['meta_revenue'],
                'meta_roas': d['meta_revenue'] / d['meta_spend'] if d['meta_spend'] > 0 else 0,
                'tt_spend': d['tt_spend'],
                'tt_revenue': d['tt_revenue'],
                'tt_roas': d['tt_revenue'] / d['tt_spend'] if d['tt_spend'] > 0 else 0,
                'total_spend': total_spend,
                'total_revenue': total_revenue,
                'total_roas': total_revenue / total_spend if total_spend > 0 else 0,
            })
        result.sort(key=lambda x: x['total_spend'], reverse=True)
        return result

    # 标注 Spend/ROAS 第一
    def add_labels(data: List[Dict]) -> List[Dict]:
        if not data:
            return data
        spend_first = max(data, key=lambda x: x['total_spend'])
        qualified = [d for d in data if d['total_spend'] >= 100]
        roas_first = max(qualified, key=lambda x: x['total_roas']) if qualified else None
        for d in data:
            labels = []
            if d == spend_first:
                labels.append('Spend Top1')
            if d == roas_first:
                labels.append('ROAS Top1')
            d['label'] = ' | '.join(labels) if labels else ''
        return data

    opt_merged = add_labels(merge_by_name(optimizer_stats))
    editor_merged = add_labels(merge_by_name(editor_stats))

    # 写入 Excel
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        if opt_merged:
            opt_df = pd.DataFrame(opt_merged)
            opt_df.columns = ['投手', 'Meta Spend', 'Meta Revenue', 'Meta ROAS',
                              'TT Spend', 'TT Revenue', 'TT ROAS',
                              '总 Spend', '总 Revenue', '总 ROAS', '标注']
            opt_df.to_excel(writer, sheet_name='投手日报', index=False)

        if editor_merged:
            ed_df = pd.DataFrame(editor_merged)
            ed_df.columns = ['剪辑师', 'Meta Spend', 'Meta Revenue', 'Meta ROAS',
                             'TT Spend', 'TT Revenue', 'TT ROAS',
                             '总 Spend', '总 Revenue', '总 ROAS', '标注']
            ed_df.to_excel(writer, sheet_name='剪辑师日报', index=False)

    print(f"[Excel] 已导出: {filename}")
    return filename


def generate_weekly_summary(
    optimizer_stats: List[Dict],
    editor_stats: List[Dict],
    start_date: str,
    end_date: str,
    min_spend_threshold: float = 1000.0
) -> Dict[str, Any]:
    """
    生成周报汇总数据

    功能:
    - 一周数据简单相加汇总
    - 筛选最佳投手 1 名 (综合评分最高)
    - 筛选最佳剪辑师 1 名 (综合评分最高)

    评分规则:
    - 综合评分 = Spend 排名分 + ROAS 排名分
    - 只有 Spend >= min_spend_threshold 的才参与最佳评选

    Args:
        optimizer_stats: 投手统计列表 (可包含多天数据)
        editor_stats: 剪辑师统计列表
        start_date: 周开始日期
        end_date: 周结束日期
        min_spend_threshold: 参与最佳评选的最低消耗阈值

    Returns:
        {
            'period': {start_date, end_date},
            'optimizer_summary': 投手周汇总,
            'editor_summary': 剪辑师周汇总,
            'best_optimizer': 最佳投手,
            'best_editor': 最佳剪辑师
        }
    """
    print(f"[周报] 生成周报汇总: {start_date} ~ {end_date}")

    def merge_weekly_stats(stats: List[Dict]) -> List[Dict]:
        """合并一周数据 (按人名聚合)"""
        merged = {}
        for s in stats:
            name = s.get('name')
            if not name:
                continue
            if name not in merged:
                merged[name] = {
                    'name': name,
                    'meta_spend': 0, 'meta_revenue': 0,
                    'tt_spend': 0, 'tt_revenue': 0,
                    'all_spend': 0, 'all_revenue': 0,
                }
            channel = s.get('channel', '')
            spend = float(s.get('total_cost', 0))
            revenue = float(s.get('total_revenue', 0))

            if channel == 'facebook':
                merged[name]['meta_spend'] += spend
                merged[name]['meta_revenue'] += revenue
            elif channel == 'tiktok':
                merged[name]['tt_spend'] += spend
                merged[name]['tt_revenue'] += revenue
            elif channel == 'all':
                merged[name]['all_spend'] += spend
                merged[name]['all_revenue'] += revenue

        result = []
        for name, d in merged.items():
            total_spend = d['meta_spend'] + d['tt_spend'] + d['all_spend']
            total_revenue = d['meta_revenue'] + d['tt_revenue'] + d['all_revenue']
            result.append({
                'name': name,
                'meta_spend': d['meta_spend'],
                'meta_revenue': d['meta_revenue'],
                'meta_roas': d['meta_revenue'] / d['meta_spend'] if d['meta_spend'] > 0 else 0,
                'tt_spend': d['tt_spend'],
                'tt_revenue': d['tt_revenue'],
                'tt_roas': d['tt_revenue'] / d['tt_spend'] if d['tt_spend'] > 0 else 0,
                'total_spend': total_spend,
                'total_revenue': total_revenue,
                'total_roas': total_revenue / total_spend if total_spend > 0 else 0,
            })
        result.sort(key=lambda x: x['total_spend'], reverse=True)
        return result

    def find_best_performer(stats: List[Dict], threshold: float) -> Optional[Dict]:
        """找出最佳表现者 (综合评分)"""
        qualified = [s for s in stats if s['total_spend'] >= threshold]
        if not qualified:
            return None

        # 按 Spend 排名
        spend_sorted = sorted(qualified, key=lambda x: x['total_spend'], reverse=True)
        for i, s in enumerate(spend_sorted):
            s['spend_rank'] = i + 1

        # 按 ROAS 排名
        roas_sorted = sorted(qualified, key=lambda x: x['total_roas'], reverse=True)
        for i, s in enumerate(roas_sorted):
            s['roas_rank'] = i + 1

        # 综合评分 = Spend排名 + ROAS排名 (越小越好)
        for s in qualified:
            s['combined_score'] = s['spend_rank'] + s['roas_rank']

        # 找综合评分最低的
        best = min(qualified, key=lambda x: x['combined_score'])
        return best

    # 合并周数据
    opt_weekly = merge_weekly_stats(optimizer_stats)
    editor_weekly = merge_weekly_stats(editor_stats)

    # 找最佳表现者
    best_opt = find_best_performer(opt_weekly, min_spend_threshold)
    best_editor = find_best_performer(editor_weekly, min_spend_threshold)

    # 计算汇总
    opt_total_spend = sum(s['total_spend'] for s in opt_weekly)
    opt_total_revenue = sum(s['total_revenue'] for s in opt_weekly)
    editor_total_spend = sum(s['total_spend'] for s in editor_weekly)
    editor_total_revenue = sum(s['total_revenue'] for s in editor_weekly)

    result = {
        'period': {'start_date': start_date, 'end_date': end_date},
        'optimizer_summary': {
            'count': len(opt_weekly),
            'total_spend': opt_total_spend,
            'total_revenue': opt_total_revenue,
            'avg_roas': opt_total_revenue / opt_total_spend if opt_total_spend > 0 else 0,
            'details': opt_weekly
        },
        'editor_summary': {
            'count': len(editor_weekly),
            'total_spend': editor_total_spend,
            'total_revenue': editor_total_revenue,
            'avg_roas': editor_total_revenue / editor_total_spend if editor_total_spend > 0 else 0,
            'details': editor_weekly
        },
        'best_optimizer': best_opt,
        'best_editor': best_editor
    }

    # 打印周报
    print(f"\n{'='*70}")
    print(f"周报汇总 ({start_date} ~ {end_date})")
    print(f"{'='*70}")

    print(f"\n投手周汇总 ({len(opt_weekly)} 人):")
    print(f"  总消耗: ${opt_total_spend:,.0f}")
    print(f"  总收入: ${opt_total_revenue:,.0f}")
    print(f"  平均 ROAS: {opt_total_revenue/opt_total_spend*100:.1f}%" if opt_total_spend > 0 else "")

    if best_opt:
        print(f"\n  最佳投手: {best_opt['name']}")
        print(f"    Spend: ${best_opt['total_spend']:,.0f} (排名 #{best_opt['spend_rank']})")
        print(f"    ROAS: {best_opt['total_roas']*100:.1f}% (排名 #{best_opt['roas_rank']})")
        print(f"    综合评分: {best_opt['combined_score']}")

    print(f"\n剪辑师周汇总 ({len(editor_weekly)} 人):")
    print(f"  总消耗: ${editor_total_spend:,.0f}")
    print(f"  总收入: ${editor_total_revenue:,.0f}")
    print(f"  平均 ROAS: {editor_total_revenue/editor_total_spend*100:.1f}%" if editor_total_spend > 0 else "")

    if best_editor:
        print(f"\n  最佳剪辑师: {best_editor['name']}")
        print(f"    Spend: ${best_editor['total_spend']:,.0f} (排名 #{best_editor['spend_rank']})")
        print(f"    ROAS: {best_editor['total_roas']*100:.1f}% (排名 #{best_editor['roas_rank']})")
        print(f"    综合评分: {best_editor['combined_score']}")

    print(f"{'='*70}\n")

    return result


async def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='XMP 多渠道数据抓取')
    parser.add_argument('--date', help='日期 YYYY-MM-DD，默认今天')
    parser.add_argument('--yesterday', action='store_true', help='采集昨天 (T-1) 的数据')
    parser.add_argument('--upload', action='store_true', help='上传到 BigQuery')
    parser.add_argument('--stats', action='store_true', help='生成投手/剪辑师统计')
    parser.add_argument('--excel', action='store_true', help='导出 Excel 文件')
    parser.add_argument('--lark-doc', action='store_true', help='导出到飞书文档')
    parser.add_argument('--from-bq', action='store_true', help='从 BigQuery 读取数据（不重新抓取）')
    parser.add_argument('--doc-token', help='飞书文档 token，默认从 XMP_DOC_TOKEN 环境变量获取')
    parser.add_argument('--weekly', action='store_true', help='生成周报汇总')
    parser.add_argument('--days', type=int, default=7, help='周报天数，默认7天')
    parser.add_argument('--relogin', action='store_true', help='强制重新登录获取 Token')
    args = parser.parse_args()

    # 如果指定了 --relogin，删除 token 文件强制重新登录
    if args.relogin:
        if os.path.exists(XMP_TOKEN_FILE):
            os.remove(XMP_TOKEN_FILE)
            print(f"[XMP] 已删除旧 Token 文件，将重新登录")
        else:
            print(f"[XMP] Token 文件不存在，将进行登录")

    # 确定采集日期
    # 优先级: --date > --yesterday > FETCH_YESTERDAY 环境变量 > 今天
    if args.date:
        date_str = args.date
    elif args.yesterday or os.getenv('FETCH_YESTERDAY', '').lower() == 'true':
        date_str = (datetime.now(BEIJING_TZ) - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"[XMP] 采集 T-1 日数据: {date_str}")
    else:
        date_str = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')

    # 周报模式
    if args.weekly:
        end_date = datetime.strptime(date_str, '%Y-%m-%d')

        # 使用 --days 参数计算日期范围（支持7天完整周报）
        days_count = args.days
        start_date = end_date - timedelta(days=days_count - 1)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        # 生成日期列表（包括周末）
        date_list = []
        current = start_date
        while current <= end_date:
            date_list.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)

        print(f"[周报] 统计周期: {start_str} ~ {end_str} ({days_count}天)")
        print(f"[周报] 日期列表: {', '.join(date_list)}")

        # 从 BigQuery 读取数据（--from-bq 模式）
        if getattr(args, 'from_bq', False):
            print(f"[周报] 从 BigQuery 读取数据...")
            bq_data = query_weekly_stats_from_bq(start_str, end_str)
            all_opt_stats = bq_data.get('optimizer_stats', [])
            all_editor_stats = bq_data.get('editor_stats', [])
            invalid_dates = bq_data.get('invalid_dates', [])

            if invalid_dates:
                print(f"\n[周报] ⚠️ 以下日期数据校验失败: {invalid_dates}")

            if not all_opt_stats and not all_editor_stats:
                print(f"[周报] 未找到有效数据")
                return
        else:
            # 逐天抓取数据（包括周末）
            all_opt_stats = []
            all_editor_stats = []

            for day_str in date_list:
                print(f"\n--- 抓取 {day_str} ---")

                result = await run_with_stats(day_str, upload_bq=args.upload)
                all_opt_stats.extend(result.get('optimizer_stats', []))
                all_editor_stats.extend(result.get('editor_stats', []))

        # 生成周报汇总
        weekly = generate_weekly_summary(
            all_opt_stats, all_editor_stats,
            start_str, end_str
        )

        # 导出 Excel
        if args.excel:
            export_stats_to_excel(all_opt_stats, all_editor_stats, f"{start_str}_to_{end_str}")

        # 导出到飞书文档
        if getattr(args, 'lark_doc', False):
            doc_token = getattr(args, 'doc_token', None)
            export_stats_to_lark_doc(all_opt_stats, all_editor_stats, f"{start_str}_to_{end_str}", doc_token)

        # 保存周报
        output_file = f"xmp_weekly_{start_str}_to_{end_str}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(weekly, f, ensure_ascii=False, indent=2, default=str)
        print(f"周报已保存到: {output_file}")

    # 从 BigQuery 读取数据生成日报（不抓取）
    elif getattr(args, 'from_bq', False):
        print(f"[BQ] 从 BigQuery 读取 {date_str} 的数据...")
        bq_data = query_stats_from_bq(date_str)
        opt_stats = bq_data.get('optimizer_stats', [])
        editor_stats = bq_data.get('editor_stats', [])
        batch_id = bq_data.get('batch_id')
        batch_valid = bq_data.get('batch_valid', True)

        if not opt_stats and not editor_stats:
            print(f"[BQ] 未找到 {date_str} 的数据")
            return

        # 校验 batch 时效性
        if not batch_valid:
            print(f"[BQ] ⚠️ 数据时效性校验失败，跳过日报生成")
            print(f"[BQ]   batch_id={batch_id} 不是 {date_str} 之后抓取的")
            print(f"[BQ]   请检查凌晨2点的 T-1 数据采集任务是否正常执行")
            return

        # 导出到飞书文档
        if getattr(args, 'lark_doc', False):
            doc_token = getattr(args, 'doc_token', None)
            export_stats_to_lark_doc(opt_stats, editor_stats, date_str, doc_token)

        # 导出 Excel
        if args.excel:
            export_stats_to_excel(opt_stats, editor_stats, date_str)

        print(f"[完成] 日报生成完毕: {date_str}")

    elif args.stats or args.excel or getattr(args, 'lark_doc', False):
        result = await run_with_stats(date_str, upload_bq=args.upload)
        if args.excel and result.get('optimizer_stats'):
            export_stats_to_excel(
                result.get('optimizer_stats', []),
                result.get('editor_stats', []),
                date_str
            )
        # 导出到飞书文档
        if getattr(args, 'lark_doc', False) and result.get('optimizer_stats'):
            doc_token = getattr(args, 'doc_token', None)
            export_stats_to_lark_doc(
                result.get('optimizer_stats', []),
                result.get('editor_stats', []),
                date_str,
                doc_token
            )
        output_file = f"xmp_data_{date_str.replace('-', '')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"数据已保存到: {output_file}")

    else:
        result = await run_once(date_str, upload_bq=args.upload)
        output_file = f"xmp_data_{date_str.replace('-', '')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"数据已保存到: {output_file}")


if __name__ == '__main__':
    asyncio.run(main())


def test_material_api():
    """测试素材详情 API"""
    from datetime import datetime

    scraper = XMPEditorStatsScraper()

    # 测试1: 按日期查询
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"\n=== 测试1: 查询 {today} 的素材 ===")
    materials = scraper.fetch_material_details(
        start_date=today,
        end_date=today,
        page_size=10
    )

    if materials:
        print(f"获取到 {len(materials)} 个素材")
        for m in materials[:3]:
            designer = m.get('designer', [])
            name = designer[0].get('name') if designer else '未知'
            print(f"  - {m.get('material_name')[:30]}... | 剪辑师: {name}")

    # 测试2: 日产出统计
    print(f"\n=== 测试2: 剪辑师日产出统计 ===")
    output = scraper.fetch_daily_editor_output(today)
    print(f"总素材数: {output['total_materials']}")
    for editor in output['editors'][:5]:
        print(f"  - {editor['name']}: {editor['count']} 个素材")


def test_editor_performance():
    """测试剪辑师产出与质量报表"""
    from datetime import datetime, timedelta

    scraper = XMPEditorStatsScraper()

    # 获取最近7天的数据
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    print(f"\n=== 测试: 剪辑师产出与质量报表 ===")
    print(f"日期范围: {start_date} ~ {end_date}")

    result = scraper.fetch_editor_performance(
        start_date=start_date,
        end_date=end_date,
        hot_threshold=500.0,
        roas_threshold=0.45
    )

    if result['editors']:
        print(f"\n前5位剪辑师详情:")
        for e in result['editors'][:5]:
            print(f"  {e['name']}:")
            print(f"    素材数: {e['material_count']}")
            print(f"    消耗: ${e['total_cost']:,.2f}")
            print(f"    D0 ROAS: {e['d0_roas']*100:.1f}%")
            print(f"    爆款率: {e['hot_rate']*100:.1f}%")
            print(f"    Top素材: {e['top_material']}")


if __name__ == '__test__':
    test_material_api()
