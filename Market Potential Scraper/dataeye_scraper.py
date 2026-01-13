"""
DataEye 短剧市场潜力数据采集器
从 DataEye 平台抓取短剧广告投放数据
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from playwright.sync_api import sync_playwright, Page, Browser, TimeoutError as PlaywrightTimeout

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('dataeye_scraper.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class DataEyeScraper:
    """DataEye 短剧数据采集器"""

    def __init__(self, headless: bool = False, cookie_file: str = 'dataeye_cookies.json'):
        """
        初始化采集器

        Args:
            headless: 是否使用无头模式
            cookie_file: Cookie 保存文件路径
        """
        self.headless = headless
        self.cookie_file = cookie_file
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.playwright = None

        # DataEye 平台配置
        self.base_url = 'https://oversea-v2.dataeye.com'
        self.target_url = 'https://oversea-v2.dataeye.com/playlet/playlet-material'
        self.username = os.getenv('DATAEYE_USERNAME')
        self.password = os.getenv('DATAEYE_PASSWORD')

        # 剧目搜索配置 - 使用剧目名称搜索框而不是素材搜索框
        self.use_drama_name_search = True

        logger.info("初始化 DataEye 采集器")
        logger.info(f"目标地址: {self.target_url}")
        logger.info(f"无头模式: {headless}")

    def __enter__(self):
        """上下文管理器入口"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()

    def start(self):
        """启动浏览器"""
        logger.info("启动浏览器...")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=['--start-maximized']
        )
        context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        self.page = context.new_page()
        logger.info("浏览器启动成功")

    def close(self):
        """关闭浏览器"""
        if self.page:
            self.page.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        logger.info("浏览器已关闭")

    def load_cookies(self) -> bool:
        """加载保存的 Cookie"""
        if not os.path.exists(self.cookie_file):
            logger.info("Cookie 文件不存在，需要重新登录")
            return False

        try:
            with open(self.cookie_file, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            self.page.context.add_cookies(cookies)
            logger.info(f"成功加载 {len(cookies)} 个 Cookie")
            return True
        except Exception as e:
            logger.error(f"加载 Cookie 失败: {e}")
            return False

    def save_cookies(self):
        """保存当前 Cookie"""
        try:
            cookies = self.page.context.cookies()
            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, indent=2, ensure_ascii=False)
            logger.info(f"成功保存 {len(cookies)} 个 Cookie")
        except Exception as e:
            logger.error(f"保存 Cookie 失败: {e}")

    def login(self) -> bool:
        """登录 DataEye 平台"""
        logger.info("开始登录流程...")

        # 尝试加载 Cookie
        if self.load_cookies():
            logger.info("尝试使用 Cookie 登录...")
            self.page.goto(self.target_url, timeout=60000)
            time.sleep(3)

            # 检查是否成功登录
            if self._check_login_status():
                logger.info("✓ Cookie 登录成功")
                return True
            else:
                logger.warning("Cookie 已失效，需要重新登录")

        # Cookie 登录失败，使用账号密码登录
        if not self.username or not self.password:
            logger.error("未配置 DATAEYE_USERNAME 或 DATAEYE_PASSWORD")
            logger.error("请在 .env 文件中配置账号密码，或手动保存有效的 Cookie")
            return False

        logger.info("使用账号密码登录...")
        return self._login_with_credentials()

    def _check_login_status(self) -> bool:
        """检查是否已登录"""
        try:
            # 检查是否在登录页面
            if 'login' in self.page.url:
                return False

            # 检查页面是否有搜索框（登录后才有）
            search_input = self.page.query_selector('input[placeholder*="搜索"]')
            return search_input is not None
        except Exception as e:
            logger.error(f"检查登录状态失败: {e}")
            return False

    def _login_with_credentials(self) -> bool:
        """使用账号密码登录"""
        try:
            # 访问登录页面
            login_url = f"{self.base_url}/login"
            logger.info(f"访问登录页面: {login_url}")
            self.page.goto(login_url, timeout=60000)
            time.sleep(2)

            # 输入用户名
            logger.info("输入用户名...")
            username_input = self.page.wait_for_selector('input[type="text"]', timeout=10000)
            username_input.fill(self.username)
            time.sleep(1)

            # 输入密码
            logger.info("输入密码...")
            password_input = self.page.wait_for_selector('input[type="password"]', timeout=10000)
            password_input.fill(self.password)
            time.sleep(1)

            # 点击登录按钮
            logger.info("点击登录按钮...")
            login_button = self.page.wait_for_selector('button[type="submit"]', timeout=10000)
            login_button.click()

            # 等待登录完成
            logger.info("等待登录完成...")
            time.sleep(5)

            # 检查是否登录成功
            if self._check_login_status():
                logger.info("✓ 账号密码登录成功")
                self.save_cookies()
                return True
            else:
                logger.error("✗ 登录失败，请检查账号密码")
                return False

        except Exception as e:
            logger.error(f"登录过程出错: {e}")
            return False

    def search_drama(self, drama_name: str) -> Dict:
        """
        搜索短剧数据 - 分两次获取: 近2年数据 + 近30天数据

        Args:
            drama_name: 短剧名称

        Returns:
            包含搜索结果的字典
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"开始搜索短剧: {drama_name}")
        logger.info(f"{'='*60}")

        try:
            # 1. 访问目标页面
            logger.info("步骤 1: 访问短剧素材页面...")
            self.page.goto(self.target_url, timeout=60000)
            time.sleep(3)

            # 2. 点击"海外短剧版本"
            logger.info("步骤 2: 点击海外短剧版本...")
            if not self._click_overseas_version():
                logger.error("✗ 点击海外短剧版本失败")
                return {"success": False, "error": "无法点击海外短剧版本"}

            # 3. 输入搜索关键词
            logger.info("步骤 3: 输入搜索关键词...")
            if not self._input_search_keyword(drama_name):
                logger.error("✗ 输入搜索关键词失败")
                return {"success": False, "error": "无法输入搜索关键词"}

            # 4. 设置监听器并点击搜索
            logger.info("步骤 4: 设置监听器并点击搜索...")
            data_2y = self._capture_api_with_action(
                drama_name, "2年",
                lambda: self._click_search_button()
            )

            # 5. 点击"近30天"筛选
            logger.info("步骤 5: 点击近30天筛选...")
            if not self._click_time_filter_30d():
                logger.warning("✗ 点击近30天筛选失败，使用默认数据")
                data_30d = None
            else:
                # 6. 设置监听器并点击近30天
                logger.info("步骤 6: 获取近30天数据...")
                data_30d = self._capture_api_with_action(
                    drama_name, "30天",
                    lambda: time.sleep(0.5)  # 点击已完成，只需等待
                )

            # 8. 合并两次数据
            return self._merge_time_window_data(drama_name, data_2y, data_30d)

        except Exception as e:
            logger.error(f"搜索过程出错: {e}")
            return {"success": False, "error": str(e)}

    def _process_api_response(self, drama_name: str, api_data: Dict) -> Dict:
        """
        处理 searchCreative API 返回的数据，并按时间窗口聚合

        Args:
            drama_name: 短剧名称
            api_data: API 返回的 JSON 数据

        Returns:
            处理后的数据字典
        """
        try:
            logger.info("开始处理 API 响应数据...")

            # 提取基本信息
            status_code = api_data.get('statusCode', 0)
            msg = api_data.get('msg', '')

            if status_code != 200:
                logger.error(f"API 返回错误: {msg}")
                return {
                    "success": False,
                    "drama_name": drama_name,
                    "error": f"API 错误: {msg}"
                }

            # 提取分页信息
            page_info = api_data.get('page', {})
            total_records = page_info.get('totalRecords', 0)

            # 提取素材列表
            content = api_data.get('content', {})
            search_list = content.get('searchList', [])

            logger.info(f"✓ 找到 {len(search_list)} 条素材，总记录数: {total_records}")

            # 保存原始 API 数据
            screenshot_path = f"screenshots/api_{drama_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            os.makedirs('screenshots', exist_ok=True)
            with open(screenshot_path, 'w', encoding='utf-8') as f:
                json.dump(api_data, f, indent=2, ensure_ascii=False)
            logger.info(f"✓ 已保存原始 API 数据: {screenshot_path}")

            # 按时间窗口聚合数据
            aggregated_data = self._aggregate_by_time_windows(drama_name, search_list)

            return {
                "success": True,
                "drama_name": drama_name,
                "total_records": total_records,
                "materials": search_list,
                "page_info": page_info,
                "raw_api_file": screenshot_path,
                "aggregated": aggregated_data
            }

        except Exception as e:
            logger.error(f"处理 API 响应失败: {e}")
            return {
                "success": False,
                "drama_name": drama_name,
                "error": str(e)
            }

    def _aggregate_by_time_windows(self, drama_name: str, materials: List[Dict]) -> Dict:
        """
        按时间窗口聚合素材数据

        窗口 A: 近 30 天（最近热度）
        窗口 B: 近 2 年（生命周期）

        Args:
            drama_name: 短剧名称
            materials: 素材列表

        Returns:
            聚合后的数据
        """
        from datetime import datetime, timedelta

        logger.info(f"开始按时间窗口聚合数据...")

        # 计算时间窗口
        today = datetime.now()
        window_30d = today - timedelta(days=30)
        window_2y = today - timedelta(days=730)

        # 初始化聚合数据
        data_30d = {
            "materials": [],
            "total_exposure": 0,
            "total_downloads": 0,
            "creative_count": 0,
            "countries": set(),
            "platforms": set(),
            "publishers": set()
        }

        data_2y = {
            "materials": [],
            "total_exposure": 0,
            "total_downloads": 0,
            "creative_count": 0,
            "countries": set(),
            "platforms": set(),
            "publishers": set(),
            "first_seen": None,
            "last_seen": None,
            "active_days": set()
        }

        # 遍历素材进行聚合
        for material in materials:
            try:
                # 解析日期
                first_seen_str = material.get('firstSeen', '')
                last_seen_str = material.get('lastSeen', '')

                if not first_seen_str or not last_seen_str:
                    continue

                first_seen = datetime.strptime(first_seen_str, '%Y-%m-%d')
                last_seen = datetime.strptime(last_seen_str, '%Y-%m-%d')

                # 提取基本数据
                exposure = material.get('exposureNum', 0)
                downloads = material.get('downloadNum', 0)

                # 窗口 B: 近 2 年数据
                if first_seen >= window_2y or last_seen >= window_2y:
                    data_2y["materials"].append(material)
                    data_2y["total_exposure"] += exposure
                    data_2y["total_downloads"] += downloads
                    data_2y["creative_count"] += 1

                    # 更新首次/最后投放日期
                    if data_2y["first_seen"] is None or first_seen < data_2y["first_seen"]:
                        data_2y["first_seen"] = first_seen
                    if data_2y["last_seen"] is None or last_seen > data_2y["last_seen"]:
                        data_2y["last_seen"] = last_seen

                    # 统计活跃天数
                    release_days = material.get('releaseDay', 0)
                    for i in range(release_days):
                        day = first_seen + timedelta(days=i)
                        data_2y["active_days"].add(day.strftime('%Y-%m-%d'))

                    # 统计国家
                    countries = material.get('countries', [])
                    for country in countries:
                        data_2y["countries"].add(country.get('countryName', ''))

                    # 统计平台
                    media = material.get('media', {})
                    if media:
                        data_2y["platforms"].add(media.get('mediaName', ''))

                    # 统计制作方
                    publisher = material.get('publisher', {})
                    if publisher:
                        data_2y["publishers"].add(publisher.get('publisherName', ''))

                # 窗口 A: 近 30 天数据
                if last_seen >= window_30d:
                    data_30d["materials"].append(material)
                    data_30d["total_exposure"] += exposure
                    data_30d["total_downloads"] += downloads
                    data_30d["creative_count"] += 1

                    # 统计国家
                    countries = material.get('countries', [])
                    for country in countries:
                        data_30d["countries"].add(country.get('countryName', ''))

                    # 统计平台
                    media = material.get('media', {})
                    if media:
                        data_30d["platforms"].add(media.get('mediaName', ''))

                    # 统计制作方
                    publisher = material.get('publisher', {})
                    if publisher:
                        data_30d["publishers"].add(publisher.get('publisherName', ''))

            except Exception as e:
                logger.debug(f"处理素材失败: {e}")
                continue

        # 转换 set 为 list
        data_30d["countries"] = list(data_30d["countries"])
        data_30d["platforms"] = list(data_30d["platforms"])
        data_30d["publishers"] = list(data_30d["publishers"])

        data_2y["countries"] = list(data_2y["countries"])
        data_2y["platforms"] = list(data_2y["platforms"])
        data_2y["publishers"] = list(data_2y["publishers"])
        data_2y["active_days_count"] = len(data_2y["active_days"])
        data_2y["active_days"] = list(data_2y["active_days"])

        # 计算生命周期
        if data_2y["first_seen"] and data_2y["last_seen"]:
            lifecycle_days = (data_2y["last_seen"] - data_2y["first_seen"]).days
            data_2y["lifecycle_days"] = lifecycle_days
            data_2y["first_seen"] = data_2y["first_seen"].strftime('%Y-%m-%d')
            data_2y["last_seen"] = data_2y["last_seen"].strftime('%Y-%m-%d')

        logger.info(f"✓ 近30天: {data_30d['creative_count']} 条素材, 曝光 {data_30d['total_exposure']:,}")
        logger.info(f"✓ 近2年: {data_2y['creative_count']} 条素材, 曝光 {data_2y['total_exposure']:,}, 生命周期 {data_2y.get('lifecycle_days', 0)} 天")

        return {
            "window_30d": data_30d,
            "window_2y": data_2y
        }

    def _click_overseas_version(self) -> bool:
        """点击海外短剧版本"""
        try:
            # 尝试多种选择器查找"海外短剧版本"按钮
            selectors = [
                'text="海外短剧版本"',
                'button:has-text("海外短剧版本")',
                'div:has-text("海外短剧版本")',
                '[class*="tab"]:has-text("海外短剧版本")',
            ]

            for selector in selectors:
                try:
                    element = self.page.wait_for_selector(selector, timeout=5000)
                    if element:
                        element.click()
                        logger.info("✓ 成功点击海外短剧版本")
                        time.sleep(2)
                        return True
                except:
                    continue

            logger.warning("未找到海外短剧版本按钮，可能已经在该版本")
            return True

        except Exception as e:
            logger.error(f"点击海外短剧版本失败: {e}")
            return False

    def _input_search_keyword(self, keyword: str) -> bool:
        """输入搜索关键词 - 使用第二个搜索框（剧目名称搜索框）"""
        try:
            # 查找所有搜索输入框
            selectors = [
                'input[placeholder*="搜索"]',
                'input[placeholder*="search"]',
                'input[type="text"]',
            ]

            for selector in selectors:
                try:
                    # 查找所有匹配的输入框
                    all_inputs = self.page.query_selector_all(selector)
                    logger.info(f"使用选择器 '{selector}' 找到 {len(all_inputs)} 个输入框")

                    if len(all_inputs) >= 2:
                        # 使用第二个搜索框（索引为1）
                        search_input = all_inputs[1]

                        # 获取 placeholder 确认
                        placeholder = search_input.get_attribute('placeholder') or ''
                        logger.info(f"使用第二个输入框，placeholder: {placeholder}")

                        # 清空输入框
                        search_input.click()
                        time.sleep(0.3)

                        # 使用 fill 方法直接填充（更可靠）
                        search_input.fill(keyword)
                        time.sleep(0.5)

                        # 验证输入内容
                        input_value = search_input.input_value()
                        logger.info(f"✓ 输入框当前值: {input_value}")

                        # 如果 fill 失败，尝试 type 方法
                        if not input_value or input_value != keyword:
                            logger.warning(f"⚠️ fill 方法失败，尝试 type 方法...")
                            search_input.click()
                            time.sleep(0.2)
                            search_input.press('Control+A')
                            time.sleep(0.1)
                            search_input.type(keyword, delay=50)
                            time.sleep(0.5)

                            # 再次验证
                            input_value = search_input.input_value()
                            logger.info(f"✓ 重新验证输入框值: {input_value}")

                        if input_value and keyword in input_value:
                            logger.info(f"✓ 成功输入关键词: {keyword}")
                        else:
                            logger.warning(f"⚠️ 输入值不匹配! 期望: {keyword}, 实际: {input_value}")
                            # 即使不匹配也继续，因为可能是页面显示问题

                        # 等待搜索建议出现
                        time.sleep(1.5)

                        return True
                    elif len(all_inputs) == 1:
                        logger.warning(f"只找到1个输入框，尝试下一个选择器")
                        continue
                except Exception as e:
                    logger.debug(f"选择器 '{selector}' 失败: {e}")
                    continue

            logger.error("未找到第二个搜索输入框")
            return False

        except Exception as e:
            logger.error(f"输入搜索关键词失败: {e}")
            return False

    def _click_search_button(self) -> bool:
        """点击搜索按钮"""
        try:
            # 方式1: 尝试查找并点击搜索图标按钮
            icon_selectors = [
                'button[class*="search"]',
                'span[class*="icon-search"]',
                'i[class*="search"]',
                '.ant-btn:has([class*="search"])',
            ]

            for selector in icon_selectors:
                try:
                    button = self.page.wait_for_selector(selector, timeout=2000)
                    if button and button.is_visible():
                        logger.info(f"找到搜索按钮: {selector}")
                        button.click()
                        logger.info("✓ 成功点击搜索按钮")
                        time.sleep(2)  # 等待搜索执行
                        return True
                except:
                    continue

            # 方式2: 查找文本为"搜索"的按钮
            text_selectors = [
                'button:has-text("搜索")',
                'button:has-text("search")',
            ]

            for selector in text_selectors:
                try:
                    button = self.page.wait_for_selector(selector, timeout=2000)
                    if button:
                        button.click()
                        logger.info("✓ 成功点击搜索按钮（文本匹配）")
                        time.sleep(2)
                        return True
                except:
                    continue

            # 方式3: 按回车键触发搜索（最后的备选方案）
            logger.info("尝试按回车键触发搜索...")
            self.page.keyboard.press('Enter')
            logger.info("✓ 已按回车键")
            time.sleep(2)
            return True

        except Exception as e:
            logger.error(f"点击搜索按钮失败: {e}")
            return False

    def _capture_api_with_action(self, drama_name: str, time_window: str, action_func, enable_pagination: bool = True) -> Optional[Dict]:
        """
        使用 expect_response 捕获 API 响应，并自动翻页获取所有数据

        Args:
            drama_name: 短剧名称
            time_window: 时间窗口标识 (如 "2年", "30天")
            action_func: 触发 API 请求的动作函数
            enable_pagination: 是否启用自动翻页 (默认: True)

        Returns:
            API 响应的 JSON 数据（包含所有页面的数据），如果捕获失败返回 None
        """
        try:
            logger.info(f"✓ 准备捕获 {time_window} API 响应...")

            # 使用 expect_response 等待特定的 API 响应
            with self.page.expect_response(
                lambda response: 'searchCreative' in response.url and response.status == 200,
                timeout=30000  # 30秒超时
            ) as response_info:
                # 执行触发 API 的动作
                action_func()

            # 获取响应
            response = response_info.value
            logger.info(f"✓ 捕获到 API: {response.url}")

            # 保存第一页的请求参数供后续翻页使用
            try:
                request_body = response.request.post_data
                logger.debug(f"请求体类型: {type(request_body)}")

                if request_body:
                    logger.debug(f"请求体内容: {request_body}")

                    # 尝试解析 JSON
                    try:
                        self._last_search_params = json.loads(request_body)
                        logger.info(f"✓ 已保存搜索参数(JSON): {json.dumps(self._last_search_params, ensure_ascii=False)}")
                    except json.JSONDecodeError:
                        # 如果不是 JSON，尝试解析 URL 编码参数
                        logger.info("请求体不是 JSON，尝试解析 URL 编码参数...")
                        from urllib.parse import parse_qs
                        params_dict = {}
                        for pair in request_body.split('&'):
                            if '=' in pair:
                                key, value = pair.split('=', 1)
                                params_dict[key] = value
                        self._last_search_params = params_dict
                        logger.info(f"✓ 已保存搜索参数(URL编码): {json.dumps(self._last_search_params, ensure_ascii=False)}")
                else:
                    # 备用方案
                    logger.warning("⚠ 请求体为空，使用最小参数集...")
                    self._last_search_params = {
                        "pageNum": 1,
                        "pageSize": 40,
                        "keyword": drama_name,
                    }
                    logger.warning(f"⚠ 使用最小参数集: {json.dumps(self._last_search_params, ensure_ascii=False)}")

            except Exception as e:
                logger.error(f"✗ 保存搜索参数失败: {e}")
                logger.error(f"   原始数据: {request_body[:500] if request_body else 'None'}")
                self._last_search_params = {
                    "pageNum": 1,
                    "pageSize": 40,
                    "keyword": drama_name,
                }
                logger.warning(f"⚠ 使用最小参数集: {json.dumps(self._last_search_params, ensure_ascii=False)}")

            # 解析 JSON
            json_data = response.json()
            content = json_data.get('content', {})

            if isinstance(content, dict) and 'searchList' in content:
                # 如果启用翻页功能，自动获取所有页面数据
                if enable_pagination:
                    json_data = self._capture_all_pages(drama_name, time_window, json_data)
                else:
                    # 不启用翻页时，保存第一页数据
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filepath = f"screenshots/api_{drama_name}_{time_window}_{timestamp}.json"
                    os.makedirs('screenshots', exist_ok=True)

                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, indent=2, ensure_ascii=False)
                    logger.info(f"✓ 已保存: {filepath}")

                return json_data
            else:
                logger.warning(f"content 不符合要求")
                return None

        except Exception as e:
            logger.error(f"捕获失败: {e}")
            return None

    def _capture_all_pages(self, drama_name: str, time_window: str, first_page_data: Dict) -> Dict:
        """
        自动翻页获取所有数据

        Args:
            drama_name: 短剧名称
            time_window: 时间窗口标识 (如 "2年", "30天")
            first_page_data: 第一页的数据

        Returns:
            合并所有页面后的完整数据
        """
        try:
            # 提取分页信息
            page_info = first_page_data.get('page', {})
            total_records = page_info.get('totalRecords', 0)
            page_size = page_info.get('pageSize', 40)

            # 计算总页数
            total_pages = (total_records + page_size - 1) // page_size

            logger.info(f"📊 数据统计: 总记录={total_records}, 每页={page_size}, 总页数={total_pages}")

            # 如果只有一页，直接返回
            if total_pages <= 1:
                logger.info("✓ 只有一页数据，无需翻页")
                return first_page_data

            # 合并所有页面的数据
            all_materials = first_page_data.get('content', {}).get('searchList', [])
            logger.info(f"✓ 第 1 页: 获取 {len(all_materials)} 条记录")

            # 从第2页开始翻页
            for page_num in range(2, total_pages + 1):
                logger.info(f"📄 正在获取第 {page_num}/{total_pages} 页...")

                # 先尝试 API 方式
                page_data = self._fetch_page(page_num)

                # 如果 API 失败，尝试点击按钮翻页
                if not page_data:
                    logger.warning(f"⚠ API 翻页失败，尝试点击按钮翻页...")
                    if self._click_page_button(page_num):
                        # 等待页面加载
                        time.sleep(3)
                        # 从页面提取数据
                        page_data = self._extract_current_page_data()

                if page_data:
                    materials = page_data.get('content', {}).get('searchList', [])
                    all_materials.extend(materials)
                    logger.info(f"✓ 第 {page_num} 页: 获取 {len(materials)} 条记录 (累计: {len(all_materials)})")
                    time.sleep(1)
                else:
                    logger.warning(f"⚠ 第 {page_num} 页获取失败，跳过")

            # 更新第一页数据中的 searchList
            first_page_data['content']['searchList'] = all_materials

            # 保存完整数据
            filepath = f"screenshots/{drama_name}_{time_window}.json"
            os.makedirs('screenshots', exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(first_page_data, f, indent=4, ensure_ascii=False)

            logger.info(f"✓ 完整数据已保存: {filepath}")
            logger.info(f"✓ 总计获取 {len(all_materials)} 条记录")

            return first_page_data

        except Exception as e:
            logger.error(f"翻页过程出错: {e}")
            return first_page_data

    def _fetch_page(self, page_num: int) -> Optional[Dict]:
        """
        获取指定页码的数据（通过直接调用 API，使用保存的搜索参数）

        Args:
            page_num: 页码

        Returns:
            该页的 API 响应数据
        """
        try:
            logger.info(f"通过 API 直接获取第 {page_num} 页数据...")

            # 检查是否有保存的搜索参数
            if not hasattr(self, '_last_search_params') or not self._last_search_params:
                logger.error("没有保存的搜索参数，无法翻页")
                return None

            # 使用保存的搜索参数，只修改页码
            search_params = self._last_search_params.copy()

            # 根据参数格式决定如何修改页码
            if 'pageId' in search_params:
                search_params['pageId'] = str(page_num)
            else:
                search_params['pageNum'] = page_num

            search_params['pageSize'] = '40' if isinstance(list(search_params.values())[0], str) else 40

            # 构建请求体（支持 URL 编码和 JSON 两种格式）
            if all(isinstance(v, str) for v in search_params.values()):
                # URL 编码格式
                body_str = '&'.join([f"{k}={v}" for k, v in search_params.items()])
                content_type = 'application/x-www-form-urlencoded'
                logger.info(f"使用 URL 编码格式: {body_str[:100]}...")
            else:
                # JSON 格式
                body_str = json.dumps(search_params)
                content_type = 'application/json'
                logger.info(f"使用 JSON 格式: {body_str[:100]}...")

            # 使用 page.evaluate 直接调用 API
            js_fetch_page = f"""
            async () => {{
                try {{
                    const response = await fetch('https://oversea-v2.dataeye.com/api/playlet/creative/searchCreative', {{
                        method: 'POST',
                        headers: {{
                            'Content-Type': '{content_type}',
                        }},
                        body: `{body_str}`
                    }});

                    if (response.ok) {{
                        return await response.json();
                    }}
                    return null;
                }} catch (e) {{
                    console.error('API 调用失败:', e);
                    return null;
                }}
            }}
            """

            json_data = self.page.evaluate(js_fetch_page)

            if json_data:
                logger.info(f"✓ 成功获取第 {page_num} 页数据")
                return json_data
            else:
                logger.warning(f"⚠ 第 {page_num} 页数据为空")
                return None

        except Exception as e:
            logger.error(f"获取第 {page_num} 页失败: {e}")
            return None

    def _click_next_page_ui(self, target_page: int) -> bool:
        """
        通过点击页面按钮翻页（备用方案）

        Args:
            target_page: 目标页码

        Returns:
            是否点击成功
        """
        try:
            logger.info(f"尝试点击翻到第 {target_page} 页...")

            # 等待分页元素加载
            time.sleep(1)

            # 先尝试直接点击页码
            page_num_selectors = [
                f'li[title="{target_page}"]',
                f'.ant-pagination-item-{target_page}',
                f'[class*="pagination"] li:has-text("{target_page}")',
                f'li:has-text("{target_page}")',
            ]

            for selector in page_num_selectors:
                try:
                    page_btn = self.page.query_selector(selector)
                    if page_btn and page_btn.is_visible():
                        logger.info(f"✓ 找到页码按钮: {selector}")
                        page_btn.click()
                        logger.info(f"✓ 点击页码 {target_page}")
                        time.sleep(2)
                        return True
                except:
                    continue

            logger.warning(f"未找到页码 {target_page} 的按钮")
            return False

        except Exception as e:
            logger.error(f"点击翻页失败: {e}")
            return False

    def _is_last_page_ui(self) -> bool:
        """
        检查是否到达最后一页

        Returns:
            是否是最后一页
        """
        try:
            # 检查下一页按钮是否被禁用
            last_page_indicators = [
                '.ant-pagination-next.ant-pagination-disabled',
                '[class*="pagination"] [class*="next"][disabled]',
                '[class*="pagination"] [class*="next"][class*="disabled"]',
            ]

            for selector in last_page_indicators:
                try:
                    elem = self.page.query_selector(selector)
                    if elem and elem.is_visible():
                        logger.info(f"检测到最后一页标识: {selector}")
                        return True
                except:
                    continue

            return False

        except Exception as e:
            logger.error(f"检查最后一页失败: {e}")
            return False

    def _click_time_filter_30d(self) -> bool:
        """
        合并两个时间窗口的数据

        Args:
            drama_name: 短剧名称
            data_2y: 近2年的 API 数据
            data_30d: 近30天的 API 数据

        Returns:
            合并后的数据字典
        """
        try:
            logger.info("开始合并两个时间窗口的数据...")

            # 如果两年数据为空，返回失败
            if not data_2y:
                logger.error("近2年数据为空，无法继续处理")
                return {
                    "success": False,
                    "drama_name": drama_name,
                    "error": "近2年数据为空"
                }

            # 处理近2年数据
            result_2y = self._process_api_response(drama_name, data_2y)
            if not result_2y.get("success"):
                return result_2y

            # 如果30天数据为空，只返回2年数据
            if not data_30d:
                logger.warning("近30天数据为空，仅使用近2年数据")
                return result_2y

            # 处理近30天数据
            result_30d = self._process_api_response(drama_name, data_30d)

            # 合并结果
            merged_result = {
                "success": True,
                "drama_name": drama_name,
                "window_2y": result_2y.get("window_2y", {}),
                "window_30d": result_30d.get("window_30d", {}) if result_30d.get("success") else result_2y.get("window_30d", {}),
                "total_records_2y": result_2y.get("total_records", 0),
                "total_records_30d": result_30d.get("total_records", 0) if result_30d.get("success") else 0,
            }

            logger.info("✓ 数据合并完成")
            logger.info(f"  - 近2年: {merged_result['window_2y'].get('creative_count', 0)} 条素材")
            logger.info(f"  - 近30天: {merged_result['window_30d'].get('creative_count', 0)} 条素材")

            return merged_result

        except Exception as e:
            logger.error(f"合并数据失败: {e}")
            return {
                "success": False,
                "drama_name": drama_name,
                "error": f"合并数据失败: {str(e)}"
            }

    def _extract_search_results(self, drama_name: str) -> Dict:
        """提取搜索结果数据 - 包含所有维度"""
        try:
            # 等待数据加载
            time.sleep(3)

            # 截图保存当前页面
            screenshot_path = f"screenshots/search_{drama_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            os.makedirs('screenshots', exist_ok=True)
            self.page.screenshot(path=screenshot_path)
            logger.info(f"已保存截图: {screenshot_path}")

            # 检查是否有搜索结果
            no_data_selectors = [
                'text="暂无数据"',
                'text="No data"',
                '.empty-data',
            ]

            for selector in no_data_selectors:
                try:
                    if self.page.query_selector(selector):
                        logger.warning("搜索结果为空")
                        return {
                            "success": True,
                            "drama_name": drama_name,
                            "results": [],
                            "message": "暂无数据"
                        }
                except:
                    continue

            # 提取表格数据
            results = []
            table_rows = self.page.query_selector_all('table tbody tr')

            if table_rows:
                logger.info(f"找到 {len(table_rows)} 行表格数据")
                for row in table_rows:
                    row_data = self._extract_detailed_row_data(row)
                    if row_data:
                        results.append(row_data)
            else:
                logger.warning("未找到表格数据，尝试提取页面所有文本")
                # 提取整个页面的文本内容作为备份
                page_text = self.page.inner_text('body')
                results.append({
                    "type": "page_text",
                    "content": page_text[:5000]  # 限制长度
                })

            return {
                "success": True,
                "drama_name": drama_name,
                "results": results,
                "total_count": len(results),
                "screenshot": screenshot_path
            }

        except Exception as e:
            logger.error(f"提取搜索结果失败: {e}")
            return {
                "success": False,
                "drama_name": drama_name,
                "error": str(e)
            }

    def _extract_detailed_row_data(self, row) -> Optional[Dict]:
        """从表格行提取详细数据 - 包含所有维度"""
        try:
            cells = row.query_selector_all('td')
            if not cells:
                return None

            # 提取所有单元格的文本
            cell_texts = [cell.inner_text().strip() for cell in cells]

            if not cell_texts:
                return None

            # 构建详细的数据结构
            row_data = {
                "raw_data": cell_texts,
                "cell_count": len(cell_texts)
            }

            # 尝试解析具体字段（根据表格列顺序）
            # 注意：实际列顺序需要根据页面调整
            try:
                if len(cell_texts) >= 2:
                    row_data["drama_name"] = cell_texts[0] if cell_texts[0] else None
                    row_data["publisher"] = cell_texts[1] if len(cell_texts) > 1 else None

                # 提取数值型数据
                for i, text in enumerate(cell_texts):
                    # 检测是否包含数字（曝光量、素材数等）
                    if any(char.isdigit() for char in text):
                        row_data[f"field_{i}"] = text

            except Exception as parse_error:
                logger.debug(f"解析字段失败: {parse_error}")

            return row_data

        except Exception as e:
            logger.debug(f"提取行数据失败: {e}")
            return None

    def _extract_row_data(self, row) -> Optional[Dict]:
        """从表格行提取数据（旧方法，保留兼容性）"""
        try:
            cells = row.query_selector_all('td')
            if not cells:
                return None

            # 提取所有单元格的文本
            cell_texts = [cell.inner_text().strip() for cell in cells]

            # 返回原始数据，后续可以根据实际表格结构调整
            return {
                "raw_data": cell_texts,
                "cell_count": len(cell_texts)
            }

        except Exception as e:
            logger.debug(f"提取行数据失败: {e}")
            return None

    def _extract_card_data(self, card) -> Optional[Dict]:
        """从卡片提取数据"""
        try:
            # 提取卡片内的所有文本
            text = card.inner_text().strip()
            if not text:
                return None

            return {
                "raw_text": text
            }

        except Exception as e:
            logger.debug(f"提取卡片数据失败: {e}")
            return None

    def scrape_multiple_dramas(self, drama_list: List[str]) -> List[Dict]:
        """
        批量搜索多个短剧

        Args:
            drama_list: 短剧名称列表

        Returns:
            所有短剧的搜索结果列表
        """
        all_results = []

        for i, drama_name in enumerate(drama_list, 1):
            logger.info(f"\n进度: {i}/{len(drama_list)}")
            result = self.search_drama(drama_name)
            all_results.append(result)

            # 每次搜索后等待一段时间，避免请求过快
            if i < len(drama_list):
                wait_time = 3
                logger.info(f"等待 {wait_time} 秒后继续...")
                time.sleep(wait_time)

        return all_results

    def _click_page_button(self, page_num: int) -> bool:
        """
        点击页面上的页码按钮

        Args:
            page_num: 目标页码

        Returns:
            是否点击成功
        """
        try:
            logger.info(f"尝试点击页码 {page_num}...")

            # 尝试多种选择器
            selectors = [
                f'li[title="{page_num}"]',
                f'[class*="pagination-item-{page_num}"]',
                f'li:has-text("{page_num}")',
            ]

            for selector in selectors:
                try:
                    btn = self.page.query_selector(selector)
                    if btn and btn.is_visible():
                        btn.click()
                        logger.info(f"✓ 成功点击页码 {page_num}")
                        return True
                except:
                    continue

            logger.warning(f"未找到页码 {page_num} 的按钮")
            return False

        except Exception as e:
            logger.error(f"点击页码失败: {e}")
            return False

    def _extract_current_page_data(self) -> Optional[Dict]:
        """
        从当前页面提取数据（通过拦截 API）

        Returns:
            当前页的数据
        """
        try:
            # 等待 API 响应
            with self.page.expect_response(
                lambda r: 'searchCreative' in r.url,
                timeout=10000
            ) as response_info:
                pass

            response = response_info.value
            if response.status == 200:
                return response.json()

            return None

        except Exception as e:
            logger.error(f"提取页面数据失败: {e}")
            return None


def main():
    """主函数 - 测试爬虫"""
    logger.info("="*60)
    logger.info("DataEye 短剧数据采集器")
    logger.info("="*60)

    # 测试剧集列表 - 用户指定的10部短剧
    test_dramas = [
        "天降萌宝老祖，孝子贤孙都跪下",
        "离婚！本小姐爱的起放得下",
        "穿过荆棘拥抱你",
        "他不渡我",
        "此情唯你可消",
        "我是元婴期！四个姐姐瞧不起我",
        "带崽嫁入豪门",
        "蜜桃乌龙",
        "重生后整顿前夫全家",
        "断手医圣",
    ]

    try:
        with DataEyeScraper(headless=False) as scraper:
            # 登录
            logger.info("\n步骤 1: 登录 DataEye 平台")
            if not scraper.login():
                logger.error("登录失败，退出程序")
                return

            # 搜索短剧
            logger.info("\n步骤 2: 开始搜索短剧数据")
            results = scraper.scrape_multiple_dramas(test_dramas)

            # 保存结果
            output_file = f"dataeye_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)

            logger.info(f"\n✓ 数据已保存到: {output_file}")
            logger.info(f"✓ 共搜索 {len(results)} 个短剧")

    except KeyboardInterrupt:
        logger.info("\n用户中断程序")
    except Exception as e:
        logger.error(f"\n程序出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
