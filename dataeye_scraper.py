"""
DataEye Playlet Material Scraper
从 https://oversea-v2.dataeye.com/playlet/playlet-material 抓取素材数据和视频
"""

import os
import sys
import json
import time
import re
import requests
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright, Page, Browser
from dotenv import load_dotenv
from urllib.parse import urlparse, urljoin

# 设置控制台编码为 UTF-8，避免 Windows 下的编码问题
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')


class DataEyeScraper:
    """DataEye 素材数据抓取器"""

    # 版本配置
    VERSION_CONFIG = {
        'overseas': {
            'name': '海外短剧版',
            'base_url': 'https://oversea-v2.dataeye.com',
            'target_url': 'https://oversea-v2.dataeye.com/playlet/playlet-material',
            'cookie_file': 'dataeye_cookies.json',
            'gcs_prefix': 'adx',
        },
        'china': {
            'name': '国内行业版',
            'base_url': 'https://adxray-app.dataeye.com',
            'target_url': 'https://adxray-app.dataeye.com/creative/material',
            'cookie_file': 'dataeye_cn_cookies.json',
            'gcs_prefix': 'adx_cn',
        }
    }

    def __init__(self, username: str, password: str, headless: bool = False, cookie_file: str = None,
                 max_records: int = None, date_filter: str = None, version: str = 'overseas'):
        """
        初始化抓取器

        Args:
            username: 登录用户名/邮箱
            password: 登录密码
            headless: 是否使用无头模式
            cookie_file: Cookie 保存文件路径
            max_records: 最大获取记录数
            date_filter: 日期过滤 ('today', 'yesterday', 或 'YYYY-MM-DD')
            version: 版本 ('overseas' 海外版, 'china' 国内版)
        """
        self.username = username
        self.password = password
        self.headless = headless
        self.max_records = max_records
        self.date_filter = date_filter
        self.browser: Browser = None
        self.page: Page = None
        self.captured_responses = []
        self.video_urls = []

        # 版本配置
        self.version = version
        if version not in self.VERSION_CONFIG:
            raise ValueError(f"不支持的版本: {version}，可选: {list(self.VERSION_CONFIG.keys())}")

        config = self.VERSION_CONFIG[version]
        self.version_name = config['name']
        self.base_url = config['base_url']
        self.target_url = config['target_url']
        self.cookie_file = cookie_file or config['cookie_file']
        self.gcs_prefix = config['gcs_prefix']

        print(f"初始化 DataEye 爬虫 [{self.version_name}]")
        print(f"  目标 URL: {self.target_url}")

    def start(self):
        """启动浏览器"""
        print("正在启动浏览器...")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        # 创建带有更大视口的页面
        self.page = self.browser.new_page(viewport={'width': 1920, 'height': 1080})
        print("浏览器启动成功")

    def stop(self):
        """关闭浏览器"""
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        print("浏览器已关闭")

    def _get_gcs_client(self):
        """获取 GCS 客户端"""
        try:
            from google.cloud import storage
            return storage.Client()
        except ImportError:
            return None
        except Exception:
            return None

    def _upload_screenshot_to_gcs(self, local_path, gcs_path=None, batch_id=None):
        """上传截图到 GCS"""
        try:
            gcs_bucket = os.getenv('GCS_BUCKET_NAME')
            if not gcs_bucket:
                return False

            client = self._get_gcs_client()
            if not client:
                return False

            if not os.path.exists(local_path):
                return False

            bucket = client.bucket(gcs_bucket)
            if gcs_path is None:
                # 如果有批次 ID，放到批次目录下
                if batch_id:
                    gcs_path = f'adx/batch_{batch_id}/screenshots/{os.path.basename(local_path)}'
                else:
                    gcs_path = f'adx/screenshots/{os.path.basename(local_path)}'
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            print(f"  ✓ 截图已上传到 GCS: gs://{gcs_bucket}/{gcs_path}")
            return True
        except Exception as e:
            print(f"  ⚠ 上传截图到 GCS 失败: {e}")
            return False

    def _save_cookies_to_gcs(self, cookies):
        """保存 Cookie 到 GCS"""
        try:
            gcs_bucket = os.getenv('GCS_BUCKET_NAME')
            if not gcs_bucket:
                return False

            client = self._get_gcs_client()
            if not client:
                return False

            bucket = client.bucket(gcs_bucket)
            blob = bucket.blob('adx/cookies/dataeye_cookies.json')
            blob.upload_from_string(
                json.dumps(cookies, ensure_ascii=False, indent=2),
                content_type='application/json'
            )
            print(f"✓ Cookie 已保存到 GCS: gs://{gcs_bucket}/adx/cookies/dataeye_cookies.json")
            return True
        except Exception as e:
            print(f"⚠ 保存 Cookie 到 GCS 失败: {e}")
            return False

    def _load_cookies_from_gcs(self):
        """从 GCS 加载 Cookie"""
        try:
            gcs_bucket = os.getenv('GCS_BUCKET_NAME')
            if not gcs_bucket:
                return None

            client = self._get_gcs_client()
            if not client:
                return None

            bucket = client.bucket(gcs_bucket)
            blob = bucket.blob('adx/cookies/dataeye_cookies.json')

            if not blob.exists():
                print(f"GCS 上不存在 Cookie 文件")
                return None

            content = blob.download_as_string()
            cookies = json.loads(content)
            print(f"✓ 从 GCS 加载了 {len(cookies)} 个 Cookie")
            return cookies
        except Exception as e:
            print(f"⚠ 从 GCS 加载 Cookie 失败: {e}")
            return None

    def save_cookies(self):
        """保存当前页面的 Cookie 到文件和 GCS"""
        try:
            cookies = self.page.context.cookies()
            # 保存到本地文件
            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)
            print(f"✓ Cookie 已保存到 {self.cookie_file}")

            # 同时保存到 GCS（如果在 Cloud Run 环境）
            if os.getenv('CLOUD_RUN') or os.getenv('GCS_BUCKET_NAME'):
                self._save_cookies_to_gcs(cookies)

            return True
        except Exception as e:
            print(f"✗ 保存 Cookie 失败: {e}")
            return False

    def load_cookies(self):
        """从文件或 GCS 加载 Cookie"""
        cookies = None

        # 优先从本地文件加载
        try:
            if os.path.exists(self.cookie_file):
                with open(self.cookie_file, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                if cookies:
                    print(f"✓ 从本地文件加载了 {len(cookies)} 个 Cookie")
        except Exception as e:
            print(f"⚠ 从本地加载 Cookie 失败: {e}")

        # 如果本地没有，尝试从 GCS 加载
        if not cookies:
            cookies = self._load_cookies_from_gcs()

        if not cookies:
            print("未找到可用的 Cookie")
            return False

        try:
            # 添加 Cookie 到浏览器上下文
            self.page.context.add_cookies(cookies)
            print(f"✓ 已加载 {len(cookies)} 个 Cookie")
            return True
        except Exception as e:
            print(f"✗ 加载 Cookie 失败: {e}")
            return False

    def is_logged_in(self):
        """检查是否已登录"""
        try:
            current_url = self.page.url
            # 如果不在登录页面，说明已登录
            if "login" not in current_url.lower() and "signin" not in current_url.lower():
                # 额外检查：尝试访问目标页面
                if "playlet-material" in current_url:
                    return True
            return False
        except:
            return False

    def login_with_cookies(self):
        """尝试使用 Cookie 登录"""
        print("尝试使用已保存的 Cookie 登录...")

        # 先加载 Cookie
        if not self.load_cookies():
            return False

        # 访问目标页面
        print(f"正在访问: {self.target_url}")
        self.page.goto(self.target_url, timeout=60000)

        try:
            self.page.wait_for_load_state("networkidle", timeout=30000)
        except:
            pass
        time.sleep(3)

        # 检查是否登录成功
        current_url = self.page.url
        print(f"当前 URL: {current_url}")

        # 严格检查：必须到达目标页面，而不仅仅是"不在登录页"
        login_success = self._verify_login_success(current_url)

        if login_success:
            print("✓ Cookie 登录成功！")
            return True
        else:
            print("✗ Cookie 已过期或无效，需要重新登录")
            return False

    def _verify_login_success(self, current_url: str) -> bool:
        """验证是否真正登录成功"""
        current_url_lower = current_url.lower()

        # 1. 如果在登录页面，肯定没登录成功
        if "login" in current_url_lower or "signin" in current_url_lower:
            print("  检测到登录页面 URL")
            return False

        # 2. 检查是否到达了目标页面
        if self.version == 'overseas':
            # 海外版：检查是否在 playlet-material 页面
            if "playlet-material" in current_url_lower or "playlet/playlet-material" in current_url_lower:
                return self._check_page_has_content()
        elif self.version == 'china':
            # 国内版：检查是否在 creative/material 页面
            if "creative/material" in current_url_lower or "adxray-app" in current_url_lower:
                return self._check_page_has_content()

        # 3. 检查页面是否有登录弹窗或提示
        if self._has_login_prompt():
            print("  检测到登录提示弹窗")
            return False

        # 4. 如果 URL 包含目标域名但不在目标页面，可能被重定向了
        print(f"  URL 不匹配目标页面，当前: {current_url}")
        return False

    def _check_page_has_content(self) -> bool:
        """检查页面是否有实际内容（素材列表）"""
        try:
            # 检查是否有素材卡片或列表元素
            content_selectors = [
                '[class*="material"]',
                '[class*="card"]',
                '[class*="list-item"]',
                '[class*="creative"]',
                '.ant-table',
                '.el-table',
            ]
            for selector in content_selectors:
                elements = self.page.query_selector_all(selector)
                if len(elements) > 0:
                    print(f"  页面有内容 (找到 {len(elements)} 个 {selector} 元素)")
                    return True

            # 如果没找到内容元素，等待一下再检查
            time.sleep(2)
            for selector in content_selectors:
                elements = self.page.query_selector_all(selector)
                if len(elements) > 0:
                    return True

            print("  页面没有找到内容元素")
            return False
        except Exception as e:
            print(f"  检查页面内容失败: {e}")
            return False

    def _has_login_prompt(self) -> bool:
        """检查页面是否有登录提示"""
        try:
            login_prompt_selectors = [
                'text=请登录',
                'text=请先登录',
                'text=登录后查看',
                'text=Please login',
                'text=Sign in',
                '[class*="login-modal"]',
                '[class*="login-dialog"]',
            ]
            for selector in login_prompt_selectors:
                try:
                    elem = self.page.query_selector(selector)
                    if elem and elem.is_visible():
                        return True
                except:
                    continue
            return False
        except:
            return False

    def login(self):
        """登录 DataEye 平台（优先使用 Cookie）"""
        print(f"正在登录 DataEye 平台...")

        # 优先尝试 Cookie 登录（本地文件或 GCS）
        # 在 Cloud Run 环境中，cookie 文件在 GCS 上
        has_local_cookie = os.path.exists(self.cookie_file)
        has_gcs_cookie = bool(os.getenv('GCS_BUCKET_NAME'))

        if has_local_cookie or has_gcs_cookie:
            print(f"尝试 Cookie 登录 (本地: {has_local_cookie}, GCS: {has_gcs_cookie})")
            if self.login_with_cookies():
                return True
            print("Cookie 登录失败，尝试账号密码登录...")

        # 访问目标页面（会自动跳转到登录页）
        print(f"正在访问: {self.target_url}")
        self.page.goto(self.target_url, timeout=60000)

        # 等待页面加载
        print("等待页面加载...")
        try:
            self.page.wait_for_load_state("networkidle", timeout=60000)
        except:
            print("页面加载超时，继续尝试...")
        time.sleep(3)

        # 保存截图用于调试
        self.page.screenshot(path="dataeye_before_login.png")
        print("已保存截图到 dataeye_before_login.png")

        current_url = self.page.url
        print(f"当前 URL: {current_url}")

        # 检查是否需要登录
        if "login" in current_url.lower() or "signin" in current_url.lower():
            print("检测到登录页面，开始填写登录信息...")
            return self._do_login()
        elif "playlet-material" in current_url:
            print("已经登录，无需重复登录")
            return True
        else:
            print("尝试查找登录表单...")
            return self._do_login()

    def _do_login(self):
        """执行登录操作"""
        try:
            # 等待登录表单加载
            time.sleep(2)

            # 检查是否有验证码
            captcha_handled = self._handle_captcha_if_present()

            # 尝试常见的登录表单选择器
            username_selectors = [
                'input[name="accountId"]',  # DataEye 特定
                'input[type="text"]',
                'input[type="email"]',
                'input[name="username"]',
                'input[name="email"]',
                'input[placeholder*="邮箱"]',
                'input[placeholder*="用户名"]',
                'input[placeholder*="email"]',
                'input[placeholder*="Email"]',
                'input[placeholder*="账号"]',
                '#username',
                '#email',
            ]

            password_selectors = [
                'input[type="password"]',
                'input[name="password"]',
                'input[placeholder*="密码"]',
                'input[placeholder*="password"]',
                '#password',
            ]

            # 尝试找到用户名输入框
            username_filled = False
            for selector in username_selectors:
                try:
                    elements = self.page.query_selector_all(selector)
                    for element in elements:
                        if element.is_visible():
                            element.fill(self.username)
                            print(f"✓ 找到用户名输入框: {selector}")
                            username_filled = True
                            break
                    if username_filled:
                        break
                except Exception as e:
                    continue

            if not username_filled:
                print("✗ 未能找到用户名输入框！")
                self._debug_page_inputs()
                return False

            # 尝试找到密码输入框
            password_filled = False
            for selector in password_selectors:
                try:
                    elements = self.page.query_selector_all(selector)
                    for element in elements:
                        if element.is_visible():
                            element.fill(self.password)
                            print(f"✓ 找到密码输入框: {selector}")
                            password_filled = True
                            break
                    if password_filled:
                        break
                except Exception as e:
                    continue

            if not password_filled:
                print("✗ 未能找到密码输入框！")
                self._debug_page_inputs()
                return False

            # 点击登录按钮
            login_button_selectors = [
                'button[type="submit"]',
                'button:has-text("登录")',
                'button:has-text("Login")',
                'button:has-text("Sign in")',
                'button:has-text("Log in")',
                'input[type="submit"]',
                '.login-btn',
                '.login-button',
                '#login-button',
                'button.ant-btn-primary',
                'button.el-button--primary',
            ]

            login_clicked = False
            for selector in login_button_selectors:
                try:
                    btn = self.page.query_selector(selector)
                    if btn and btn.is_visible():
                        btn.click()
                        print(f"✓ 点击登录按钮: {selector}")
                        login_clicked = True
                        break
                except Exception as e:
                    continue

            if not login_clicked:
                print("✗ 未能找到登录按钮！")
                self._debug_page_buttons()
                return False

            # 等待登录完成
            print("等待登录完成...")
            time.sleep(5)

            try:
                self.page.wait_for_load_state("networkidle", timeout=30000)
            except:
                pass

            # 保存登录后截图
            self.page.screenshot(path="dataeye_after_login.png")
            print("已保存登录后截图到 dataeye_after_login.png")

            current_url = self.page.url
            print(f"登录后 URL: {current_url}")

            # 检查是否登录成功
            if "login" not in current_url.lower() and "signin" not in current_url.lower():
                print("✓ 登录成功！")

                # 保存 Cookie 供下次使用
                self.save_cookies()

                # 导航到目标页面
                if "playlet-material" not in current_url:
                    print(f"正在跳转到目标页面: {self.target_url}")
                    self.page.goto(self.target_url, timeout=60000)
                    try:
                        self.page.wait_for_load_state("networkidle", timeout=60000)
                    except:
                        pass
                    time.sleep(3)

                return True
            else:
                # 登录失败，等待用户手动完成
                return self._wait_for_manual_login()

        except Exception as e:
            print(f"登录过程出错: {str(e)}")
            self.page.screenshot(path="dataeye_login_error.png")
            print("已保存错误截图到 dataeye_login_error.png")
            return False

    def _debug_page_inputs(self):
        """调试：打印页面上所有 input 元素"""
        try:
            inputs = self.page.query_selector_all('input')
            print(f"页面上找到 {len(inputs)} 个 input 元素:")
            for inp in inputs:
                inp_type = inp.get_attribute('type') or 'unknown'
                inp_name = inp.get_attribute('name') or ''
                inp_placeholder = inp.get_attribute('placeholder') or ''
                inp_id = inp.get_attribute('id') or ''
                print(f"  - type={inp_type}, name={inp_name}, id={inp_id}, placeholder={inp_placeholder}")
        except Exception as e:
            print(f"调试输入框失败: {e}")

    def _debug_page_buttons(self):
        """调试：打印页面上所有 button 元素"""
        try:
            buttons = self.page.query_selector_all('button')
            print(f"页面上找到 {len(buttons)} 个 button 元素:")
            for btn in buttons:
                btn_text = btn.inner_text().strip()[:50] if btn.inner_text() else ''
                btn_class = btn.get_attribute('class') or ''
                btn_type = btn.get_attribute('type') or ''
                print(f"  - text={btn_text}, type={btn_type}, class={btn_class[:50]}")
        except Exception as e:
            print(f"调试按钮失败: {e}")

    def _handle_captcha_if_present(self):
        """检测并处理验证码"""
        # 常见的验证码元素选择器
        captcha_selectors = [
            'input[placeholder*="验证码"]',
            'input[placeholder*="captcha"]',
            'input[placeholder*="Captcha"]',
            'input[name*="captcha"]',
            'input[name*="verifyCode"]',
            'input[name*="code"]',
            'input[id*="captcha"]',
            'input[id*="verifyCode"]',
            '.captcha-input',
            '#captcha',
            # 图形验证码图片
            'img[src*="captcha"]',
            'img[src*="verify"]',
            'img[src*="code"]',
            '.captcha-img',
            '#captchaImg',
            # 滑块验证码
            '.slider-captcha',
            '.slide-verify',
            '.geetest_holder',
            '#geetest',
            '.nc-container',  # 阿里云滑块
            '.verify-wrap',
        ]

        captcha_found = False
        captcha_type = None

        for selector in captcha_selectors:
            try:
                element = self.page.query_selector(selector)
                if element and element.is_visible():
                    captcha_found = True
                    if 'slider' in selector or 'slide' in selector or 'geetest' in selector or 'nc-' in selector:
                        captcha_type = 'slider'
                    elif 'img' in selector:
                        captcha_type = 'image'
                    else:
                        captcha_type = 'text'
                    print(f"✓ 检测到验证码元素: {selector} (类型: {captcha_type})")
                    break
            except:
                continue

        if captcha_found:
            return self._wait_for_manual_captcha(captcha_type)

        return True

    def _wait_for_manual_login(self):
        """等待用户手动完成登录"""
        print("\n" + "=" * 60)
        print("⚠️  自动登录未成功，请在浏览器中手动完成登录")
        print("=" * 60)
        print("请在浏览器窗口中：")
        print("  1. 输入用户名和密码（如果还没输入）")
        print("  2. 完成验证码（如果有）")
        print("  3. 点击登录按钮")
        print("  4. 等待页面跳转到目标页面")
        print("\n完成登录后，按 Enter 键继续...")
        print("=" * 60 + "\n")

        try:
            input(">>> 按 Enter 键继续...")
        except EOFError:
            print("非交互模式，等待 120 秒...")
            time.sleep(120)

        # 检查登录状态
        time.sleep(2)
        current_url = self.page.url
        print(f"当前 URL: {current_url}")

        if "login" not in current_url.lower() and "signin" not in current_url.lower():
            print("✓ 手动登录成功！")
            self.save_cookies()

            # 导航到目标页面
            if "playlet-material" not in current_url:
                print(f"正在跳转到目标页面: {self.target_url}")
                self.page.goto(self.target_url, timeout=60000)
                try:
                    self.page.wait_for_load_state("networkidle", timeout=60000)
                except:
                    pass
                time.sleep(3)

            return True
        else:
            print("✗ 仍未登录成功，请重试")
            return False

    def _wait_for_manual_captcha(self, captcha_type='unknown'):
        """等待用户手动完成验证码"""
        print("\n" + "=" * 60)
        print("⚠️  检测到验证码！")
        print("=" * 60)

        if captcha_type == 'slider':
            print("验证码类型: 滑块验证码")
            print("请在浏览器中手动完成滑块验证...")
        elif captcha_type == 'image':
            print("验证码类型: 图形验证码")
            print("请在浏览器中查看验证码图片并输入...")
        else:
            print("验证码类型: 文本验证码")
            print("请在浏览器中输入验证码...")

        print("\n提示: 请在浏览器窗口中手动完成验证码")
        print("完成后按 Enter 键继续...")
        print("=" * 60 + "\n")

        # 保存验证码页面截图
        self.page.screenshot(path="dataeye_captcha.png")
        print("已保存验证码截图到 dataeye_captcha.png")

        # 等待用户输入
        try:
            input(">>> 完成验证码后，按 Enter 键继续...")
        except EOFError:
            # 在非交互模式下（如 Cloud Run），等待一段时间
            print("非交互模式，等待 60 秒让验证码自动处理...")
            time.sleep(60)

        print("继续登录流程...")
        time.sleep(2)
        return True

    def _apply_page_filters(self):
        """在页面上应用筛选条件"""
        print("正在设置页面筛选条件...")

        try:
            # 在 headless 模式下等待更长时间让页面完全加载
            print("  等待页面元素加载...")
            time.sleep(5)

            # 先保存一张截图用于调试
            try:
                self.page.screenshot(path="dataeye_before_filter.png")
                print("  已保存筛选前截图到 dataeye_before_filter.png")
                self._upload_screenshot_to_gcs("dataeye_before_filter.png")
            except Exception as e:
                print(f"  ⚠ 保存筛选前截图失败: {e}")

            # 根据版本应用不同的筛选条件
            if self.version == 'china':
                self._apply_china_filters()
            else:
                self._apply_overseas_filters()

        except Exception as e:
            print(f"  ⚠ 设置筛选条件时出错: {e}")
            # 尝试保存错误截图
            try:
                self.page.screenshot(path="dataeye_filter_error.png")
                print("  已保存错误截图到 dataeye_filter_error.png")
            except:
                pass

    def _apply_china_filters(self):
        """应用国内版筛选条件
        顺序：今天 -> 最多曝光 -> 新增素材
        """
        print("  [国内版] 开始设置筛选条件...")

        # 1. 点击"今天"日期筛选
        if self.date_filter == 'today':
            print("  步骤1: 选择日期 - 今天...")
            today_selectors = [
                'text=今天',
                'span:has-text("今天")',
                'div:has-text("今天")',
                '[class*="filter"]:has-text("今天")',
                '[class*="date"]:has-text("今天")',
            ]
            today_clicked = False
            for selector in today_selectors:
                try:
                    elems = self.page.query_selector_all(selector)
                    for elem in elems:
                        if elem.is_visible():
                            elem.click()
                            print(f"  ✓ 已选择日期: 今天")
                            today_clicked = True
                            time.sleep(2)
                            break
                    if today_clicked:
                        break
                except:
                    continue
            if not today_clicked:
                print("  ⚠ 未能选择日期: 今天")

        # 2. 点击"最多曝光"排序
        print("  步骤2: 选择排序 - 最多曝光...")
        sort_selectors = [
            'text=最多曝光',
            'span:has-text("最多曝光")',
            'div:has-text("最多曝光")',
            '[class*="sort"]:has-text("最多曝光")',
        ]
        sort_clicked = False
        for selector in sort_selectors:
            try:
                elems = self.page.query_selector_all(selector)
                for elem in elems:
                    if elem.is_visible():
                        elem.click()
                        print(f"  ✓ 已选择排序: 最多曝光")
                        sort_clicked = True
                        time.sleep(2)
                        break
                if sort_clicked:
                    break
            except:
                continue
        if not sort_clicked:
            print("  ⚠ 未能选择排序: 最多曝光")

        # 3. 勾选"新增素材"
        print("  步骤3: 勾选新增素材...")
        new_material_selectors = [
            'text=新增素材',
            'span:has-text("新增素材")',
            'label:has-text("新增素材")',
            '[class*="checkbox"]:has-text("新增素材")',
        ]
        new_material_clicked = False
        for selector in new_material_selectors:
            try:
                elems = self.page.query_selector_all(selector)
                for elem in elems:
                    if elem.is_visible():
                        elem.click()
                        print(f"  ✓ 已勾选: 新增素材")
                        new_material_clicked = True
                        time.sleep(2)
                        break
                if new_material_clicked:
                    break
            except:
                continue
        if not new_material_clicked:
            print("  ⚠ 未能勾选: 新增素材")

        # 等待页面刷新
        try:
            self.page.wait_for_load_state("networkidle", timeout=10000)
        except:
            pass
        time.sleep(2)

        # 保存筛选后截图
        try:
            self.page.screenshot(path="dataeye_after_filter.png")
            print("  已保存筛选后截图")
            self._upload_screenshot_to_gcs("dataeye_after_filter.png")
        except Exception as e:
            print(f"  ⚠ 保存筛选后截图失败: {e}")

    def _apply_overseas_filters(self):
        """应用海外版筛选条件（原有逻辑）"""
        print("  [海外版] 开始设置筛选条件...")

        # 1. 先点击排序 - 选择"热度值"
        sort_selectors = [
            # 使用纯文本匹配，最稳定
            'text=热度值',
            ':has-text("热度值")',
            '.oversea-selector-item-overflow:has-text("热度值")',
            'div.oversea-selector-item-overflow:has-text("热度值")',
            '[class*="selector-item"]:has-text("热度值")',
            '[class*="selector"]:has-text("热度值")',
        ]
        sort_clicked = False
        for selector in sort_selectors:
            try:
                elem = self.page.query_selector(selector)
                if elem and elem.is_visible():
                    # 使用 expect_response 等待 API 响应
                    try:
                        with self.page.expect_response(
                            lambda r: 'searchCreative' in r.url,
                            timeout=10000
                        ):
                            elem.click()
                            print(f"  ✓ 已点击热度值 (selector: {selector})")
                    except:
                        elem.click()
                        print(f"  ✓ 已点击热度值 (selector: {selector}) [无响应等待]")
                    sort_clicked = True
                    time.sleep(2)
                    break
            except Exception as e:
                print(f"  尝试 {selector} 失败: {e}")
                continue

        if not sort_clicked:
            print("  ⚠ 未能点击热度值排序")

        # 2. 再点击日期筛选 - 选择"今天"
        if self.date_filter == 'today':
            # 调试：查找页面上所有包含"今天"或"首次发现"的元素
            print("  查找日期筛选相关元素...")
            try:
                date_elements = self.page.evaluate('''() => {
                    const results = [];
                    const walker = document.createTreeWalker(
                        document.body,
                        NodeFilter.SHOW_TEXT,
                        null,
                        false
                    );
                    while (walker.nextNode()) {
                        const text = walker.currentNode.textContent.trim();
                        if (text.includes('今天') || text.includes('首次发现') || text.includes('7天') || text.includes('30天')) {
                            const parent = walker.currentNode.parentElement;
                            if (parent) {
                                const rect = parent.getBoundingClientRect();
                                results.push({
                                    text: text.substring(0, 50),
                                    tag: parent.tagName,
                                    className: parent.className,
                                    visible: rect.width > 0 && rect.height > 0,
                                    top: rect.top
                                });
                            }
                        }
                    }
                    return results.slice(0, 15);
                }''')
                for elem in date_elements:
                    print(f"    找到: {elem}")
            except Exception as e:
                print(f"    查找失败: {e}")

            # 使用找到的精确选择器点击"今天"
            today_selectors = [
                # 优先使用纯文本匹配，最稳定
                'li:has-text("今天")',
                # 备用：通配符匹配动态生成的 class 名称
                'li[class*="picker-presets-li"]:has-text("今天")',
                '[class*="picker-presets-li"]:has-text("今天")',
            ]
            today_clicked = False
            for selector in today_selectors:
                try:
                    elem = self.page.query_selector(selector)
                    if elem and elem.is_visible():
                        tag = elem.evaluate('el => el.tagName')
                        cls = elem.evaluate('el => el.className')
                        print(f"  找到今天元素: tag={tag}, class={cls}")
                        # 使用 expect_response 等待 API 响应
                        try:
                            with self.page.expect_response(
                                lambda r: 'searchCreative' in r.url,
                                timeout=10000
                            ):
                                elem.click()
                                print(f"  ✓ 已选择日期筛选: 今天 (selector: {selector})")
                        except:
                            elem.click()
                            print(f"  ✓ 已选择日期筛选: 今天 (selector: {selector}) [无响应等待]")
                        today_clicked = True
                        time.sleep(2)
                        # 保存点击后截图
                        self.page.screenshot(path="dataeye_after_today_click.png")
                        print("  已保存点击今天后截图")
                        break
                except Exception as e:
                    print(f"  尝试 {selector} 失败: {e}")
                    continue

            if not today_clicked:
                print("  ⚠ 未能点击今天日期筛选")
            else:
                # 点击"确定"按钮应用日期筛选
                confirm_selectors = [
                    'button:has-text("确定")',
                    '.ant-btn-primary:has-text("确定")',
                    '[class*="confirm"]:has-text("确定")',
                    'button.oversea-btn-primary',
                ]
                for selector in confirm_selectors:
                    try:
                        elem = self.page.query_selector(selector)
                        if elem and elem.is_visible():
                            try:
                                with self.page.expect_response(
                                    lambda r: 'searchCreative' in r.url,
                                    timeout=10000
                                ):
                                    elem.click()
                                    print(f"  ✓ 已点击确定按钮 (selector: {selector})")
                            except:
                                elem.click()
                                print(f"  ✓ 已点击确定按钮 (selector: {selector}) [无响应等待]")
                            time.sleep(2)
                            break
                    except:
                        continue

        # 等待页面刷新
        try:
            self.page.wait_for_load_state("networkidle", timeout=10000)
        except:
            pass
        time.sleep(2)

    def extract_data(self):
        """从页面提取数据 - 通过拦截 API 响应获取 JSON 数据"""
        print("正在提取数据...")

        try:
            return self._extract_from_api()
        except Exception as e:
            print(f"✗ 数据提取出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def _get_filter_date(self):
        """获取过滤日期"""
        if not self.date_filter:
            return None
        if self.date_filter == 'today':
            return datetime.now().strftime('%Y-%m-%d')
        elif self.date_filter == 'yesterday':
            from datetime import timedelta
            return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            return self.date_filter

    def _filter_records_by_date(self, records):
        """按日期过滤记录"""
        if not self.date_filter:
            return records

        filter_date = self._get_filter_date()
        if not filter_date:
            return records

        filtered = []
        for record in records:
            # 尝试多个可能的日期字段（优先使用 firstSeen/lastSeen）
            date_fields = ['firstSeen', 'lastSeen', 'firstTime', 'createTime', 'updateTime', 'date', 'created_at', 'firstShowTime']
            record_date = None
            for field in date_fields:
                if field in record and record[field]:
                    date_value = record[field]
                    # 处理时间戳（毫秒）
                    if isinstance(date_value, (int, float)) and date_value > 1000000000000:
                        record_date = datetime.fromtimestamp(date_value / 1000).strftime('%Y-%m-%d')
                    elif isinstance(date_value, (int, float)):
                        record_date = datetime.fromtimestamp(date_value).strftime('%Y-%m-%d')
                    elif isinstance(date_value, str):
                        record_date = date_value[:10]  # 取前10个字符 YYYY-MM-DD
                    break

            if record_date and record_date == filter_date:
                filtered.append(record)

        return filtered

    def _sort_records_by_heat(self, records):
        """按热度值排序记录（降序）"""
        return sorted(records, key=lambda x: x.get('heatNum', 0), reverse=True)

    def _extract_from_api(self):
        """通过拦截 API 请求获取 JSON 数据"""
        all_records = []
        all_pages_data = []
        total_count = 0
        page = 1
        page_size = 20
        self.captured_responses = []
        self.video_urls = []

        # 显示过滤条件
        if self.date_filter:
            print(f"日期过滤: {self._get_filter_date()}")
        if self.max_records:
            print(f"最大记录数: {self.max_records}")

        def handle_response(response):
            """处理网络响应"""
            try:
                url = response.url
                # 优先捕获 searchCreative API（主要数据接口）
                if 'searchCreative' in url:
                    if response.status == 200:
                        try:
                            json_data = response.json()
                            print(f"✓ 捕获到 searchCreative API: {url[:100]}...")
                            self.captured_responses.append({
                                'url': url,
                                'data': json_data
                            })
                        except:
                            pass
                # 捕获其他可能的 API 响应
                elif any(keyword in url for keyword in [
                    'playlet', 'material', 'creative'
                ]):
                    if response.status == 200:
                        content_type = response.headers.get('content-type', '')
                        if 'application/json' in content_type:
                            try:
                                json_data = response.json()
                                print(f"✓ 捕获到 API 响应: {url[:100]}...")
                                self.captured_responses.append({
                                    'url': url,
                                    'data': json_data
                                })
                            except:
                                pass

                # 捕获 MP4 视频 URL
                if '.mp4' in url.lower():
                    print(f"✓ 发现视频 URL: {url[:100]}...")
                    self.video_urls.append(url)

            except Exception as e:
                pass

        # 注册响应监听器
        self.page.on('response', handle_response)

        try:
            # 先注册监听器，再刷新页面
            print("刷新页面以捕获 API 响应...")

            # 使用 expect_response 等待 API 响应
            # 国内版使用 searchMaterial，海外版使用 searchCreative
            api_pattern = 'searchMaterial' if self.version == 'china' else 'searchCreative'
            try:
                with self.page.expect_response(
                    lambda r, pattern=api_pattern: pattern in r.url,
                    timeout=30000
                ) as response_info:
                    self.page.reload()
                    print(f"等待 {api_pattern} API 响应...")

                # 获取响应数据
                response = response_info.value
                if response.status == 200:
                    try:
                        json_data = response.json()
                        print(f"✓ 直接捕获到 searchCreative API")
                        self.captured_responses.append({
                            'url': response.url,
                            'data': json_data
                        })
                    except Exception as e:
                        print(f"  解析响应失败: {e}")
            except Exception as e:
                print(f"  等待 searchCreative 超时: {e}")

            try:
                self.page.wait_for_load_state("networkidle", timeout=30000)
            except:
                pass

            # 在 headless 模式下等待更长时间
            print("等待页面完全加载...")
            time.sleep(5)

            # 打印已捕获的响应数量
            print(f"已捕获 {len(self.captured_responses)} 个 API 响应")

            # 先在页面上应用筛选条件（点击热度值和今日）
            # 这会触发 API 请求，响应会被上面的监听器捕获
            self._apply_page_filters()

            # 等待 API 响应
            print("等待 API 响应...")
            time.sleep(3)

            # 再次打印捕获的响应数量
            print(f"筛选后共捕获 {len(self.captured_responses)} 个 API 响应")

            # 保存当前页面截图
            try:
                self.page.screenshot(path="dataeye_data_page.png")
                print("已保存数据页面截图到 dataeye_data_page.png")
                self._upload_screenshot_to_gcs("dataeye_data_page.png")
            except Exception as e:
                print(f"  ⚠ 保存数据页面截图失败: {e}")

            # 解析捕获的响应
            page_records, total_count, page_size = self._parse_api_response(self.captured_responses)

            if page_records:
                # 注意：日期过滤已在页面上通过点击"今天"完成，不需要再在代码中过滤
                # 如果需要额外的日期过滤（比如页面筛选失败），可以取消下面的注释
                # if self.date_filter:
                #     page_records = self._filter_records_by_date(page_records)
                print(f"第 1 页获取到 {len(page_records)} 条记录，总计 {total_count} 条")

                # 限制记录数
                if self.max_records and len(page_records) > self.max_records:
                    page_records = page_records[:self.max_records]

                all_records.extend(page_records)
                all_pages_data.append({
                    "page": 1,
                    "page_size": page_size,
                    "list": page_records,
                    "count": len(page_records)
                })

                # 从记录中提取视频 URL
                self._extract_video_urls_from_records(page_records)
            else:
                print("第一页未获取到数据，尝试从页面 DOM 提取...")
                # 尝试从页面 DOM 提取数据
                page_records = self._extract_from_dom()
                if page_records:
                    all_records.extend(page_records)
                    all_pages_data.append({
                        "page": 1,
                        "list": page_records,
                        "count": len(page_records)
                    })

            # 检查是否已达到最大记录数
            if self.max_records and len(all_records) >= self.max_records:
                all_records = all_records[:self.max_records]
                print(f"已达到最大记录数 {self.max_records}，停止获取")
            # 如果有更多页面，继续翻页
            elif total_count > len(all_records):
                total_pages = (total_count + page_size - 1) // page_size
                print(f"预计 {total_pages} 页，每页 {page_size} 条")

                while len(all_records) < total_count and page < 100:
                    # 检查是否已达到最大记录数
                    if self.max_records and len(all_records) >= self.max_records:
                        print(f"已达到最大记录数 {self.max_records}，停止翻页")
                        break

                    page += 1
                    print(f"正在获取第 {page} 页数据... (已获取 {len(all_records)} 条)")

                    # 记录当前响应数量，用于检测新响应
                    prev_response_count = len(self.captured_responses)

                    # 使用 expect_response 等待 API 响应
                    try:
                        with self.page.expect_response(
                            lambda r: 'searchCreative' in r.url,
                            timeout=15000
                        ) as response_info:
                            # 点击下一页
                            clicked = self._click_next_page(page)
                            if not clicked:
                                print("无法翻页，停止获取")
                                break

                        # 获取响应
                        response = response_info.value
                        if response.status == 200:
                            try:
                                json_data = response.json()
                                self.captured_responses.append({
                                    'url': response.url,
                                    'data': json_data
                                })
                                print(f"  ✓ 捕获到翻页 API 响应")
                            except:
                                pass
                    except Exception as e:
                        print(f"  等待 API 响应超时: {e}")

                    # 调试：打印捕获的响应数量
                    print(f"  总共捕获到 {len(self.captured_responses)} 个响应")

                    # 只解析新捕获的响应
                    new_captured = self.captured_responses[prev_response_count:]
                    page_records, _, _ = self._parse_api_response(new_captured)
                    if page_records:
                        # 注意：日期过滤已在页面上通过点击"今天"完成，不需要再在代码中过滤
                        # if self.date_filter:
                        #     page_records = self._filter_records_by_date(page_records)

                        print(f"第 {page} 页获取到 {len(page_records)} 条记录")

                        # 限制记录数
                        if self.max_records:
                            remaining = self.max_records - len(all_records)
                            if len(page_records) > remaining:
                                page_records = page_records[:remaining]

                        all_records.extend(page_records)
                        all_pages_data.append({
                            "page": page,
                            "page_size": page_size,
                            "list": page_records,
                            "count": len(page_records)
                        })
                        self._extract_video_urls_from_records(page_records)
                    else:
                        print(f"第 {page} 页无数据，停止翻页")
                        break

                    if self._is_last_page():
                        print("已到达最后一页")
                        break

        finally:
            try:
                self.page.remove_listener('response', handle_response)
            except:
                pass

        if all_records:
            # 按热度排序
            all_records = self._sort_records_by_heat(all_records)
            print(f"✓ 共获取 {len(all_records)} 条记录（已按热度排序）")
            print(f"✓ 共发现 {len(self.video_urls)} 个视频 URL")

            # 保存完整的响应结构
            full_response = {
                "code": 200,
                "data": {
                    "total": total_count if total_count > 0 else len(all_records),
                    "total_pages": len(all_pages_data),
                    "fetched_records": len(all_records),
                    "pages": all_pages_data,
                    "video_urls": list(set(self.video_urls))  # 去重
                },
                "message": "success",
                "fetched_at": datetime.now().isoformat()
            }

            with open("dataeye_responses.json", "w", encoding="utf-8") as f:
                json.dump(full_response, f, ensure_ascii=False, indent=2)
            print("已保存完整响应数据到 dataeye_responses.json")

            return all_records

        print("未能从 API 响应中提取到有效数据")
        return None

    def _parse_api_response(self, captured_data):
        """解析 API 响应"""
        # 调试：打印所有捕获的 API URL
        print(f"  捕获到 {len(captured_data)} 个 API 响应:")
        for item in captured_data:
            print(f"    - {item['url'][:100]}...")

        # 优先查找 searchCreative API 响应（海外版）
        for item in captured_data:
            if 'searchCreative' in item['url']:
                data = item['data']
                # DataEye API 响应结构: statusCode + content.searchList
                if isinstance(data, dict) and data.get('statusCode') == 200:
                    content = data.get('content', {})
                    if isinstance(content, dict):
                        search_list = content.get('searchList', [])
                        if search_list and len(search_list) > 0:
                            # 获取分页信息
                            page_info = data.get('page', {})
                            total = page_info.get('totalRecords', 0) if page_info else content.get('totalRecord', 0)
                            page_size = page_info.get('pageSize', 40) if page_info else 40
                            print(f"  使用 API (海外版): {item['url'][:80]}...")
                            return search_list, total, page_size

        # 查找 searchMaterial API 响应（国内版）
        for item in captured_data:
            if 'searchMaterial' in item['url']:
                data = item['data']
                print(f"  尝试解析 searchMaterial API...")
                # 国内版 API 响应结构可能不同，尝试多种格式
                if isinstance(data, dict):
                    # 尝试格式1: code + data.list
                    if data.get('code') == 200 or data.get('code') == 0:
                        nested = data.get('data', {})
                        if isinstance(nested, dict):
                            records = nested.get('list', nested.get('records', []))
                            if records and len(records) > 0:
                                total = nested.get('total', nested.get('totalCount', len(records)))
                                page_size = nested.get('pageSize', nested.get('size', 20))
                                print(f"  使用 API (国内版 格式1): {item['url'][:80]}...")
                                return records, total, page_size
                    # 尝试格式2: statusCode + content
                    if data.get('statusCode') == 200:
                        content = data.get('content', {})
                        if isinstance(content, dict):
                            records = content.get('list', content.get('searchList', []))
                            if records and len(records) > 0:
                                total = content.get('total', content.get('totalRecord', len(records)))
                                page_size = content.get('pageSize', 20)
                                print(f"  使用 API (国内版 格式2): {item['url'][:80]}...")
                                return records, total, page_size
                    # 尝试格式3: 直接是 data 数组
                    if isinstance(data.get('data'), list) and len(data.get('data', [])) > 0:
                        records = data['data']
                        total = data.get('total', len(records))
                        page_size = data.get('pageSize', 20)
                        print(f"  使用 API (国内版 格式3): {item['url'][:80]}...")
                        return records, total, page_size

        # 回退：尝试其他素材相关 API
        priority_keywords = ['creative', 'material', 'playlet']
        for keyword in priority_keywords:
            for item in captured_data:
                if keyword in item['url'] and 'searchCreative' not in item['url'] and 'searchMaterial' not in item['url']:
                    records, total, size = self._extract_from_response(item['data'])
                    if records and len(records) > 0:
                        first_record = records[0]
                        if isinstance(first_record, dict) and any(
                            k in first_record for k in ['videoList', 'videoUrl', 'video_url', 'materialId', 'picList', 'id']
                        ):
                            print(f"  使用 API (回退): {item['url'][:80]}...")
                            return records, total, size

        return [], 0, 40

    def _extract_from_response(self, data):
        """从单个响应中提取数据"""
        if not isinstance(data, dict):
            return [], 0, 20

        # 尝试不同的数据结构
        nested = data.get('data', data)

        if isinstance(nested, dict):
            # 常见的列表字段名
            list_keys = ['list', 'items', 'records', 'data', 'rows', 'content']
            for key in list_keys:
                records = nested.get(key)
                if records and isinstance(records, list) and len(records) > 0:
                    total = nested.get('total', nested.get('totalCount', nested.get('count', 0)))
                    page_size = nested.get('pageSize', nested.get('page_size', nested.get('size', 20)))
                    return records, total, page_size

        elif isinstance(nested, list) and len(nested) > 0:
            return nested, len(nested), len(nested)

        return [], 0, 20

    def _extract_from_dom(self):
        """从页面 DOM 提取数据（备用方案）"""
        try:
            print("尝试从 DOM 提取数据...")
            # 这里需要根据实际页面结构调整选择器
            # 暂时返回空列表
            return []
        except Exception as e:
            print(f"DOM 提取失败: {e}")
            return []

    def _extract_video_urls_from_records(self, records):
        """从记录中提取视频 URL"""
        for record in records:
            if isinstance(record, dict):
                # 遍历所有字段查找视频 URL
                for key, value in record.items():
                    if isinstance(value, str) and '.mp4' in value.lower():
                        self.video_urls.append(value)
                    elif isinstance(value, str) and ('video' in key.lower() or 'url' in key.lower()):
                        if value.startswith('http'):
                            self.video_urls.append(value)

    def _click_next_page(self, target_page):
        """点击下一页按钮"""
        try:
            # 先尝试直接点击页码（更可靠）
            page_num_selectors = [
                f'.ant-pagination-item-{target_page}',
                f'li[title="{target_page}"]',
                f'.ant-pagination-item:has-text("{target_page}")',
                f'a.ant-pagination-item-link:has-text("{target_page}")',
            ]

            for selector in page_num_selectors:
                try:
                    page_btn = self.page.query_selector(selector)
                    if page_btn and page_btn.is_visible():
                        print(f"  找到页码按钮: {selector}")
                        # 尝试点击内部的 <a> 标签
                        inner_link = page_btn.query_selector('a')
                        if inner_link:
                            inner_link.click()
                            print(f"  点击页码 {target_page} 内部链接")
                        else:
                            page_btn.click()
                            print(f"  点击页码 {target_page}")
                        return True
                except Exception as e:
                    print(f"  尝试 {selector} 失败: {e}")
                    continue

            # 回退：点击下一页按钮
            next_btn_selectors = [
                '.ant-pagination-next:not(.ant-pagination-disabled)',
                '.el-pagination .btn-next:not([disabled])',
                'button:has-text("下一页"):not([disabled])',
                'li.next:not(.disabled) a',
                'a:has-text(">")',
                '.pagination-next:not(.disabled)',
            ]

            for selector in next_btn_selectors:
                try:
                    next_btn = self.page.query_selector(selector)
                    if next_btn and next_btn.is_visible():
                        print(f"  找到下一页按钮: {selector}")
                        next_btn.click()
                        print(f"  点击下一页按钮")
                        return True
                except Exception as e:
                    print(f"  尝试 {selector} 失败: {e}")
                    continue

            print("  未找到任何翻页按钮")
            return False
        except Exception as e:
            print(f"点击下一页失败: {e}")
            return False

    def _is_last_page(self):
        """检查是否到达最后一页"""
        last_page_indicators = [
            '.ant-pagination-next.ant-pagination-disabled',
            '.el-pagination .btn-next[disabled]',
            'button.next[disabled]',
        ]

        for selector in last_page_indicators:
            try:
                if self.page.query_selector(selector):
                    return True
            except:
                continue

        return False

    def _classify_url(self, url: str) -> str:
        """
        分类 URL 类型

        Returns:
            'direct_video': 直接视频链接 (.mp4, .webm 等)
            'youtube': YouTube 视频
            'skip': 应跳过的链接 (App Store, Google Play 等)
        """
        url_lower = url.lower()
        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        # 直接视频链接
        video_extensions = ['.mp4', '.webm', '.m3u8', '.flv', '.avi', '.mov']
        if any(ext in url_lower for ext in video_extensions):
            return 'direct_video'

        # YouTube 视频
        if 'youtube.com' in domain or 'youtu.be' in domain:
            return 'youtube'

        # 应跳过的链接
        skip_domains = [
            'play.google.com',
            'apps.apple.com',
            'itunes.apple.com',
            'app.adjust.com',
            'bit.ly',
            't.co',
        ]
        if any(skip_domain in domain for skip_domain in skip_domains):
            return 'skip'

        # 落地页（通常不是视频）
        landing_indicators = ['utm_', 'campaign', 'adid=', 'click', 'track']
        if any(indicator in url_lower for indicator in landing_indicators):
            return 'skip'

        # 默认尝试作为直接视频下载
        return 'direct_video'

    def _download_direct_video(self, url: str, filepath: Path) -> bool:
        """下载直接视频链接"""
        try:
            response = requests.get(url, stream=True, timeout=60, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            response.raise_for_status()

            content_type = response.headers.get('content-type', '').lower()
            # 检查是否真的是视频
            if 'text/html' in content_type:
                return False

            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # 验证文件大小（太小可能是错误页面）
            file_size = os.path.getsize(filepath)
            if file_size < 10000:  # 小于 10KB 可能不是视频
                os.remove(filepath)
                return False

            return True
        except Exception as e:
            if filepath.exists():
                os.remove(filepath)
            raise e

    def _download_youtube_video(self, url: str, filepath: Path) -> bool:
        """使用 yt-dlp 下载 YouTube 视频"""
        import subprocess
        try:
            # 构建输出路径（不带扩展名，yt-dlp 会自动添加）
            output_template = str(filepath.with_suffix(''))

            cmd = [
                'yt-dlp',
                '-f', 'best[height<=720]',  # 限制分辨率以加快下载
                '-o', output_template + '.%(ext)s',
                '--no-playlist',
                '--quiet',
                '--no-warnings',
                url
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

            if result.returncode == 0:
                # 查找下载的文件（可能是 .mp4, .webm 等）
                for ext in ['.mp4', '.webm', '.mkv']:
                    downloaded = Path(output_template + ext)
                    if downloaded.exists():
                        # 重命名为 .mp4
                        if ext != '.mp4':
                            final_path = filepath.with_suffix('.mp4')
                            downloaded.rename(final_path)
                        return True
                return False
            else:
                return False
        except FileNotFoundError:
            print("  ⚠ yt-dlp 未安装，跳过 YouTube 视频")
            return False
        except subprocess.TimeoutExpired:
            print("  ⚠ 下载超时")
            return False
        except Exception as e:
            raise e

    def download_videos(self, output_dir: str = "videos", max_videos: int = None):
        """
        下载所有捕获的视频

        Args:
            output_dir: 视频保存目录
            max_videos: 最大下载数量（None 表示全部下载）
        """
        if not self.video_urls:
            print("没有视频 URL 可下载")
            return []

        # 去重
        unique_urls = list(set(self.video_urls))

        # 分类 URL
        direct_videos = []
        youtube_videos = []
        skipped_urls = []

        for url in unique_urls:
            url_type = self._classify_url(url)
            if url_type == 'direct_video':
                direct_videos.append(url)
            elif url_type == 'youtube':
                youtube_videos.append(url)
            else:
                skipped_urls.append(url)

        print(f"URL 分类: 直接视频 {len(direct_videos)} 个, YouTube {len(youtube_videos)} 个, 跳过 {len(skipped_urls)} 个")

        # 创建输出目录
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        downloaded_files = []

        # 合并要下载的 URL（直接视频优先）
        urls_to_download = direct_videos + youtube_videos
        if max_videos:
            urls_to_download = urls_to_download[:max_videos]

        for i, url in enumerate(urls_to_download, 1):
            try:
                url_type = self._classify_url(url)
                print(f"[{i}/{len(urls_to_download)}] [{url_type}] {url[:60]}...")

                filename = f"video_{i}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"
                filepath = output_path / filename

                success = False
                if url_type == 'youtube':
                    success = self._download_youtube_video(url, filepath)
                else:
                    success = self._download_direct_video(url, filepath)

                if success and filepath.exists():
                    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
                    print(f"  ✓ 已保存: {filepath} ({file_size_mb:.2f} MB)")
                    downloaded_files.append(str(filepath))
                else:
                    print(f"  ✗ 下载失败或不是有效视频")

            except Exception as e:
                print(f"  ✗ 下载失败: {e}")

        print(f"✓ 共下载 {len(downloaded_files)} 个视频到 {output_dir}/")
        return downloaded_files

    def save_data(self, data, format='json'):
        """保存数据到文件"""
        if not data:
            print("没有数据可保存")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if format == 'json':
            filename = f"dataeye_data_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"✓ 数据已保存到 {filename}")

        elif format == 'csv':
            import pandas as pd
            filename = f"dataeye_data_{timestamp}.csv"
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"✓ 数据已保存到 {filename}")

    def run(self, save_format='json', download_videos=True, video_dir='videos',
            upload_to_bq=False, bq_project=None, bq_dataset=None,
            upload_to_gcs=False, gcs_bucket=None) -> bool:
        """
        运行完整的抓取流程

        Args:
            save_format: 保存格式 ('json', 'csv')
            download_videos: 是否下载视频
            video_dir: 视频保存目录
            upload_to_bq: 是否上传到 BigQuery
            bq_project: BigQuery 项目 ID
            bq_dataset: BigQuery 数据集 ID
            upload_to_gcs: 是否上传视频到 GCS
            gcs_bucket: GCS bucket 名称

        Returns:
            bool: 是否成功
        """
        success = False
        try:
            self.start()

            if self.login():
                data = self.extract_data()
                if data:
                    self.save_data(data, format=save_format)

                    # 生成批次 ID（时间戳格式）
                    batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')

                    # 根据版本区分存储路径
                    version_video_dir = f"{video_dir}/{self.version}"

                    # 下载视频 - 从 videoList 提取真正的 mp4 URL
                    if download_videos:
                        self._download_videos_from_data(data, output_dir=version_video_dir)

                    # 上传到 GCS（JSON + 视频 + 截图）- 按版本区分目录
                    if upload_to_gcs and gcs_bucket:
                        print(f"\n=== 开始上传到 GCS ===")
                        print(f"版本: {self.version}")
                        print(f"批次 ID: {batch_id}")
                        print(f"批次目录: gs://{gcs_bucket}/{self.gcs_prefix}/batch_{batch_id}/")

                        self._upload_json_to_gcs(gcs_bucket, batch_id)
                        self._upload_videos_to_gcs(version_video_dir, gcs_bucket, batch_id)
                        # 上传截图到批次目录
                        self._upload_screenshots_to_batch(gcs_bucket, batch_id)

                    # 上传到 BigQuery（按版本区分表名）
                    if upload_to_bq and bq_project and bq_dataset:
                        self._upload_to_bigquery(bq_project, bq_dataset, batch_id)

                    success = True
                else:
                    print("⚠ 未能提取到数据，请检查页面结构")
                    # 即使没有数据，也上传截图用于调试
                    if upload_to_gcs and gcs_bucket:
                        batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
                        self._upload_screenshots_to_batch(gcs_bucket, batch_id)
            else:
                print("⚠ 登录失败，无法继续")
                # 登录失败也上传截图用于调试
                if upload_to_gcs and gcs_bucket:
                    batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
                    self._upload_screenshots_to_batch(gcs_bucket, batch_id)

        except Exception as e:
            print(f"✗ 运行出错: {str(e)}")
            import traceback
            traceback.print_exc()

        finally:
            self.stop()

        return success

    def _download_videos_from_data(self, data, output_dir: str = "videos", max_videos: int = None):
        """
        从数据中提取 videoList 并下载视频

        Args:
            data: 记录列表（每条记录包含 videoList）
            output_dir: 视频保存目录
            max_videos: 最大下载数量
        """
        # data 是记录列表
        records = data if isinstance(data, list) else []

        # 从记录中提取所有视频 URL
        video_urls = []
        for record in records:
            video_list = record.get('videoList', [])
            # 兼容海外版 (materialId) 和国内版 (id) 的字段名
            material_id = record.get('materialId') or record.get('id') or 'unknown'
            for video_url in video_list:
                if video_url and '.mp4' in video_url.lower():
                    video_urls.append({
                        'url': video_url,
                        'material_id': material_id
                    })

        if not video_urls:
            print("没有找到可下载的视频 URL")
            return []

        # 去重（按 URL）
        seen_urls = set()
        unique_videos = []
        for v in video_urls:
            if v['url'] not in seen_urls:
                seen_urls.add(v['url'])
                unique_videos.append(v)

        print(f"找到 {len(unique_videos)} 个唯一视频 URL")

        if max_videos:
            unique_videos = unique_videos[:max_videos]

        # 创建输出目录
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        downloaded_files = []

        for i, video_info in enumerate(unique_videos, 1):
            url = video_info['url']
            material_id = video_info['material_id']

            try:
                print(f"[{i}/{len(unique_videos)}] 下载 material_id={material_id}...")

                # 使用 material_id 作为文件名
                filename = f"{material_id}.mp4"
                filepath = output_path / filename

                # 如果文件已存在，跳过
                if filepath.exists():
                    print(f"  ⏭ 已存在，跳过: {filepath}")
                    downloaded_files.append(str(filepath))
                    continue

                success = self._download_direct_video(url, filepath)

                if success and filepath.exists():
                    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
                    print(f"  ✓ 已保存: {filepath} ({file_size_mb:.2f} MB)")
                    downloaded_files.append(str(filepath))
                else:
                    print(f"  ✗ 下载失败")

            except Exception as e:
                print(f"  ✗ 下载失败: {e}")

        print(f"✓ 共下载 {len(downloaded_files)} 个视频到 {output_dir}/")
        return downloaded_files

    def _upload_json_to_gcs(self, bucket_name: str, batch_id: str = None):
        """上传 JSON 数据到 GCS（按版本区分目录）"""
        try:
            from google.cloud import storage

            # 读取本地 JSON 文件
            with open("dataeye_responses.json", "r", encoding="utf-8") as f:
                data = json.load(f)

            # 上传到 GCS（使用版本前缀 + 批次 ID）
            # 路径格式: {gcs_prefix}/batch_{batch_id}/data.json
            # 例如: adx/batch_20231219_120000/data.json 或 adx_cn/batch_20231219_120000/data.json
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob_path = f"{self.gcs_prefix}/batch_{batch_id}/data.json"
            blob = bucket.blob(blob_path)
            blob.upload_from_string(
                json.dumps(data, ensure_ascii=False, indent=2),
                content_type='application/json'
            )
            print(f"✓ 已上传 JSON 数据到 GCS: gs://{bucket_name}/{blob_path}")
            return batch_id

        except ImportError:
            print("⚠ 未安装 google-cloud-storage，跳过 GCS 上传")
        except Exception as e:
            import traceback
            print(f"✗ JSON 上传到 GCS 失败: {e}")
            traceback.print_exc()
        return batch_id

    def _upload_videos_to_gcs(self, video_dir: str, bucket_name: str, batch_id: str = None):
        """上传视频到 GCS（按版本区分目录）"""
        try:
            from google.cloud import storage

            client = storage.Client()
            bucket = client.bucket(bucket_name)

            # 获取视频目录下的所有 mp4 文件
            video_path = Path(video_dir)
            if not video_path.exists():
                print(f"⚠ 视频目录不存在: {video_dir}")
                return

            video_files = list(video_path.glob("*.mp4"))
            if not video_files:
                print(f"⚠ 没有找到视频文件: {video_dir}")
                return

            uploaded_count = 0
            for video_file in video_files:
                # 路径格式: {gcs_prefix}/batch_{batch_id}/videos/{filename}
                blob_path = f"{self.gcs_prefix}/batch_{batch_id}/videos/{video_file.name}"
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(str(video_file))
                uploaded_count += 1

            print(f"✓ 已上传 {uploaded_count} 个视频到 GCS: gs://{bucket_name}/{self.gcs_prefix}/batch_{batch_id}/videos/")

        except ImportError:
            print("⚠ 未安装 google-cloud-storage，跳过 GCS 上传")
        except Exception as e:
            import traceback
            print(f"✗ 视频上传到 GCS 失败: {e}")
            traceback.print_exc()

    def _upload_screenshots_to_batch(self, bucket_name: str, batch_id: str):
        """上传所有截图到批次目录（按版本区分）"""
        screenshot_files = [
            "dataeye_before_login.png",
            "dataeye_before_filter.png",
            "dataeye_after_filter.png",
            "dataeye_after_today_click.png",
            "dataeye_data_page.png",
            "dataeye_login_error.png",
            "dataeye_captcha.png"
        ]

        try:
            from google.cloud import storage
            client = storage.Client()
            bucket = client.bucket(bucket_name)

            uploaded_count = 0
            for screenshot in screenshot_files:
                if os.path.exists(screenshot):
                    # 路径格式: {gcs_prefix}/batch_{batch_id}/screenshots/{filename}
                    blob_path = f"{self.gcs_prefix}/batch_{batch_id}/screenshots/{screenshot}"
                    blob = bucket.blob(blob_path)
                    blob.upload_from_filename(screenshot)
                    uploaded_count += 1

            if uploaded_count > 0:
                print(f"✓ 已上传 {uploaded_count} 个截图到 GCS: gs://{bucket_name}/{self.gcs_prefix}/batch_{batch_id}/screenshots/")

        except ImportError:
            print("⚠ 未安装 google-cloud-storage，跳过截图上传")
        except Exception as e:
            print(f"⚠ 截图上传失败: {e}")

    def _upload_to_bigquery(self, project_id: str, dataset_id: str, batch_id: str = None):
        """上传数据到 BigQuery（按版本区分表名，追加模式）"""
        try:
            from bigquery_storage import BigQueryUploader

            with open("dataeye_responses.json", "r", encoding="utf-8") as f:
                data = json.load(f)

            uploader = BigQueryUploader(project_id, dataset_id)
            # 固定表名: dataeye_overseas 或 dataeye_china
            # 数据追加到表中，用 batch_id 字段区分不同批次
            table_name = f"dataeye_{self.version}"
            count = uploader.upload_dataeye_materials(data, batch_id=batch_id, table_prefix=table_name)
            print(f"✓ 已追加 {count} 条记录到 BigQuery")
            print(f"  表: {dataset_id}.{table_name}")
            print(f"  批次 ID: {batch_id}")

        except ImportError:
            print("⚠ 未安装 google-cloud-bigquery，跳过 BigQuery 上传")
        except AttributeError as e:
            print(f"⚠ BigQuery 上传器尚未支持 DataEye 数据: {e}")
        except Exception as e:
            import traceback
            print(f"✗ BigQuery 上传失败: {e}")
            traceback.print_exc()


def save_cookies_manually():
    """手动登录并保存 Cookie 的模式"""
    load_dotenv()

    username = os.getenv('DATAEYE_USERNAME', '')
    password = os.getenv('DATAEYE_PASSWORD', '')

    scraper = DataEyeScraper(
        username=username,
        password=password,
        headless=False  # 必须显示浏览器
    )

    try:
        scraper.start()

        # 访问登录页面
        print(f"正在打开 DataEye 平台...")
        scraper.page.goto(scraper.target_url, timeout=60000)

        print("\n" + "=" * 60)
        print("请在浏览器中手动完成登录")
        print("=" * 60)
        print("步骤：")
        print("  1. 输入用户名和密码")
        print("  2. 完成验证码（如果有）")
        print("  3. 点击登录按钮")
        print("  4. 等待页面跳转到目标页面")
        print("\n登录成功后，按 Enter 键保存 Cookie...")
        print("=" * 60 + "\n")

        input(">>> 按 Enter 键保存 Cookie...")

        # 检查是否登录成功
        current_url = scraper.page.url
        print(f"当前 URL: {current_url}")

        if "login" not in current_url.lower() and "signin" not in current_url.lower():
            scraper.save_cookies()
            print("\n✓ Cookie 保存成功！下次运行时将自动使用 Cookie 登录。")
        else:
            print("\n⚠ 似乎还未登录成功，是否仍要保存 Cookie？")
            confirm = input("输入 y 保存，其他键取消: ")
            if confirm.lower() == 'y':
                scraper.save_cookies()

    except Exception as e:
        print(f"出错: {e}")
    finally:
        input("\n按 Enter 键关闭浏览器...")
        scraper.stop()


def run_single_version(version: str, username: str, password: str, config: dict) -> bool:
    """运行单个版本的爬取"""
    print(f"\n{'='*60}")
    print(f"开始爬取: {version} 版本")
    print(f"{'='*60}")

    # 创建抓取器实例
    scraper = DataEyeScraper(
        username=username,
        password=password,
        headless=config['use_headless'],
        max_records=config['max_records'],
        date_filter=config['date_filter'],
        version=version
    )

    try:
        # 运行抓取，获取返回值
        success = scraper.run(
            save_format='json',
            download_videos=config['download_videos'],
            video_dir=config['video_dir'],
            upload_to_bq=config['upload_to_bq'],
            bq_project=config['bq_project'],
            bq_dataset=config['bq_dataset'],
            upload_to_gcs=config['upload_to_gcs'],
            gcs_bucket=config['gcs_bucket']
        )

        if success:
            print(f"✓ {version} 版本爬取完成")
        else:
            print(f"✗ {version} 版本爬取失败")

        return success
    except Exception as e:
        print(f"✗ {version} 版本爬取异常: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    load_dotenv()

    # 检查命令行参数
    if len(sys.argv) > 1 and sys.argv[1] == '--save-cookies':
        save_cookies_manually()
        return

    # 从环境变量获取登录信息
    username = os.getenv('DATAEYE_USERNAME', '')
    password = os.getenv('DATAEYE_PASSWORD', '')

    # Cookie 模式下账号密码可选（仅在 Cookie 失效时需要）
    if not username or not password:
        print("警告: 未设置 DATAEYE_USERNAME 和 DATAEYE_PASSWORD，将仅使用 Cookie 登录")
        print("如果 Cookie 失效，登录将失败")

    # BigQuery 配置 - 使用 ADX 专用数据集
    bq_project = os.getenv('BQ_PROJECT_ID')
    bq_dataset = os.getenv('ADX_BQ_DATASET_ID', 'adx_data')  # DataEye/ADX 专用数据集
    upload_to_bq = bool(bq_project and bq_dataset)

    # GCS 配置
    gcs_bucket = os.getenv('GCS_BUCKET_NAME')
    upload_to_gcs = os.getenv('DATAEYE_UPLOAD_TO_GCS', 'false').lower() == 'true'

    # 视频下载目录
    video_dir = os.getenv('DATAEYE_VIDEO_DIR', 'videos')

    # 检测是否在 Cloud Run 环境（多种方式检测）
    is_cloud_run = (
        os.getenv('K_SERVICE') is not None or  # Cloud Run Services
        os.getenv('CLOUD_RUN_JOB') is not None or  # Cloud Run Jobs
        os.getenv('CLOUD_RUN', 'false').lower() == 'true'  # 手动设置
    )

    # 调试：打印环境变量
    print(f"环境检测: K_SERVICE={os.getenv('K_SERVICE')}, CLOUD_RUN_JOB={os.getenv('CLOUD_RUN_JOB')}, CLOUD_RUN={os.getenv('CLOUD_RUN')}")
    print(f"is_cloud_run={is_cloud_run}")

    # 获取配置参数
    max_records = int(os.getenv('DATAEYE_MAX_RECORDS', '100'))  # 默认 100 条
    date_filter = os.getenv('DATAEYE_DATE_FILTER', 'today')  # 默认今天

    # 版本配置：overseas, china, 或 both（同时爬取两个版本）
    version = os.getenv('DATAEYE_VERSION', 'both')
    print(f"版本配置: {version}")

    # headless 模式：Cloud Run 环境强制使用，或通过环境变量控制
    use_headless = is_cloud_run or os.getenv('HEADLESS', 'false').lower() == 'true'
    print(f"使用 headless 模式: {use_headless}")

    # 是否下载视频
    download_videos = os.getenv('DATAEYE_DOWNLOAD_VIDEOS', 'false').lower() == 'true'

    # 公共配置
    config = {
        'use_headless': use_headless,
        'max_records': max_records,
        'date_filter': date_filter,
        'download_videos': download_videos,
        'video_dir': video_dir,
        'upload_to_bq': upload_to_bq,
        'bq_project': bq_project,
        'bq_dataset': bq_dataset,
        'upload_to_gcs': upload_to_gcs,
        'gcs_bucket': gcs_bucket,
    }

    # 根据版本配置运行
    results = {}

    if version == 'both':
        # 同时爬取海外版和国内版
        print("\n" + "="*60)
        print("模式: 同时爬取海外版和国内版")
        print("="*60)

        # 先爬海外版
        results['overseas'] = run_single_version('overseas', username, password, config)

        # 再爬国内版
        results['china'] = run_single_version('china', username, password, config)

        # 汇总结果
        print("\n" + "="*60)
        print("爬取结果汇总")
        print("="*60)
        for ver, success in results.items():
            status = "✓ 成功" if success else "✗ 失败"
            print(f"  {ver}: {status}")
    else:
        # 单版本爬取
        results[version] = run_single_version(version, username, password, config)


if __name__ == "__main__":
    main()
