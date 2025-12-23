"""
XMP Data Scraper
从 https://xmp.mobvista.com 抓取账户数据
"""

import os
import sys
import json
import time
from datetime import datetime
from playwright.sync_api import sync_playwright, Page, Browser
from dotenv import load_dotenv
import pandas as pd

# 设置控制台编码为 UTF-8，避免 Windows 下的编码问题
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')


class XMPScraper:
    """XMP 数据抓取器"""

    def __init__(self, username: str, password: str, headless: bool = False, cookie_file: str = "xmp_cookies.json"):
        """
        初始化抓取器

        Args:
            username: 登录用户名
            password: 登录密码
            headless: 是否使用无头模式（不显示浏览器窗口）
            cookie_file: Cookie 保存文件路径
        """
        self.username = username
        self.password = password
        self.headless = headless
        self.cookie_file = cookie_file
        self.browser: Browser = None
        self.page: Page = None

    def start(self):
        """启动浏览器"""
        print("正在启动浏览器...")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        self.page = self.browser.new_page()
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

    def _upload_screenshot_to_gcs(self, local_path, gcs_path=None):
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
                gcs_path = f'xmp/screenshots/{os.path.basename(local_path)}'
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
            blob = bucket.blob('xmp/cookies/xmp_cookies.json')
            blob.upload_from_string(
                json.dumps(cookies, ensure_ascii=False, indent=2),
                content_type='application/json'
            )
            print(f"✓ Cookie 已保存到 GCS: gs://{gcs_bucket}/xmp/cookies/xmp_cookies.json")
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
            blob = bucket.blob('xmp/cookies/xmp_cookies.json')

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

    def login_with_cookies(self):
        """尝试使用 Cookie 登录"""
        print("尝试使用已保存的 Cookie 登录...")

        # 先加载 Cookie
        if not self.load_cookies():
            return False

        # 访问目标页面
        target_url = "https://xmp.mobvista.com/ads_manage/summary/material"
        print(f"正在访问: {target_url}")
        self.page.goto(target_url, timeout=60000)

        try:
            self.page.wait_for_load_state("networkidle", timeout=30000)
        except:
            pass
        time.sleep(3)

        # 检查是否登录成功
        current_url = self.page.url
        print(f"当前 URL: {current_url}")

        if "login" not in current_url.lower():
            print("✓ Cookie 登录成功！")
            return True
        else:
            print("✗ Cookie 已过期或无效，需要重新登录")
            return False

    def login(self):
        """登录 XMP 平台（优先使用 Cookie）"""
        print(f"正在登录 XMP 平台...")

        # 优先尝试 Cookie 登录（本地文件或 GCS）
        has_local_cookie = os.path.exists(self.cookie_file)
        has_gcs_cookie = bool(os.getenv('GCS_BUCKET_NAME'))

        if has_local_cookie or has_gcs_cookie:
            print(f"尝试 Cookie 登录 (本地: {has_local_cookie}, GCS: {has_gcs_cookie})")
            if self.login_with_cookies():
                return True
            print("Cookie 登录失败，尝试账号密码登录...")

        # 访问登录页面
        print("正在访问登录页面...")
        self.page.goto("https://xmp.mobvista.com/ads_manage/summary/account", timeout=60000)

        # 等待登录表单加载
        print("等待登录页面加载...")
        try:
            self.page.wait_for_load_state("networkidle", timeout=60000)
        except:
            print("页面加载超时，继续尝试...")
        time.sleep(3)  # 额外等待确保页面完全加载

        # 填写用户名和密码
        # 注意：这里的选择器需要根据实际页面结构调整
        print("填写登录信息...")
        try:
            # 尝试常见的登录表单选择器
            username_selectors = [
                'input[type="text"]',
                'input[name="username"]',
                'input[placeholder*="用户名"]',
                'input[placeholder*="邮箱"]',
                'input[placeholder*="email"]'
            ]

            password_selectors = [
                'input[type="password"]',
                'input[name="password"]',
                'input[placeholder*="密码"]'
            ]

            # 尝试找到用户名输入框
            username_filled = False
            for selector in username_selectors:
                try:
                    self.page.fill(selector, self.username, timeout=5000)
                    print(f"✓ 找到用户名输入框: {selector}")
                    username_filled = True
                    break
                except:
                    print(f"  尝试 {selector} 失败")
                    continue

            if not username_filled:
                print("✗ 未能找到用户名输入框！")
                self.page.screenshot(path="no_username_field.png")
                print("已保存截图到 no_username_field.png")
                # 打印页面上所有 input 元素帮助调试
                inputs = self.page.query_selector_all('input')
                print(f"页面上找到 {len(inputs)} 个 input 元素:")
                for inp in inputs:
                    inp_type = inp.get_attribute('type') or 'unknown'
                    inp_name = inp.get_attribute('name') or ''
                    inp_placeholder = inp.get_attribute('placeholder') or ''
                    print(f"  - type={inp_type}, name={inp_name}, placeholder={inp_placeholder}")
                return False

            # 尝试找到密码输入框
            password_filled = False
            for selector in password_selectors:
                try:
                    self.page.fill(selector, self.password, timeout=5000)
                    print(f"✓ 找到密码输入框: {selector}")
                    password_filled = True
                    break
                except:
                    print(f"  尝试 {selector} 失败")
                    continue

            if not password_filled:
                print("✗ 未能找到密码输入框！")
                self.page.screenshot(path="no_password_field.png")
                print("已保存截图到 no_password_field.png")
                return False

            # 点击登录按钮
            login_button_selectors = [
                'button:has-text("Log in")',
                'button.el-button--primary',
                'button[type="submit"]',
                'button:has-text("登录")',
                'button:has-text("Login")',
                'button:has-text("Sign")',
                '.login-button',
                '#login-button',
                'input[type="submit"]'
            ]

            login_clicked = False
            for selector in login_button_selectors:
                try:
                    self.page.click(selector, timeout=5000)
                    print(f"✓ 点击登录按钮: {selector}")
                    login_clicked = True
                    break
                except:
                    print(f"  尝试 {selector} 失败")
                    continue

            if not login_clicked:
                print("✗ 未能找到登录按钮！")
                self.page.screenshot(path="no_login_button.png")
                print("已保存截图到 no_login_button.png")
                # 打印页面上所有 button 元素帮助调试
                buttons = self.page.query_selector_all('button')
                print(f"页面上找到 {len(buttons)} 个 button 元素:")
                for btn in buttons:
                    btn_text = btn.inner_text().strip()[:50] if btn.inner_text() else ''
                    btn_class = btn.get_attribute('class') or ''
                    print(f"  - text={btn_text}, class={btn_class}")
                return False

            # 等待登录完成（等待页面跳转或特定元素出现）
            print("等待登录完成...")
            time.sleep(5)  # 先等待一段时间让登录请求完成
            try:
                # 等待 URL 变化，表示登录成功跳转
                self.page.wait_for_url("**/m/report/**", timeout=30000)
                print("检测到页面跳转")
            except:
                print("等待页面跳转超时，继续检查...")
            try:
                self.page.wait_for_load_state("networkidle", timeout=60000)
            except:
                print("登录后页面加载超时，继续检查...")
            time.sleep(5)  # 额外等待确保页面完全加载

            # 检查是否登录成功
            current_url = self.page.url
            print(f"当前 URL: {current_url}")

            # 保存登录后的截图用于调试
            self.page.screenshot(path="after_login.png")
            print("已保存登录后截图到 after_login.png")
            # 上传截图到 GCS（用于 Cloud Run 调试）
            self._upload_screenshot_to_gcs("after_login.png")

            # 检查是否登录成功：URL 不含 login，或者页面包含登录成功的元素
            login_success = False

            # 方法1: 检查 URL
            if "login" not in current_url.lower():
                print("✓ URL 检测：已离开登录页")
                login_success = True

            # 方法2: 检查页面是否有 Dashboard 元素（即使 URL 还是 login）
            if not login_success:
                print("URL 仍在登录页，检查页面内容...")
                dashboard_selectors = [
                    '.user-info',           # 用户信息区域
                    '.dashboard',           # Dashboard 容器
                    'text=Dashboard',       # Dashboard 文字
                    'text=Reports',         # Reports 菜单
                    '.ant-menu',            # Ant Design 菜单
                    '.sidebar',             # 侧边栏
                    'text=Sample Data Reports',  # 示例报告
                    'text=Getting Started',      # 新手引导
                ]
                for selector in dashboard_selectors:
                    try:
                        if self.page.locator(selector).first.is_visible(timeout=2000):
                            print(f"✓ 页面内容检测：找到 {selector}，登录成功")
                            login_success = True
                            break
                    except:
                        continue

            if login_success:
                print("登录成功！")

                # 保存 Cookie 供下次使用
                self.save_cookies()

                # 导航到目标页面
                target_url = "https://xmp.mobvista.com/ads_manage/summary/material"
                print(f"正在跳转到目标页面: {target_url}")
                self.page.goto(target_url, timeout=60000)
                try:
                    self.page.wait_for_load_state("networkidle", timeout=60000)
                except:
                    print("目标页面加载超时，继续尝试...")
                time.sleep(3)
                print(f"当前 URL: {self.page.url}")

                # 关闭新手引导弹窗（如果存在）
                self._close_onboarding_dialog()

                return True
            else:
                print("可能登录失败，请检查截图")
                return False

        except Exception as e:
            print(f"登录过程出错: {str(e)}")
            # 保存截图用于调试
            try:
                self.page.screenshot(path="login_error.png")
                print("已保存错误截图到 login_error.png")
            except:
                pass
            return False

    def _close_onboarding_dialog(self):
        """关闭新手引导弹窗（如果存在）"""
        try:
            print("检查是否有新手引导弹窗...")
            time.sleep(2)

            # 方法1: 尝试点击关闭按钮 (×)
            close_selectors = [
                '.ivu-modal-close',
                '.modal-close',
                'button.close',
                '.dialog-close',
                '[aria-label="Close"]',
                '.ivu-icon-ios-close',
            ]

            for selector in close_selectors:
                try:
                    close_btn = self.page.query_selector(selector)
                    if close_btn and close_btn.is_visible():
                        close_btn.click()
                        print(f"✓ 点击关闭按钮关闭弹窗: {selector}")
                        time.sleep(1)
                        return True
                except:
                    continue

            # 方法2: 点击 Next 直到完成（1 of 5 -> Get Started）
            max_steps = 10  # 最多点击10次防止死循环
            for step in range(max_steps):
                try:
                    # 查找 Next 按钮
                    next_btn = self.page.query_selector('button:has-text("Next")')
                    if next_btn and next_btn.is_visible():
                        next_btn.click()
                        print(f"  点击 Next 按钮 (步骤 {step + 1})")
                        time.sleep(0.5)
                        continue

                    # 查找 Get Started / 完成 / Done 按钮
                    finish_selectors = [
                        'button:has-text("Get Started")',
                        'button:has-text("Get started")',
                        'button:has-text("Done")',
                        'button:has-text("完成")',
                        'button:has-text("Finish")',
                        'button:has-text("Got it")',
                    ]

                    for selector in finish_selectors:
                        try:
                            finish_btn = self.page.query_selector(selector)
                            if finish_btn and finish_btn.is_visible():
                                finish_btn.click()
                                print(f"✓ 点击完成按钮关闭引导: {selector}")
                                time.sleep(1)
                                return True
                        except:
                            continue

                    # 没有找到任何按钮，退出循环
                    break

                except Exception as e:
                    break

            print("未检测到新手引导弹窗或已关闭")
            return False

        except Exception as e:
            print(f"关闭引导弹窗时出错: {e}")
            return False

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

    def _extract_from_api(self):
        """通过拦截 API 请求获取 JSON 数据，支持分页获取所有数据"""
        all_records = []
        all_pages_data = []  # 保存每一页的完整响应
        total_count = 0
        page = 1
        page_size = 100  # 每页数量，设置为100
        captured_data = []
        api_base_url = None  # 保存API基础URL

        def handle_response(response):
            """处理网络响应"""
            nonlocal captured_data, api_base_url
            try:
                url = response.url
                if any(keyword in url for keyword in ['channel/list', 'channel/summary', 'material/list']):
                    if response.status == 200:
                        content_type = response.headers.get('content-type', '')
                        if 'application/json' in content_type:
                            try:
                                json_data = response.json()
                                print(f"✓ 捕获到 API 响应: {url[:100]}...")
                                captured_data.append({
                                    'url': url,
                                    'data': json_data
                                })
                                # 保存API URL用于后续分页请求
                                if 'channel/list' in url and api_base_url is None:
                                    api_base_url = url
                            except:
                                pass
            except Exception as e:
                pass

        # 注册响应监听器
        self.page.on('response', handle_response)

        try:
            # 首先尝试通过页面设置每页显示更多数据
            print("尝试设置每页显示100条数据...")
            self._try_set_page_size_to_100()

            # 刷新页面以捕获 API 请求
            print("刷新页面以捕获 API 请求...")
            captured_data = []
            self.page.reload()

            try:
                self.page.wait_for_load_state("networkidle", timeout=30000)
            except:
                pass
            time.sleep(3)

            # 刷新后重新设置每页100条
            print("刷新后重新设置每页显示100条...")
            self._try_set_page_size_to_100()
            time.sleep(3)

            # 从第一页响应中获取总数和每页数量
            page_records, total_count, page_size = self._parse_api_response(captured_data)

            if page_records:
                print(f"第 1 页获取到 {len(page_records)} 条记录，总计 {total_count} 条")
                all_records.extend(page_records)
                # 保存第1页的完整响应
                all_pages_data.append({
                    "page": 1,
                    "page_size": page_size,
                    "list": page_records,
                    "count": len(page_records)
                })
            else:
                print("第一页未获取到数据")
                return None

            # 如果total_count为0，尝试从页面获取总数
            if total_count == 0:
                total_count = self._get_total_from_page()
                print(f"从页面获取到总数: {total_count}")

            # 如果还是0，假设有更多数据，继续翻页直到没有数据
            if total_count == 0:
                print("无法获取总数，将持续翻页直到无数据...")
                total_count = 999999  # 设置一个大数，靠实际数据判断结束

            # 计算总页数
            if page_size > 0:
                total_pages = (total_count + page_size - 1) // page_size
                print(f"预计 {total_pages} 页，每页 {page_size} 条")
            else:
                total_pages = 999

            # 获取剩余页面的数据
            consecutive_empty = 0  # 连续空页计数
            while len(all_records) < total_count:
                page += 1

                if page > total_pages:
                    break

                print(f"正在获取第 {page} 页数据... (已获取 {len(all_records)} 条)")

                # 清空捕获的数据
                captured_data = []

                # 使用 expect_response 等待 API 响应，带重试机制
                max_retries = 3
                page_success = False
                for retry in range(max_retries):
                    try:
                        with self.page.expect_response(
                            lambda resp: 'channel/list' in resp.url and resp.status == 200,
                            timeout=30000  # 增加到 30 秒
                        ) as response_info:
                            # 尝试点击下一页
                            clicked = self._click_next_page(page)
                            if not clicked:
                                print("无法翻页，停止获取")
                                break

                        # 获取响应数据
                        response = response_info.value
                        try:
                            json_data = response.json()
                            page_records, _, current_page_size = self._extract_from_response(json_data)
                            if page_records:
                                print(f"第 {page} 页获取到 {len(page_records)} 条记录")
                                all_records.extend(page_records)
                                # 保存该页的完整响应
                                all_pages_data.append({
                                    "page": page,
                                    "page_size": current_page_size,
                                    "list": page_records,
                                    "count": len(page_records)
                                })
                                consecutive_empty = 0
                                page_success = True
                                break  # 成功，跳出重试循环
                            else:
                                consecutive_empty += 1
                                print(f"第 {page} 页无数据 (连续 {consecutive_empty} 页)")
                                page_success = True
                                break
                        except Exception as e:
                            print(f"解析响应失败: {e}")
                            consecutive_empty += 1
                            page_success = True
                            break

                    except Exception as e:
                        print(f"等待API响应超时: {e} (重试 {retry + 1}/{max_retries})")
                        if retry < max_retries - 1:
                            time.sleep(3)  # 重试前等待
                            # 刷新页面状态
                            try:
                                self.page.wait_for_load_state("networkidle", timeout=10000)
                            except:
                                pass
                        continue

                # 如果所有重试都失败，使用备用方式
                if not page_success:
                    print(f"第 {page} 页所有重试失败，尝试备用方式")
                    time.sleep(3)
                    page_records, _, current_page_size = self._parse_api_response(captured_data)
                    if page_records:
                        print(f"第 {page} 页获取到 {len(page_records)} 条记录 (备用方式)")
                        all_records.extend(page_records)
                        all_pages_data.append({
                            "page": page,
                            "page_size": current_page_size,
                            "list": page_records,
                            "count": len(page_records)
                        })
                        consecutive_empty = 0
                    else:
                        consecutive_empty += 1
                        print(f"第 {page} 页无数据 (连续 {consecutive_empty} 页)")

                if consecutive_empty >= 3:
                    print("连续3页无数据，停止分页")
                    break

                # 检查是否到达最后一页
                if self._is_last_page():
                    print("已到达最后一页")
                    break

                # 安全限制
                if page > 500:
                    print("达到最大页数限制 (500)")
                    break

                time.sleep(1)  # 短暂等待避免请求过快

        finally:
            # 移除监听器
            try:
                self.page.remove_listener('response', handle_response)
            except:
                pass

        if all_records:
            print(f"✓ 共获取 {len(all_records)} 条记录")
            # 保存完整的响应结构（包含每一页的分页信息）
            full_response = {
                "code": 200,
                "data": {
                    "total": total_count if total_count != 999999 else len(all_records),
                    "total_pages": len(all_pages_data),
                    "fetched_records": len(all_records),
                    "pages": all_pages_data  # 每一页的完整数据
                },
                "business_code": 200,
                "message": "success"
            }
            with open("api_responses.json", "w", encoding="utf-8") as f:
                json.dump(full_response, f, ensure_ascii=False, indent=2)
            print("已保存完整响应数据到 api_responses.json")
            return all_records

        print("未能从 API 响应中提取到有效数据")
        return None

    def _try_set_page_size_to_100(self):
        """尝试设置每页显示100条数据"""
        try:
            time.sleep(3)
            # 等待分页组件加载
            try:
                self.page.wait_for_selector('.ivu-page', timeout=10000)
            except:
                pass

            # 查找分页选择器
            page_size_selectors = [
                '.ivu-page-options-sizer',
                '.ivu-select',
                '.el-pagination__sizes .el-input',
                '.ant-pagination-options-size-changer',
                'select[class*="page"]',
            ]

            for selector in page_size_selectors:
                try:
                    size_selector = self.page.query_selector(selector)
                    if size_selector:
                        size_selector.click()
                        time.sleep(0.5)

                        # 尝试选择100条选项
                        options_100 = [
                            '.ivu-select-item:has-text("100")',
                            '.el-select-dropdown__item:has-text("100")',
                            '.ant-select-item:has-text("100")',
                            'li:has-text("100 条/页")',
                            'li:has-text("100")',
                        ]

                        for opt in options_100:
                            try:
                                option = self.page.query_selector(opt)
                                if option:
                                    option.click()
                                    print("✓ 已设置每页显示100条")
                                    time.sleep(2)
                                    return True
                            except:
                                continue
                except:
                    continue

            print("未找到分页选择器，使用默认每页数量")
            return False
        except Exception as e:
            print(f"设置每页数量失败: {e}")
            return False

    def _get_total_from_page(self):
        """从页面元素获取总数"""
        try:
            # 常见的总数显示选择器
            total_selectors = [
                '.ivu-page-total',
                '.el-pagination__total',
                '.ant-pagination-total-text',
                'span:has-text("共")',
                '.total-count',
            ]

            for selector in total_selectors:
                try:
                    element = self.page.query_selector(selector)
                    if element:
                        text = element.inner_text()
                        # 提取数字
                        import re
                        numbers = re.findall(r'\d+', text)
                        if numbers:
                            total = int(numbers[0])
                            if total > 0:
                                return total
                except:
                    continue

            return 0
        except:
            return 0

    def _parse_api_response(self, captured_data):
        """解析 API 响应，提取记录、总数和每页数量"""
        page_records = []
        total_count = 0
        page_size = 20

        # 优先匹配的 API 路径
        priority_apis = ['material/list', 'admanage/channel/list', 'account/list']

        # 先尝试优先 API
        for api_path in priority_apis:
            for item in captured_data:
                if api_path in item['url']:
                    records, total, size = self._extract_from_response(item['data'])
                    if records:
                        return records, total, size

        # 如果优先 API 没有数据，尝试所有响应
        for item in captured_data:
            records, total, size = self._extract_from_response(item['data'])
            if records:
                return records, total, size

        return page_records, total_count, page_size

    def _extract_from_response(self, data):
        """从单个响应中提取数据"""
        if not isinstance(data, dict):
            return [], 0, 20

        # 尝试不同的数据结构
        nested = data.get('data', data)

        if isinstance(nested, dict):
            records = nested.get('list', nested.get('items', nested.get('records', [])))
            total = nested.get('total', nested.get('total_count', nested.get('totalCount', 0)))
            page_size = nested.get('page_size', nested.get('pageSize', nested.get('size', 20)))

            if records and isinstance(records, list):
                return records, total, page_size

        return [], 0, 20

    def _click_next_page(self, target_page):
        """点击下一页按钮"""
        try:
            # 下一页按钮选择器列表
            next_btn_selectors = [
                '.ivu-page-next:not(.ivu-page-disabled)',
                '.el-pagination .btn-next:not([disabled])',
                'button.next:not([disabled])',
                'button:has-text("下一页"):not([disabled])',
                '.pagination-next:not(.disabled)',
                'a.next:not(.disabled)',
                'li.next:not(.disabled) a',
                '.ant-pagination-next:not(.ant-pagination-disabled)',
            ]

            for selector in next_btn_selectors:
                try:
                    next_btn = self.page.query_selector(selector)
                    if next_btn:
                        next_btn.click()
                        print(f"  点击下一页按钮: {selector}")
                        return True
                except:
                    continue

            # 尝试直接点击页码
            page_num_selectors = [
                f'.ivu-page-item:has-text("{target_page}")',
                f'.el-pager li:has-text("{target_page}")',
                f'.ant-pagination-item-{target_page}',
                f'a[data-page="{target_page}"]',
            ]

            for selector in page_num_selectors:
                try:
                    page_btn = self.page.query_selector(selector)
                    if page_btn:
                        page_btn.click()
                        print(f"  点击页码 {target_page}")
                        return True
                except:
                    continue

            return False
        except Exception as e:
            print(f"点击下一页失败: {e}")
            return False

    def _is_last_page(self):
        """检查是否到达最后一页"""
        last_page_indicators = [
            '.ivu-page-next.ivu-page-disabled',
            '.el-pagination .btn-next[disabled]',
            'button.next[disabled]',
            '.pagination-next.disabled',
            '.ant-pagination-next.ant-pagination-disabled',
        ]

        for selector in last_page_indicators:
            try:
                if self.page.query_selector(selector):
                    return True
            except:
                continue

        return False

    def save_data(self, data, format='csv'):
        """
        保存数据到文件

        Args:
            data: 要保存的数据
            format: 保存格式 ('csv', 'json', 'excel')
        """
        if not data:
            print("没有数据可保存")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if format == 'csv':
            filename = f"xmp_data_{timestamp}.csv"
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"✓ 数据已保存到 {filename}")

        elif format == 'json':
            filename = f"xmp_data_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"✓ 数据已保存到 {filename}")

        elif format == 'excel':
            filename = f"xmp_data_{timestamp}.xlsx"
            df = pd.DataFrame(data)
            df.to_excel(filename, index=False)
            print(f"✓ 数据已保存到 {filename}")

    def run(self, save_format='csv', upload_to_gcs=False, gcs_bucket=None,
            upload_to_bq=False, bq_project=None, bq_dataset=None):
        """
        运行完整的抓取流程

        Args:
            save_format: 保存格式 ('csv', 'json', 'excel')
            upload_to_gcs: 是否上传到 GCS
            gcs_bucket: GCS bucket 名称
            upload_to_bq: 是否上传到 BigQuery
            bq_project: BigQuery 项目 ID
            bq_dataset: BigQuery 数据集 ID
        """
        try:
            self.start()

            if self.login():
                data = self.extract_data()
                if data:
                    self.save_data(data, format=save_format)

                    # 生成批次 ID（时间戳格式）
                    batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
                    print(f"\n=== 批次 ID: {batch_id} ===")

                    # 上传到 GCS
                    if upload_to_gcs and gcs_bucket:
                        self._upload_to_gcs(gcs_bucket, batch_id)

                    # 上传到 BigQuery
                    if upload_to_bq and bq_project and bq_dataset:
                        self._upload_to_bigquery(bq_project, bq_dataset, batch_id)
                else:
                    print("⚠ 未能提取到数据，请检查页面结构")
            else:
                print("⚠ 登录失败，无法继续")

        except Exception as e:
            print(f"✗ 运行出错: {str(e)}")

        finally:
            self.stop()

    def _upload_to_gcs(self, bucket_name: str, batch_id: str = None):
        """上传 api_responses.json 到 GCS"""
        try:
            from gcs_storage import GCSUploader

            # 读取本地 JSON 文件
            with open("api_responses.json", "r", encoding="utf-8") as f:
                data = json.load(f)

            # 上传到 GCS（使用批次 ID）
            uploader = GCSUploader(bucket_name)
            blob_path = uploader.generate_xmp_blob_path("material", batch_id=batch_id)
            uploader.upload_json(data, blob_path)
            print(f"✓ 已上传 JSON 数据到 GCS: gs://{bucket_name}/{blob_path}")

        except ImportError:
            print("⚠ 未安装 google-cloud-storage，跳过 GCS 上传")
        except Exception as e:
            import traceback
            print(f"✗ GCS 上传失败: {e}")
            traceback.print_exc()

    def _upload_to_bigquery(self, project_id: str, dataset_id: str, batch_id: str = None):
        """上传数据到 BigQuery（追加模式）"""
        try:
            from bigquery_storage import BigQueryUploader

            # 读取本地 JSON 文件
            with open("api_responses.json", "r", encoding="utf-8") as f:
                data = json.load(f)

            # 上传到 BigQuery（追加到固定表，用 batch_id 字段区分批次）
            uploader = BigQueryUploader(project_id, dataset_id)
            count = uploader.upload_xmp_materials(data, batch_id=batch_id)
            print(f"✓ 已追加 {count} 条记录到 BigQuery")
            print(f"  表: {dataset_id}.xmp_materials")
            if batch_id:
                print(f"  批次 ID: {batch_id}")

        except ImportError:
            print("⚠ 未安装 google-cloud-bigquery，跳过 BigQuery 上传")
        except Exception as e:
            print(f"✗ BigQuery 上传失败: {e}")


def save_cookies_manually():
    """手动登录并保存 Cookie 的模式"""
    load_dotenv()

    username = os.getenv('XMP_USERNAME', '')
    password = os.getenv('XMP_PASSWORD', '')

    scraper = XMPScraper(
        username=username,
        password=password,
        headless=False  # 必须显示浏览器
    )

    try:
        scraper.start()

        # 访问登录页面
        print(f"正在打开 XMP 平台...")
        scraper.page.goto("https://xmp.mobvista.com/m/login", timeout=60000)

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

        if "login" not in current_url.lower():
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


def main():
    """主函数"""
    # 加载环境变量
    load_dotenv()

    # 检查命令行参数
    if len(sys.argv) > 1 and sys.argv[1] == '--save-cookies':
        save_cookies_manually()
        return

    # 从环境变量获取登录信息
    username = os.getenv('XMP_USERNAME')
    password = os.getenv('XMP_PASSWORD')

    if not username or not password:
        print("错误: 请在 .env 文件中设置 XMP_USERNAME 和 XMP_PASSWORD")
        return

    # GCS 配置
    gcs_bucket = os.getenv('GCS_BUCKET_NAME')
    upload_to_gcs = bool(gcs_bucket)

    # BigQuery 配置
    bq_project = os.getenv('BQ_PROJECT_ID')
    bq_dataset = os.getenv('BQ_DATASET_ID')
    upload_to_bq = bool(bq_project and bq_dataset)

    # 检测是否在 Cloud Run 环境（无显示器）
    is_cloud_run = os.getenv('K_SERVICE') is not None or os.getenv('CLOUD_RUN', 'false').lower() == 'true'

    # 创建抓取器实例
    scraper = XMPScraper(
        username=username,
        password=password,
        headless=is_cloud_run  # Cloud Run 环境使用 headless 模式
    )

    # 运行抓取
    scraper.run(
        save_format='csv',
        upload_to_gcs=upload_to_gcs,
        gcs_bucket=gcs_bucket,
        upload_to_bq=upload_to_bq,
        bq_project=bq_project,
        bq_dataset=bq_dataset
    )


if __name__ == "__main__":
    main()
