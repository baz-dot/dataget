#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
交互式搜索工具 - 手动输入剧名搜索，自动抓取 JSON 并分析
"""
import logging
import json
import os
import time
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright, Page, Route
from typing import Dict, List, Any
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InteractiveSearchTool:
    """交互式搜索工具 - 监听 API 并自动抓取数据"""

    def __init__(self, headless: bool = False):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.page = None
        self.captured_data = {}  # 存储抓取的数据 {drama_name: json_data}
        self.current_search_keyword = None
        self.data_dir = Path("captured_data")
        self.data_dir.mkdir(exist_ok=True)

    def start(self):
        """启动浏览器"""
        logger.info("正在启动浏览器...")
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

        # 设置 API 拦截器
        self._setup_api_interceptor()

        logger.info("✓ 浏览器启动成功")

    def _setup_api_interceptor(self):
        """设置 API 拦截器，监听搜索请求"""

        def handle_route(route: Route):
            """处理 API 请求"""
            request = route.request

            # 继续请求
            response = route.fetch()

            # 检查是否是搜索 API
            if 'searchCreative' in request.url or 'search' in request.url.lower():
                try:
                    # 获取响应数据
                    body = response.json()

                    # 保存数据
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

                    # 尝试从请求中提取搜索关键词
                    search_keyword = self._extract_search_keyword(request)

                    if search_keyword:
                        logger.info(f"✓ 捕获到搜索结果: {search_keyword}")

                        # 保存到内存
                        if search_keyword not in self.captured_data:
                            self.captured_data[search_keyword] = []
                        self.captured_data[search_keyword].append(body)

                        # 保存到文件
                        filename = f"{search_keyword}_{timestamp}.json"
                        filepath = self.data_dir / filename
                        with open(filepath, 'w', encoding='utf-8') as f:
                            json.dump(body, f, ensure_ascii=False, indent=2)

                        logger.info(f"✓ 数据已保存: {filepath}")

                        # 显示数据统计
                        self._show_data_stats(body, search_keyword)

                except Exception as e:
                    logger.debug(f"处理响应时出错: {e}")

            # 返回响应
            route.fulfill(response=response)

        # 拦截所有 API 请求
        self.page.route("**/api/**", handle_route)
        self.page.route("**/*search*", handle_route)

    def _extract_search_keyword(self, request) -> str:
        """从请求中提取搜索关键词"""
        try:
            # 尝试从 POST body 中提取
            if request.method == "POST":
                post_data = request.post_data
                if post_data:
                    try:
                        data = json.loads(post_data)
                        # 常见的搜索关键词字段
                        for key in ['keyword', 'searchKeyword', 'query', 'q', 'name', 'title']:
                            if key in data:
                                return str(data[key]).strip()
                    except:
                        pass

            # 尝试从 URL 参数中提取
            url = request.url
            if '?' in url:
                params = url.split('?')[1]
                for param in params.split('&'):
                    if '=' in param:
                        key, value = param.split('=', 1)
                        if key.lower() in ['keyword', 'q', 'query', 'search', 'name']:
                            return value.strip()

            # 如果无法提取，使用时间戳
            return f"search_{datetime.now().strftime('%H%M%S')}"

        except Exception as e:
            logger.debug(f"提取搜索关键词失败: {e}")
            return f"search_{datetime.now().strftime('%H%M%S')}"

    def _show_data_stats(self, data: Dict, keyword: str):
        """显示数据统计信息"""
        try:
            total_records = 0
            creative_count = 0

            # 尝试提取统计信息
            if 'page' in data:
                total_records = data['page'].get('totalRecords', 0)

            if 'content' in data and 'searchList' in data['content']:
                creative_count = len(data['content']['searchList'])

            logger.info(f"  └─ 关键词: {keyword}")
            logger.info(f"  └─ 总记录数: {total_records}")
            logger.info(f"  └─ 本页素材数: {creative_count}")

        except Exception as e:
            logger.debug(f"显示统计信息失败: {e}")

    def load_cookies(self):
        """加载已保存的 Cookie"""
        cookie_file = Path("dataeye_cookies.json")
        if cookie_file.exists():
            try:
                with open(cookie_file, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                    self.page.context.add_cookies(cookies)
                logger.info("✓ Cookie 加载成功")
                return True
            except Exception as e:
                logger.warning(f"Cookie 加载失败: {e}")
                return False
        else:
            logger.warning("未找到 Cookie 文件，需要手动登录")
            return False

    def save_cookies(self):
        """保存 Cookie"""
        try:
            cookies = self.page.context.cookies()
            with open("dataeye_cookies.json", 'w', encoding='utf-8') as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)
            logger.info("✓ Cookie 已保存")
        except Exception as e:
            logger.error(f"保存 Cookie 失败: {e}")

    def open_search_page(self, url: str = None):
        """打开搜索页面"""
        if url is None:
            url = "https://oversea-v2.dataeye.com/playlet/playlet-material"

        logger.info(f"正在打开页面: {url}")
        self.page.goto(url, timeout=60000)

        # 尝试加载 Cookie
        self.load_cookies()

        # 刷新页面以应用 Cookie
        if Path("dataeye_cookies.json").exists():
            self.page.reload()

        logger.info("✓ 页面已打开")
        logger.info("=" * 60)
        logger.info("现在你可以在浏览器中手动搜索剧名了")
        logger.info("程序会自动捕获 API 返回的 JSON 数据")
        logger.info("=" * 60)

    def wait_for_user_input(self):
        """等待用户输入命令"""
        logger.info("\n可用命令:")
        logger.info("  - 直接在浏览器中搜索剧名（程序会自动捕获数据）")
        logger.info("  - 输入 'status' 查看已捕获的数据")
        logger.info("  - 输入 'analyze' 分析所有数据并生成报告")
        logger.info("  - 输入 'save' 保存当前 Cookie")
        logger.info("  - 输入 'quit' 退出程序")
        logger.info("")

        while True:
            try:
                command = input("请输入命令 (或直接在浏览器中搜索): ").strip().lower()

                if command == 'quit':
                    logger.info("正在退出...")
                    return False

                elif command == 'status':
                    self._show_captured_status()

                elif command == 'analyze':
                    self._analyze_all_data()

                elif command == 'save':
                    self.save_cookies()

                elif command == '':
                    # 空命令，继续等待
                    time.sleep(1)

                else:
                    logger.info(f"未知命令: {command}")

            except KeyboardInterrupt:
                logger.info("\n用户中断程序")
                return False
            except Exception as e:
                logger.error(f"处理命令时出错: {e}")

    def _show_captured_status(self):
        """显示已捕获的数据状态"""
        logger.info("\n" + "=" * 60)
        logger.info("已捕获的数据:")
        logger.info("=" * 60)

        if not self.captured_data:
            logger.info("  (暂无数据)")
        else:
            for idx, (keyword, data_list) in enumerate(self.captured_data.items(), 1):
                logger.info(f"{idx}. {keyword} - {len(data_list)} 个响应")

        logger.info("=" * 60 + "\n")

    def _analyze_all_data(self):
        """分析所有捕获的数据并生成报告"""
        if not self.captured_data:
            logger.warning("没有可分析的数据")
            return

        logger.info("\n开始分析数据...")

        try:
            from manual_data_processor import ManualDataProcessor
            from excel_generator import ExcelGenerator

            processor = ManualDataProcessor()
            all_drama_results = {}

            # 处理每个剧的数据
            for drama_name, data_list in self.captured_data.items():
                logger.info(f"正在分析: {drama_name}")

                # 合并所有响应的素材数据
                all_creatives = []
                for data in data_list:
                    if 'content' in data and 'searchList' in data['content']:
                        all_creatives.extend(data['content']['searchList'])

                # 聚合数据
                aggregated = processor.aggregate_drama_data(all_creatives)
                all_drama_results[drama_name] = aggregated

                logger.info(f"  ✓ {drama_name}: {len(all_creatives)} 个素材")

            # 生成 Excel 报告
            output_file = f"市场潜力分析报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            generator = ExcelGenerator()
            generator.generate_report(all_drama_results, output_file)

            logger.info(f"\n✓ 报告已生成: {output_file}")

        except Exception as e:
            logger.error(f"分析数据时出错: {e}")
            import traceback
            traceback.print_exc()

    def close(self):
        """关闭浏览器"""
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        logger.info("浏览器已关闭")


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("DataEye 交互式搜索工具")
    logger.info("=" * 60)
    logger.info("功能: 手动搜索剧名，自动抓取 JSON 并分析")
    logger.info("=" * 60)

    tool = InteractiveSearchTool(headless=False)

    try:
        # 启动浏览器
        tool.start()

        # 打开搜索页面
        tool.open_search_page()

        # 等待用户操作
        tool.wait_for_user_input()

    except KeyboardInterrupt:
        logger.info("\n用户中断程序")
    except Exception as e:
        logger.error(f"程序出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        tool.close()


if __name__ == "__main__":
    main()
