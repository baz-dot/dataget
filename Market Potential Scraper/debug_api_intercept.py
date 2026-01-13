"""
调试 API 拦截功能
测试 DataEye 平台的 API 请求拦截
"""

import os
import json
import time
import logging
from datetime import datetime
from playwright.sync_api import sync_playwright

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def debug_api_intercept(drama_name: str = "天降萌宝老祖，孝子贤孙都跪下"):
    """
    调试 API 拦截功能

    Args:
        drama_name: 测试用的剧名
    """
    logger.info("="*60)
    logger.info("开始调试 API 拦截功能")
    logger.info("="*60)

    # 用于存储所有拦截到的请求
    all_requests = []
    all_responses = []

    def handle_request(request):
        """记录所有请求"""
        url = request.url
        method = request.method

        # 记录所有请求
        all_requests.append({
            "url": url,
            "method": method,
            "timestamp": datetime.now().isoformat()
        })

        # 高亮显示可能的 API 请求
        if any(keyword in url.lower() for keyword in ['api', 'search', 'creative', 'playlet', 'material']):
            logger.info(f"🔍 发现可能的 API 请求: {method} {url}")

    def handle_response(response):
        """记录所有响应"""
        url = response.url
        status = response.status

        # 记录所有响应
        all_responses.append({
            "url": url,
            "status": status,
            "timestamp": datetime.now().isoformat()
        })

        # 高亮显示可能的 API 响应
        if any(keyword in url.lower() for keyword in ['api', 'search', 'creative', 'playlet', 'material']):
            logger.info(f"✓ 收到 API 响应: {status} {url}")

            # 尝试解析 JSON
            try:
                if response.status == 200:
                    json_data = response.json()
                    logger.info(f"📦 JSON 数据结构: {list(json_data.keys())}")

                    # 保存完整的 API 响应
                    output_file = f"debug_api_response_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, indent=2, ensure_ascii=False)
                    logger.info(f"💾 已保存 API 响应到: {output_file}")
            except Exception as e:
                logger.debug(f"无法解析 JSON: {e}")

    try:
        with sync_playwright() as p:
            # 启动浏览器
            logger.info("启动浏览器...")
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()

            # 设置请求和响应监听器
            page.on("request", handle_request)
            page.on("response", handle_response)

            # 加载 Cookie
            cookie_file = 'dataeye_cookies.json'
            if os.path.exists(cookie_file):
                logger.info("加载 Cookie...")
                with open(cookie_file, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                context.add_cookies(cookies)

            # 访问页面
            logger.info("访问 DataEye 平台...")
            page.goto('https://oversea-v2.dataeye.com/playlet/playlet-material', timeout=60000)
            time.sleep(3)

            # 点击海外短剧版本
            logger.info("点击海外短剧版本...")
            try:
                overseas_btn = page.wait_for_selector('text="海外短剧版本"', timeout=5000)
                if overseas_btn:
                    overseas_btn.click()
                    time.sleep(2)
            except:
                logger.warning("未找到海外短剧版本按钮")

            # 输入搜索关键词
            logger.info(f"输入搜索关键词: {drama_name}")
            search_inputs = page.query_selector_all('input[placeholder*="搜索"]')
            logger.info(f"找到 {len(search_inputs)} 个搜索框")

            if len(search_inputs) >= 2:
                search_input = search_inputs[1]
                search_input.click()
                search_input.fill('')
                time.sleep(0.5)
                search_input.type(drama_name, delay=100)
                logger.info("✓ 已输入搜索关键词")
                time.sleep(1)

            # 触发搜索
            logger.info("触发搜索...")
            page.keyboard.press('Enter')
            time.sleep(1)

            # 等待更长时间以捕获所有 API 请求
            logger.info("等待 API 响应 (10秒)...")
            time.sleep(10)

            # 保存所有请求和响应
            logger.info("="*60)
            logger.info("调试结果汇总")
            logger.info("="*60)
            logger.info(f"总请求数: {len(all_requests)}")
            logger.info(f"总响应数: {len(all_responses)}")

            # 保存详细日志
            debug_log = {
                "drama_name": drama_name,
                "total_requests": len(all_requests),
                "total_responses": len(all_responses),
                "requests": all_requests,
                "responses": all_responses
            }

            log_file = f"debug_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(debug_log, f, indent=2, ensure_ascii=False)
            logger.info(f"✓ 调试日志已保存: {log_file}")

            # 筛选出包含关键词的 URL
            api_urls = [r['url'] for r in all_requests if any(k in r['url'].lower() for k in ['api', 'search', 'creative'])]
            if api_urls:
                logger.info("\n发现的 API URL:")
                for url in api_urls[:10]:
                    logger.info(f"  - {url}")
            else:
                logger.warning("⚠️ 未发现任何 API 请求!")

            logger.info("\n按任意键关闭浏览器...")
            input()

            browser.close()

    except Exception as e:
        logger.error(f"调试过程出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    debug_api_intercept()
