"""
调试翻页按钮 - 检查页面结构和滚动策略
"""
import time
from dataeye_scraper import DataEyeScraper, logger

logger.info("="*60)
logger.info("翻页按钮调试工具")
logger.info("="*60)

scraper = DataEyeScraper(headless=False)
scraper.start()

# 登录
logger.info("步骤 1: 登录...")
if not scraper.login():
    logger.error("登录失败")
    scraper.close()
    exit(1)

# 访问页面并搜索
logger.info("步骤 2: 搜索短剧...")
scraper.page.goto(scraper.target_url, timeout=60000)
time.sleep(3)

# 输入搜索关键词
test_drama = "天降萌宝老祖，孝子贤孙都跪下"
scraper._click_overseas_version()
scraper._input_search_keyword(test_drama)

# 点击搜索
logger.info("步骤 3: 点击搜索...")
scraper.page.keyboard.press('Enter')
time.sleep(5)

logger.info("步骤 4: 检查页面结构...")

# 尝试多种滚动策略
logger.info("\n测试滚动策略 1: 滚动到页面底部")
scraper.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
time.sleep(2)

logger.info("测试滚动策略 2: 多次向下滚动")
for i in range(5):
    scraper.page.evaluate("window.scrollBy(0, 500)")
    time.sleep(0.5)

logger.info("测试滚动策略 3: 查找并滚动到分页组件")
try:
    pagination = scraper.page.locator('.ant-pagination').first
    if pagination.is_visible(timeout=5000):
        pagination.scroll_into_view_if_needed()
        logger.info("✓ 找到分页组件并滚动到位")
        time.sleep(2)
except Exception as e:
    logger.warning(f"未找到分页组件: {e}")

# 检查页面上的所有翻页相关元素
logger.info("\n步骤 5: 检查翻页按钮...")

# 检查所有可能的翻页按钮选择器
selectors_to_test = [
    ('下一页按钮 (li.ant-pagination-next)', 'li.ant-pagination-next'),
    ('下一页按钮 (不含disabled)', 'li.ant-pagination-next:not(.ant-pagination-disabled)'),
    ('页码按钮 (li.ant-pagination-item)', 'li.ant-pagination-item'),
    ('分页容器', '.ant-pagination'),
    ('所有按钮', 'button'),
]

for name, selector in selectors_to_test:
    try:
        elements = scraper.page.locator(selector).all()
        logger.info(f"✓ {name}: 找到 {len(elements)} 个元素")
        if len(elements) > 0:
            # 检查第一个元素是否可见
            first = scraper.page.locator(selector).first
            is_visible = first.is_visible(timeout=2000)
            logger.info(f"  - 第一个元素可见: {is_visible}")
    except Exception as e:
        logger.warning(f"✗ {name}: {e}")

logger.info("\n步骤 6: 尝试点击下一页...")
time.sleep(2)

# 尝试点击下一页按钮
next_selectors = [
    'li.ant-pagination-next:not(.ant-pagination-disabled)',
    'li.ant-pagination-next button',
    '.ant-pagination-next',
]

clicked = False
for selector in next_selectors:
    try:
        element = scraper.page.locator(selector).first
        if element.is_visible(timeout=3000):
            logger.info(f"✓ 找到下一页按钮: {selector}")
            element.scroll_into_view_if_needed()
            time.sleep(1)
            element.click()
            logger.info("✓ 点击成功！")
            clicked = True
            break
    except Exception as e:
        logger.debug(f"选择器 {selector} 失败: {e}")

if not clicked:
    logger.error("✗ 无法点击下一页按钮")

logger.info("\n浏览器将保持打开状态，请手动检查...")
logger.info("按 Ctrl+C 退出")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    logger.info("\n关闭浏览器...")
    scraper.close()
