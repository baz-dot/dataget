"""
改进的翻页方法 - 使用 JavaScript 直接操作
"""

def _click_page_number_v2(self, page_num: int) -> bool:
    """
    使用 JavaScript 直接触发翻页（不依赖滚动和点击）

    Args:
        page_num: 页码

    Returns:
        是否点击成功
    """
    try:
        logger.info(f"尝试翻到第 {page_num} 页...")

        # 方法1: 使用 JavaScript 直接点击下一页按钮
        js_click_next = """
        const nextButton = document.querySelector('li.ant-pagination-next:not(.ant-pagination-disabled)');
        if (nextButton) {
            nextButton.click();
            return true;
        }
        return false;
        """

        result = self.page.evaluate(js_click_next)
        if result:
            logger.info(f"✓ JavaScript 点击下一页成功（页码 {page_num}）")
            time.sleep(2)
            return True

        # 方法2: 查找并点击具体页码
        js_click_page = f"""
        const pageItems = document.querySelectorAll('li.ant-pagination-item');
        for (let item of pageItems) {{
            if (item.textContent.trim() === '{page_num}') {{
                item.click();
                return true;
            }}
        }}
        return false;
        """

        result = self.page.evaluate(js_click_page)
        if result:
            logger.info(f"✓ JavaScript 点击页码 {page_num} 成功")
            time.sleep(2)
            return True

        logger.warning(f"无法点击页码 {page_num}")
        return False

    except Exception as e:
        logger.error(f"点击页码 {page_num} 失败: {e}")
        return False
