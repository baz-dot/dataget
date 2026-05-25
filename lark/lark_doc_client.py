"""
飞书文档 API 客户端
用于将周报/日报数据写入飞书文档
"""

import requests
import time
import os
from typing import Optional, Dict, Any, List

# 导入日志模块
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
try:
    from utils.logger import get_logger
except ImportError:
    class SimpleLogger:
        def info(self, msg): print(f"[INFO] {msg}")
        def warning(self, msg): print(f"[WARNING] {msg}")
        def error(self, msg): print(f"[ERROR] {msg}")
        def debug(self, msg): pass
    def get_logger(name): return SimpleLogger()

logger = get_logger("dataget.lark_doc")

# 团队分组配置 (与 lark_bot.py 保持一致)
CN_TEAM = ["kimi", "silas", "zane", "kino", "hannibal", "bigzo", "echo", "felix", "jocelyn", "ponyo", "lulu", "alvin", "troy", "kendon"]
KR_TEAM = ["juria", "jade", "lyla", "joy", "heida", "john", "robin"]

def get_optimizer_team(optimizer_name: str) -> str:
    """获取投手所属团队"""
    name_lower = optimizer_name.lower() if optimizer_name else ""
    if name_lower in [n.lower() for n in CN_TEAM]:
        return "CN"
    elif name_lower in [n.lower() for n in KR_TEAM]:
        return "KR"
    return "Other"


class LarkDocClient:
    """飞书文档 API 客户端"""

    # API 基础地址
    BASE_URL = "https://open.feishu.cn/open-apis"
    # 国际版使用 larksuite
    BASE_URL_INTL = "https://open.larksuite.com/open-apis"

    def __init__(self, app_id: str, app_secret: str, use_intl: bool = True):
        """
        初始化飞书文档客户端

        Args:
            app_id: 飞书应用 App ID
            app_secret: 飞书应用 App Secret
            use_intl: 是否使用国际版 API (larksuite)
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.base_url = self.BASE_URL_INTL if use_intl else self.BASE_URL
        self.access_token = None
        self.token_expire_time = 0

    def _get_tenant_access_token(self) -> str:
        """获取 tenant_access_token"""
        # 检查 token 是否过期
        if self.access_token and time.time() < self.token_expire_time - 60:
            return self.access_token

        url = f"{self.base_url}/auth/v3/tenant_access_token/internal"
        resp = requests.post(url, json={
            "app_id": self.app_id,
            "app_secret": self.app_secret
        })

        data = resp.json()
        if data.get("code") != 0:
            logger.error(f"获取 access_token 失败: {data}")
            raise Exception(f"获取 access_token 失败: {data.get('msg')}")

        self.access_token = data.get("tenant_access_token")
        # token 有效期 2 小时
        self.token_expire_time = time.time() + data.get("expire", 7200)
        logger.info("获取 tenant_access_token 成功")
        return self.access_token

    def _get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        token = self._get_tenant_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def get_document_info(self, doc_token: str) -> Dict:
        """获取文档信息"""
        url = f"{self.base_url}/docx/v1/documents/{doc_token}"
        resp = requests.get(url, headers=self._get_headers())
        return resp.json()

    # ============ Wiki API 方法 ============

    def get_wiki_node_info(self, wiki_token: str) -> Dict:
        """获取 Wiki 节点信息，返回实际的 obj_token 和 obj_type"""
        url = f"{self.base_url}/wiki/v2/spaces/get_node"
        params = {"token": wiki_token}
        resp = requests.get(url, headers=self._get_headers(), params=params)
        return resp.json()

    def write_to_wiki(self, wiki_token: str, report_data: Dict) -> Dict:
        """
        将周报数据写入 Wiki 页面

        Wiki 页面实际上是一个文档的引用，需要先获取实际的 doc_token
        """
        # 1. 获取 Wiki 节点信息
        node_info = self.get_wiki_node_info(wiki_token)
        if node_info.get("code") != 0:
            logger.error(f"获取 Wiki 节点信息失败: {node_info}")
            return node_info

        # 2. 获取实际的文档 token
        node = node_info.get("data", {}).get("node", {})
        obj_token = node.get("obj_token")
        obj_type = node.get("obj_type")

        logger.info(f"Wiki 节点: obj_token={obj_token}, obj_type={obj_type}")

        if obj_type != "docx":
            logger.error(f"Wiki 节点类型不是文档: {obj_type}")
            return {"code": -1, "msg": f"不支持的节点类型: {obj_type}"}

        # 3. 使用实际的 doc_token 写入内容
        return self.write_weekly_report(obj_token, report_data)

    def get_block_children(self, doc_token: str, block_id: str) -> Dict:
        """获取指定块的子块"""
        url = f"{self.base_url}/docx/v1/documents/{doc_token}/blocks/{block_id}/children"
        resp = requests.get(url, headers=self._get_headers())
        return resp.json()

    def get_document_blocks(self, doc_token: str) -> Dict:
        """获取文档所有块"""
        url = f"{self.base_url}/docx/v1/documents/{doc_token}/blocks"
        resp = requests.get(url, headers=self._get_headers())
        return resp.json()

    def create_block(self, doc_token: str, block_id: str, children: List[Dict], index: int = -1) -> Dict:
        """
        在指定块下创建子块

        Args:
            doc_token: 文档 token
            block_id: 父块 ID (文档根块 ID 等于 doc_token)
            children: 子块列表
            index: 插入位置，-1 表示末尾
        """
        import time
        url = f"{self.base_url}/docx/v1/documents/{doc_token}/blocks/{block_id}/children"
        payload = {"children": children}
        if index >= 0:
            payload["index"] = index

        # 重试机制
        for attempt in range(3):
            resp = requests.post(url, headers=self._get_headers(), json=payload)
            if resp.status_code == 429:
                time.sleep(1.0 * (attempt + 1))
                continue
            time.sleep(0.3)  # 请求间隔
            try:
                return resp.json()
            except Exception as e:
                logger.warning(f"create_block 解析失败: {e}, status={resp.status_code}")
                return {"code": -1, "msg": str(e)}
        return {"code": -1, "msg": "rate limited"}

    # ============ 块构建辅助方法 ============

    @staticmethod
    def _text_element(content: str, bold: bool = False) -> Dict:
        """构建文本元素"""
        element = {
            "text_run": {
                "content": content,
                "text_element_style": {}
            }
        }
        if bold:
            element["text_run"]["text_element_style"]["bold"] = True
        return element

    @staticmethod
    def _build_text_block(content: str, bold: bool = False) -> Dict:
        """构建文本块"""
        return {
            "block_type": 2,  # text
            "text": {
                "elements": [LarkDocClient._text_element(content, bold)]
            }
        }

    @staticmethod
    def _build_heading_block(content: str, level: int = 1) -> Dict:
        """构建标题块 (level: 1-9)"""
        # heading1=3, heading2=4, ..., heading9=11
        block_type = 2 + level
        return {
            "block_type": block_type,
            f"heading{level}": {
                "elements": [LarkDocClient._text_element(content, bold=True)]
            }
        }

    @staticmethod
    def _build_divider_block() -> Dict:
        """构建分割线块"""
        return {"block_type": 22, "divider": {}}

    @staticmethod
    def _build_callout_block(content: str, emoji: str = "📊") -> Dict:
        """构建高亮块 (callout)"""
        return {
            "block_type": 19,
            "callout": {
                "background_color": 2,  # 浅蓝色背景
                "emoji_id": emoji,
            }
        }

    def _build_table_header_cell(self, content: str) -> Dict:
        """构建表格表头单元格"""
        return {
            "block_type": 2,
            "text": {
                "elements": [self._text_element(content, bold=True)],
                "style": {"align": 2}  # 居中
            }
        }

    def _build_table_cell(self, content: str, align: int = 1) -> Dict:
        """构建表格单元格 (align: 1=左, 2=中, 3=右)"""
        return {
            "block_type": 2,
            "text": {
                "elements": [self._text_element(content)],
                "style": {"align": align}
            }
        }

    def create_table(self, doc_token: str, block_id: str,
                     headers: List[str], rows: List[List[str]], index: int = -1) -> Dict:
        """
        创建表格

        Args:
            doc_token: 文档 token
            block_id: 父块 ID
            headers: 表头列表
            rows: 数据行列表
            index: 插入位置

        Returns:
            API 响应
        """
        row_count = len(rows) + 1  # +1 for header
        col_count = len(headers)

        logger.info(f"创建表格: {row_count}行 x {col_count}列")

        # 创建表格块
        table_block = {
            "block_type": 31,
            "table": {
                "property": {
                    "row_size": row_count,
                    "column_size": col_count,
                    "column_width": [120] * col_count
                }
            }
        }

        logger.info(f"表格参数: row_size={row_count}, column_size={col_count}")

        # 先创建表格
        result = self.create_block(doc_token, block_id, [table_block], index)
        if result.get("code") != 0:
            logger.error(f"创建表格失败: {result}")
            return result

        # 获取表格块 ID
        table_block_id = result.get("data", {}).get("children", [{}])[0].get("block_id")
        if not table_block_id:
            logger.error("无法获取表格块 ID")
            return {"code": -1, "msg": "无法获取表格块 ID"}

        logger.info(f"表格块 ID: {table_block_id[:12]}...")

        # 等待表格创建完成
        import time
        time.sleep(1.0)

        # 获取表格的子块（单元格）
        children_result = self.get_block_children(doc_token, table_block_id)
        if children_result.get("code") != 0:
            logger.error(f"获取表格子块失败: {children_result}")
            return children_result

        # 单元格列表
        cells = children_result.get("data", {}).get("items", [])
        logger.info(f"找到 {len(cells)} 个单元格")

        # 打印前几个单元格的详细信息
        logger.info(f"前5个单元格信息:")
        for i, cell in enumerate(cells[:5]):
            logger.info(f"  单元格{i}: block_type={cell.get('block_type')}, block_id={cell.get('block_id')[:12]}...")

        # 获取每个单元格下的文本块，如果没有则创建
        text_block_ids = []
        for idx, cell in enumerate(cells):
            cell_id = cell.get("block_id")
            if cell.get("block_type") == 32:  # table_cell
                # 获取单元格的子块
                cell_children = self.get_block_children(doc_token, cell_id)
                cell_items = cell_children.get("data", {}).get("items", [])
                text_block_id = None
                for item in cell_items:
                    if item.get("block_type") == 2:  # text
                        text_block_id = item.get("block_id")
                        logger.debug(f"单元格 {idx}: 已有文本块 block_id={text_block_id[:8]}...")
                        break

                # 如果单元格没有文本块，创建一个
                if not text_block_id:
                    text_block = {
                        "block_type": 2,  # text
                        "text": {
                            "elements": [{"text_run": {"content": ""}}]
                        }
                    }
                    create_result = self.create_block(doc_token, cell_id, [text_block])
                    if create_result.get("code") == 0:
                        new_blocks = create_result.get("data", {}).get("children", [])
                        if new_blocks and len(new_blocks) > 0:
                            # new_blocks 可能是字典列表或字符串列表
                            first_block = new_blocks[0]
                            if isinstance(first_block, dict):
                                # 字典类型：{"block_id": "xxx", "block_type": 2}
                                text_block_id = first_block.get("block_id")
                                logger.debug(f"单元格 {idx}: 创建文本块 block_id={text_block_id[:8] if text_block_id else 'None'}...")
                            elif isinstance(first_block, str):
                                # 字符串类型：直接是 block_id
                                text_block_id = first_block
                                logger.debug(f"单元格 {idx}: 创建文本块 block_id={text_block_id[:8]}...")
                            else:
                                logger.warning(f"单元格 {idx}: 创建文本块返回的类型异常: {type(first_block)}")

                if text_block_id:
                    text_block_ids.append(text_block_id)

        logger.info(f"找到/创建 {len(text_block_ids)} 个文本块")

        # 填充表格内容
        # 使用行优先顺序填充
        all_content = headers + [cell for row in rows for cell in row]

        logger.info(f"需要填充 {len(all_content)} 个单元格内容（行优先顺序）")
        logger.info(f"前10个单元格内容: {all_content[:10]}")

        for i, (text_id, content) in enumerate(zip(text_block_ids, all_content)):
            is_header = i < len(headers)
            logger.debug(f"填充单元格 {i}: {content}")
            self._update_cell_content(doc_token, text_id, content, bold=is_header)

        logger.info(f"表格创建成功: {row_count}行 x {col_count}列")
        return result

    def _update_cell_content(self, doc_token: str, block_id: str,
                             content: str, bold: bool = False) -> Dict:
        """更新单元格内容"""
        import time
        url = f"{self.base_url}/docx/v1/documents/{doc_token}/blocks/{block_id}"
        payload = {
            "update_text_elements": {
                "elements": [self._text_element(content, bold=bold)]
            }
        }
        # 重试机制
        for attempt in range(3):
            resp = requests.patch(url, headers=self._get_headers(), json=payload)
            if resp.status_code == 429:
                time.sleep(1.0 * (attempt + 1))  # 递增等待
                continue
            time.sleep(0.2)  # 正常请求间隔
            try:
                return resp.json()
            except Exception as e:
                logger.warning(f"更新单元格失败: {e}, status={resp.status_code}")
                return {"code": -1, "msg": str(e)}
        return {"code": -1, "msg": "rate limited"}

    # ============ 周报写入方法 ============

    def write_weekly_report(self, doc_token: str, report_data: Dict) -> Dict:
        """
        将周报数据写入飞书文档

        Args:
            doc_token: 文档 token
            report_data: 周报数据 (来自 BigQueryUploader.query_weekly_report_data())

        Returns:
            API 响应
        """
        blocks = []

        # 标题
        week_start = report_data.get('week_start', '')
        week_end = report_data.get('week_end', '')
        blocks.append(self._build_heading_block(f"周报 {week_start} ~ {week_end}", level=1))
        blocks.append(self._build_divider_block())

        # 大盘汇总
        summary = report_data.get('summary', {})
        blocks.append(self._build_heading_block("大盘汇总", level=2))
        blocks.append(self._build_text_block(
            f"周总消耗: ${summary.get('week_total_spend', 0):,.2f}"
        ))
        blocks.append(self._build_text_block(
            f"周总营收: ${summary.get('week_total_revenue', 0):,.2f}"
        ))
        blocks.append(self._build_text_block(
            f"周均 ROAS: {summary.get('week_avg_roas', 0):.1%}"
        ))

        # 写入文档
        result = self.create_block(doc_token, doc_token, blocks)
        logger.info(f"周报写入结果: {result.get('code')}")
        return result

    # ============ XMP 报表写入方法 ============

    def write_xmp_daily_report(self, doc_token: str, date_str: str,
                               optimizer_data: List[Dict],
                               editor_data: List[Dict]) -> Dict:
        """
        将 XMP 日报写入飞书文档

        Args:
            doc_token: 文档 token
            date_str: 日期 YYYY-MM-DD
            optimizer_data: 投手数据列表
            editor_data: 剪辑师数据列表

        Returns:
            API 响应
        """
        blocks = []

        # 标题
        blocks.append(self._build_heading_block(f"XMP 日报 {date_str}", level=1))
        blocks.append(self._build_divider_block())

        # 写入标题块
        result = self.create_block(doc_token, doc_token, blocks)
        if result.get("code") != 0:
            return result

        # 投手日报表格
        if optimizer_data:
            self._write_xmp_section(doc_token, "投手日报", optimizer_data, "投手")

        # 剪辑师日报表格
        if editor_data:
            self._write_xmp_section(doc_token, "剪辑师日报", editor_data, "剪辑师")

        logger.info(f"XMP 日报写入完成: {date_str}")
        return {"code": 0, "msg": "success"}

    def _write_xmp_section(self, doc_token: str, title: str,
                           data: List[Dict], role_name: str) -> Dict:
        """写入 XMP 报表的一个章节（标题 + 多个表格）"""
        # 添加章节标题
        blocks = [self._build_heading_block(title, level=2)]
        self.create_block(doc_token, doc_token, blocks)

        # 表格表头 (8列)
        headers = [role_name, 'Meta Spend', 'Meta ROAS', 'TT Spend', 'TT ROAS',
                   '总 Spend', '总 ROAS', '标注']

        # 构建所有行数据
        all_rows = []
        for d in data:
            row = [
                d.get('name', ''),
                f"${d.get('meta_spend', 0):,.0f}",
                f"{d.get('meta_roas', 0)*100:.1f}%",
                f"${d.get('tt_spend', 0):,.0f}",
                f"{d.get('tt_roas', 0)*100:.1f}%",
                f"${d.get('total_spend', 0):,.0f}",
                f"{d.get('total_roas', 0)*100:.1f}%",
                d.get('label', '')
            ]
            all_rows.append(row)
            # 调试日志：打印每行数据
            logger.info(f"[{title}] 行数据 ({len(row)}列): {row}")

        # 飞书表格限制：每个表格最多 5 行数据
        MAX_ROWS_PER_TABLE = 5
        result = {"code": 0, "msg": "success"}

        logger.info(f"[{title}] 表头 ({len(headers)}列): {headers}")
        logger.info(f"[{title}] 共 {len(all_rows)} 行数据")

        for i in range(0, len(all_rows), MAX_ROWS_PER_TABLE):
            chunk = all_rows[i:i + MAX_ROWS_PER_TABLE]
            logger.info(f"[{title}] 创建表格 (第{i//MAX_ROWS_PER_TABLE + 1}批): {len(chunk)} 行")
            table_result = self.create_table(doc_token, doc_token, headers, chunk)
            if table_result.get("code") != 0:
                logger.warning(f"表格创建失败 (第{i//MAX_ROWS_PER_TABLE + 1}批): {table_result}")
                result = table_result

        return result

    def write_xmp_weekly_report(self, doc_token: str,
                                start_date: str, end_date: str,
                                optimizer_data: List[Dict],
                                editor_data: List[Dict],
                                best_optimizer: Dict = None,
                                best_editor: Dict = None) -> Dict:
        """
        将 XMP 周报写入飞书文档

        Args:
            doc_token: 文档 token
            start_date: 开始日期
            end_date: 结束日期
            optimizer_data: 投手数据列表
            editor_data: 剪辑师数据列表
            best_optimizer: 最佳投手
            best_editor: 最佳剪辑师

        Returns:
            API 响应
        """
        blocks = []

        # 标题
        blocks.append(self._build_heading_block(
            f"XMP 周报 {start_date} ~ {end_date}", level=1))
        blocks.append(self._build_divider_block())

        # 写入标题
        result = self.create_block(doc_token, doc_token, blocks)
        if result.get("code") != 0:
            return result

        # 写入汇总信息
        self._write_xmp_summary(doc_token, optimizer_data, editor_data,
                                best_optimizer, best_editor)

        # 投手周报表格
        if optimizer_data:
            self._write_xmp_section(doc_token, "投手周报", optimizer_data, "投手")

        # 剪辑师周报表格
        if editor_data:
            self._write_xmp_section(doc_token, "剪辑师周报", editor_data, "剪辑师")

        logger.info(f"XMP 周报写入完成: {start_date} ~ {end_date}")
        return {"code": 0, "msg": "success"}

    def _write_xmp_summary(self, doc_token: str,
                           optimizer_data: List[Dict],
                           editor_data: List[Dict],
                           best_optimizer: Dict = None,
                           best_editor: Dict = None) -> Dict:
        """写入 XMP 周报汇总"""
        blocks = [self._build_heading_block("周报汇总", level=2)]

        # 投手汇总
        opt_total_spend = sum(d.get('total_spend', 0) for d in optimizer_data)
        opt_total_revenue = sum(d.get('total_revenue', 0) for d in optimizer_data)
        opt_avg_roas = opt_total_revenue / opt_total_spend if opt_total_spend > 0 else 0

        blocks.append(self._build_text_block("【投手汇总】", bold=True))
        blocks.append(self._build_text_block(f"投手人数: {len(optimizer_data)}"))
        blocks.append(self._build_text_block(f"总消耗: ${opt_total_spend:,.0f}"))
        blocks.append(self._build_text_block(f"总收入: ${opt_total_revenue:,.0f}"))
        blocks.append(self._build_text_block(f"平均 ROAS: {opt_avg_roas*100:.1f}%"))

        if best_optimizer:
            blocks.append(self._build_text_block(
                f"最佳投手: {best_optimizer.get('name', '')} "
                f"(Spend ${best_optimizer.get('total_spend', 0):,.0f}, "
                f"ROAS {best_optimizer.get('total_roas', 0)*100:.1f}%)"
            ))

        # 剪辑师汇总
        ed_total_spend = sum(d.get('total_spend', 0) for d in editor_data)
        ed_total_revenue = sum(d.get('total_revenue', 0) for d in editor_data)
        ed_avg_roas = ed_total_revenue / ed_total_spend if ed_total_spend > 0 else 0

        blocks.append(self._build_text_block(""))
        blocks.append(self._build_text_block("【剪辑师汇总】", bold=True))
        blocks.append(self._build_text_block(f"剪辑师人数: {len(editor_data)}"))
        blocks.append(self._build_text_block(f"总消耗: ${ed_total_spend:,.0f}"))
        blocks.append(self._build_text_block(f"总收入: ${ed_total_revenue:,.0f}"))
        blocks.append(self._build_text_block(f"平均 ROAS: {ed_avg_roas*100:.1f}%"))

        if best_editor:
            blocks.append(self._build_text_block(
                f"最佳剪辑师: {best_editor.get('name', '')} "
                f"(Spend ${best_editor.get('total_spend', 0):,.0f}, "
                f"ROAS {best_editor.get('total_roas', 0)*100:.1f}%)"
            ))

        blocks.append(self._build_divider_block())

        return self.create_block(doc_token, doc_token, blocks)

    # ============ 标准周报写入方法 ============

    def write_standard_weekly_report(self, doc_token: str, data: Dict) -> Dict:
        """
        将标准周报数据写入飞书文档 - 完整版
        与 LarkBot.send_weekly_report() 消息卡片内容保持一致

        Args:
            doc_token: 文档 token
            data: 周报数据，来自 BigQueryUploader.query_weekly_report_data()
        """
        week_start = data.get("week_start", "")
        week_end = data.get("week_end", "")

        # 写入标题
        blocks = [
            self._build_heading_block(f"📊 周报 {week_start} ~ {week_end}", level=1),
            self._build_divider_block()
        ]
        result = self.create_block(doc_token, doc_token, blocks)
        if result.get("code") != 0:
            return result

        # 1. 大盘综述
        self._write_weekly_summary_section(doc_token, data)

        # 2. 团队作战表现 (CN/KR)
        self._write_weekly_team_section(doc_token, data)

        # 3. 日趋势
        self._write_weekly_daily_trend_section(doc_token, data)

        # 4. 投手排行
        self._write_weekly_optimizer_section(doc_token, data)

        # 5. 剪辑师产出
        self._write_weekly_editor_section(doc_token, data)

        # 6. 剧集表现 (头部/潜力/衰退/尾部)
        self._write_weekly_drama_section(doc_token, data)

        # 7. 市场分析
        self._write_weekly_market_section(doc_token, data)

        # 8. AI 周度洞察
        self._write_weekly_ai_insights_section(doc_token, data)

        # 9. 本周总结与下周规划
        self._write_weekly_summary_plan_section(doc_token, data)

        logger.info(f"标准周报写入完成: {week_start} ~ {week_end}")
        return {"code": 0, "msg": "success"}

    def _write_weekly_summary_section(self, doc_token: str, data: Dict) -> Dict:
        """写入大盘综述"""
        summary = data.get("summary", {})
        prev_summary = data.get("prev_week_summary", {})

        week_spend = summary.get("week_total_spend", 0)
        week_revenue = summary.get("week_total_revenue", 0)
        week_roas = summary.get("week_avg_roas", 0)
        daily_avg = summary.get("daily_avg_spend", 0)
        week_cpm = summary.get("week_avg_cpm", 0)

        prev_spend = prev_summary.get("week_total_spend", 0)
        prev_revenue = prev_summary.get("week_total_revenue", 0)
        prev_roas = prev_summary.get("week_avg_roas", 0)
        prev_cpm = prev_summary.get("week_avg_cpm", 0)

        spend_change = (week_spend - prev_spend) / prev_spend if prev_spend > 0 else 0
        revenue_change = (week_revenue - prev_revenue) / prev_revenue if prev_revenue > 0 else 0
        roas_change = week_roas - prev_roas
        cpm_change = (week_cpm - prev_cpm) / prev_cpm if prev_cpm > 0 else 0

        # 收支比
        week_ratio = week_revenue / week_spend if week_spend > 0 else 0
        prev_ratio = prev_revenue / prev_spend if prev_spend > 0 else 0
        ratio_change = week_ratio - prev_ratio

        # emoji
        spend_emoji = "📈" if spend_change >= 0 else "📉"
        revenue_emoji = "📈" if revenue_change >= 0 else "📉"
        roas_emoji = "📈" if roas_change >= 0 else "📉"

        blocks = [self._build_heading_block("📅 大盘综述", level=2)]
        blocks.append(self._build_text_block(
            f"• 周总消耗: ${week_spend:,.0f} ({spend_emoji} {spend_change:+.0%})", bold=True))
        blocks.append(self._build_text_block(
            f"• 周总营收: ${week_revenue:,.0f} ({revenue_emoji} {revenue_change:+.0%})"))

        # 收支比
        if week_ratio > 0:
            ratio_status = "✅" if week_ratio >= 1 else "⚠️"
            blocks.append(self._build_text_block(
                f"• 收支比: {week_ratio:.1%} {ratio_status} ({ratio_change:+.1%})"))

        blocks.append(self._build_text_block(
            f"• 周均 ROAS: {week_roas:.1%} ({roas_emoji} {roas_change:+.1%})"))
        blocks.append(self._build_text_block(
            f"• 日均消耗: ${daily_avg:,.0f}"))

        # CPM
        if week_cpm > 0:
            cpm_emoji = "📈" if cpm_change >= 0 else "📉"
            cpm_warning = " ⚠️" if cpm_change > 0.05 else ""
            blocks.append(self._build_text_block(
                f"• 平均 CPM: ${week_cpm:.2f} ({cpm_emoji} {cpm_change:+.0%}){cpm_warning}"))

        blocks.append(self._build_divider_block())
        return self.create_block(doc_token, doc_token, blocks)

    def _write_weekly_optimizer_section(self, doc_token: str, data: Dict) -> Dict:
        """写入投手排行"""
        optimizer_weekly = data.get("optimizer_weekly", [])
        if not optimizer_weekly:
            return {"code": 0, "msg": "no data"}

        blocks = [self._build_heading_block("📊 投手周度排行", level=2)]
        self.create_block(doc_token, doc_token, blocks)

        # 飞书表格最多支持 9 行（含表头），所以数据最多 8 行
        MAX_TABLE_ROWS = 8
        table_data = optimizer_weekly[:MAX_TABLE_ROWS]
        extra_data = optimizer_weekly[MAX_TABLE_ROWS:]

        # 表格数据
        headers = ["排名", "投手", "消耗", "ROAS", "环比"]
        rows = []
        for i, opt in enumerate(table_data):
            roas_change = opt.get("roas_change", 0)
            rows.append([
                str(i + 1),
                opt.get("name", ""),
                f"${opt.get('spend', 0):,.0f}",
                f"{opt.get('roas', 0):.1%}",
                f"{roas_change:+.1%}" if roas_change else "-"
            ])

        if rows:
            self.create_table(doc_token, doc_token, headers, rows)

        # 超出部分用文本显示
        if extra_data:
            extra_blocks = [self._build_text_block("其他投手:", bold=True)]
            for i, opt in enumerate(extra_data):
                idx = MAX_TABLE_ROWS + i + 1
                roas_change = opt.get("roas_change", 0)
                change_str = f" ({roas_change:+.1%})" if roas_change else ""
                extra_blocks.append(self._build_text_block(
                    f"{idx}. {opt.get('name', '')}: ${opt.get('spend', 0):,.0f} | "
                    f"ROAS {opt.get('roas', 0):.1%}{change_str}"
                ))
            self.create_block(doc_token, doc_token, extra_blocks)

        return {"code": 0, "msg": "success"}

    def _write_weekly_editor_section(self, doc_token: str, data: Dict) -> Dict:
        """写入剪辑师产出与质量"""
        editor_stats = data.get("editor_stats", [])
        if not editor_stats:
            return {"code": 0, "msg": "no data"}

        blocks = [self._build_heading_block("🎬 剪辑师产出与质量", level=2)]
        self.create_block(doc_token, doc_token, blocks)

        # 显示完整字段：上新素材数、消耗贡献、D0 ROAS、爆款率、Top素材
        for i, editor in enumerate(editor_stats[:10]):
            name = editor.get("name", "")
            material_count = editor.get("material_count", 0)
            spend = editor.get("spend", 0)
            roas = editor.get("roas", 0)
            hot_rate = editor.get("hot_rate", 0)
            top_material = editor.get("top_material", "")

            # 格式化消耗
            spend_str = f"${spend/1000:.1f}k" if spend >= 1000 else f"${spend:.0f}"

            # 截断 top_material 名称
            if top_material and len(top_material) > 30:
                top_material = top_material[:27] + "..."

            editor_blocks = []
            editor_blocks.append(self._build_text_block(
                f"{i+1}. {name}", bold=True))
            editor_blocks.append(self._build_text_block(
                f"   上新素材数: {material_count} | 消耗贡献: {spend_str} | "
                f"D0 ROAS: {roas:.1%} | 爆款率: {hot_rate:.1%}"))
            if top_material:
                editor_blocks.append(self._build_text_block(
                    f"   Top素材: {top_material}"))

            self.create_block(doc_token, doc_token, editor_blocks)

        divider = [self._build_divider_block()]
        self.create_block(doc_token, doc_token, divider)

        return {"code": 0, "msg": "success"}

    def _write_weekly_drama_section(self, doc_token: str, data: Dict) -> Dict:
        """写入剧集表现"""
        top_dramas = data.get("top_dramas", [])
        potential_dramas = data.get("potential_dramas", [])
        declining_dramas = data.get("declining_dramas", [])
        losing_dramas = data.get("losing_dramas", [])

        blocks = [self._build_heading_block("🎬 剧集周度表现", level=2)]

        # 头部剧集
        if top_dramas:
            blocks.append(self._build_text_block("【🔥 头部剧集】消耗 > $10k 且 ROAS > 40%", bold=True))
            for i, drama in enumerate(top_dramas[:5]):
                countries = ", ".join(drama.get("top_countries", [])[:3])
                country_str = f" | {countries}" if countries else ""
                blocks.append(self._build_text_block(
                    f"{i+1}. 《{drama.get('name', '')}》: ${drama.get('spend', 0):,.0f} | "
                    f"ROAS {drama.get('roas', 0):.1%}{country_str}"))

        # 潜力剧集
        if potential_dramas:
            blocks.append(self._build_text_block(""))
            blocks.append(self._build_text_block("【⭐ 潜力剧集】消耗 $1k-$10k 且 ROAS > 50%", bold=True))
            for i, drama in enumerate(potential_dramas[:3]):
                blocks.append(self._build_text_block(
                    f"{i+1}. 《{drama.get('name', '')}》: ${drama.get('spend', 0):,.0f} | "
                    f"ROAS {drama.get('roas', 0):.1%} ⭐ 建议放量"))

        # 衰退预警
        if declining_dramas:
            blocks.append(self._build_text_block(""))
            blocks.append(self._build_text_block("【⚠️ 衰退预警】ROAS 周环比下降 > 10%", bold=True))
            for i, drama in enumerate(declining_dramas[:3]):
                roas_change = drama.get("roas_change", 0)
                blocks.append(self._build_text_block(
                    f"{i+1}. 《{drama.get('name', '')}》: ROAS {drama.get('roas', 0):.1%} "
                    f"(📉 {roas_change:+.1%})"))

        # 尾部亏损剧集
        if losing_dramas:
            blocks.append(self._build_text_block(""))
            blocks.append(self._build_text_block("【💀 尾部亏损】消耗 > $1k 且 ROAS < 25%", bold=True))
            for i, drama in enumerate(losing_dramas[:5]):
                spend = drama.get("spend", 0)
                spend_str = f"${spend/1000:.1f}k" if spend >= 1000 else f"${spend:.0f}"
                blocks.append(self._build_text_block(
                    f"{i+1}. 《{drama.get('name', '')}》: 耗 {spend_str} | "
                    f"ROAS {drama.get('roas', 0):.1%} 🚨 建议止损"))

        blocks.append(self._build_divider_block())
        return self.create_block(doc_token, doc_token, blocks)

    def _write_weekly_market_section(self, doc_token: str, data: Dict) -> Dict:
        """写入市场分析"""
        top_countries = data.get("top_countries", [])
        emerging_markets = data.get("emerging_markets", [])

        blocks = [self._build_heading_block("🌍 市场分析", level=2)]

        # 主力市场
        if top_countries:
            blocks.append(self._build_text_block("【💪 主力市场】", bold=True))
            for country in top_countries[:5]:
                change = country.get("roas_change", 0)
                change_str = f" ({change:+.1%})" if change else ""
                blocks.append(self._build_text_block(
                    f"{country.get('name', '')}: ${country.get('spend', 0):,.0f} | "
                    f"ROAS {country.get('roas', 0):.1%}{change_str}"))

        # 新兴机会
        if emerging_markets:
            blocks.append(self._build_text_block(""))
            blocks.append(self._build_text_block("【🚀 新兴机会】", bold=True))
            for market in emerging_markets[:3]:
                blocks.append(self._build_text_block(
                    f"{market.get('name', '')}: ROAS {market.get('roas', 0):.1%} "
                    f"(${market.get('spend', 0):,.0f})"))

        blocks.append(self._build_divider_block())
        return self.create_block(doc_token, doc_token, blocks)

    def _write_weekly_team_section(self, doc_token: str, data: Dict) -> Dict:
        """写入团队作战表现板块"""
        optimizer_weekly = data.get("optimizer_weekly", [])
        if not optimizer_weekly:
            return {"code": 0, "msg": "no data"}

        # 按团队分组统计
        cn_spend, cn_revenue, kr_spend, kr_revenue = 0, 0, 0, 0
        cn_count, kr_count = 0, 0
        cn_campaigns, kr_campaigns = 0, 0

        for opt in optimizer_weekly:
            name = opt.get("name", "")
            spend = opt.get("spend", 0)
            revenue = opt.get("revenue", 0)
            campaigns = opt.get("campaign_count", 0)
            team = get_optimizer_team(name)

            if team == "CN":
                cn_spend += spend
                cn_revenue += revenue
                cn_count += 1
                cn_campaigns += campaigns
            elif team == "KR":
                kr_spend += spend
                kr_revenue += revenue
                kr_count += 1
                kr_campaigns += campaigns

        cn_roas = cn_revenue / cn_spend if cn_spend > 0 else 0
        kr_roas = kr_revenue / kr_spend if kr_spend > 0 else 0
        total_spend = cn_spend + kr_spend

        blocks = [self._build_heading_block("⚔️ 团队作战表现", level=2)]

        # CN 团队
        if cn_count > 0:
            cn_ratio = cn_spend / total_spend if total_spend > 0 else 0
            blocks.append(self._build_text_block("🇨🇳 CN 投放团队", bold=True))
            blocks.append(self._build_text_block(
                f"• 消耗: ${cn_spend:,.0f} (占比 {cn_ratio:.0%}) | ROAS: {cn_roas:.1%}"))
            blocks.append(self._build_text_block(f"• 计划数: {cn_campaigns} 个"))

        # KR 团队
        if kr_count > 0:
            kr_ratio = kr_spend / total_spend if total_spend > 0 else 0
            blocks.append(self._build_text_block("🇰🇷 KR 投放团队", bold=True))
            blocks.append(self._build_text_block(
                f"• 消耗: ${kr_spend:,.0f} (占比 {kr_ratio:.0%}) | ROAS: {kr_roas:.1%}"))
            blocks.append(self._build_text_block(f"• 计划数: {kr_campaigns} 个"))

        blocks.append(self._build_divider_block())
        return self.create_block(doc_token, doc_token, blocks)

    def _write_weekly_daily_trend_section(self, doc_token: str, data: Dict) -> Dict:
        """写入日趋势板块"""
        daily_stats = data.get("daily_stats", [])
        if not daily_stats:
            return {"code": 0, "msg": "no data"}

        blocks = [self._build_heading_block("📈 日趋势", level=2)]
        self.create_block(doc_token, doc_token, blocks)

        # 表格数据 (最多 7 天)
        headers = ["日期", "消耗", "ROAS", "趋势"]
        rows = []
        prev_roas = 0
        for day in daily_stats[:7]:
            date_str = day.get("date", "")
            if len(date_str) >= 10:
                date_display = f"{date_str[5:7]}/{date_str[8:10]}"
            else:
                date_display = date_str
            spend = day.get("spend", 0)
            roas = day.get("roas", 0)
            trend = "📈" if roas > prev_roas else "📉" if roas < prev_roas else "➡️"
            prev_roas = roas
            rows.append([date_display, f"${spend:,.0f}", f"{roas:.1%}", trend])

        if rows:
            self.create_table(doc_token, doc_token, headers, rows)

        return {"code": 0, "msg": "success"}

    def _write_weekly_ai_insights_section(self, doc_token: str, data: Dict) -> Dict:
        """写入 AI 周度洞察板块"""
        ai_insights = data.get("ai_insights", {})
        if not ai_insights:
            return {"code": 0, "msg": "no data"}

        blocks = [self._build_heading_block("🤖 AI 周度洞察", level=2)]

        key_findings = ai_insights.get("key_findings", "")
        risk_alerts = ai_insights.get("risk_alerts", "")
        suggestions = ai_insights.get("next_week_suggestions", "")

        if key_findings:
            blocks.append(self._build_text_block(f"• 核心发现: {key_findings}"))
        if risk_alerts:
            blocks.append(self._build_text_block(f"• 风险提示: {risk_alerts}"))
        if suggestions:
            blocks.append(self._build_text_block(f"• 下周建议: {suggestions}"))

        blocks.append(self._build_divider_block())
        return self.create_block(doc_token, doc_token, blocks)

    def _write_weekly_summary_plan_section(self, doc_token: str, data: Dict) -> Dict:
        """写入本周总结与下周规划板块"""
        potential_dramas = data.get("potential_dramas", [])
        losing_dramas = data.get("losing_dramas", [])
        emerging_markets = data.get("emerging_markets", [])

        blocks = [self._build_heading_block("📝 本周总结与下周规划", level=2)]

        next_week_actions = []

        # 1. 扩量建议
        if potential_dramas:
            top = potential_dramas[0]
            next_week_actions.append(
                f"扩量: 针对《{top.get('name', '')}》进行放量测试 "
                f"(当前 ROAS {top.get('roas', 0):.0%})")

        # 2. 止损建议
        if losing_dramas:
            top = losing_dramas[0]
            next_week_actions.append(
                f"止损: 关停《{top.get('name', '')}》相关计划 "
                f"(ROAS {top.get('roas', 0):.0%})")

        # 3. 市场机会
        if emerging_markets:
            top = emerging_markets[0]
            next_week_actions.append(
                f"拓展: 测试 {top.get('name', '')} 市场 "
                f"(ROAS {top.get('roas', 0):.0%})")

        if next_week_actions:
            blocks.append(self._build_text_block("【下周重点】", bold=True))
            for i, action in enumerate(next_week_actions[:3]):
                blocks.append(self._build_text_block(f"{i+1}. {action}"))

        blocks.append(self._build_divider_block())
        return self.create_block(doc_token, doc_token, blocks)


def create_doc_client(app_id: str = None, app_secret: str = None, use_intl: bool = True) -> LarkDocClient:
    """
    创建飞书文档客户端

    Args:
        app_id: App ID，不传则从环境变量获取
        app_secret: App Secret，不传则从环境变量获取
        use_intl: 是否使用国际版 API
    """
    app_id = app_id or os.getenv("LARK_APP_ID")
    app_secret = app_secret or os.getenv("LARK_APP_SECRET")

    if not app_id or not app_secret:
        raise ValueError("需要配置 LARK_APP_ID 和 LARK_APP_SECRET")

    return LarkDocClient(app_id, app_secret, use_intl)