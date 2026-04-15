"""
读取 2x2 表格的单元格顺序
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from lark.lark_doc_client import create_doc_client
from dotenv import load_dotenv

load_dotenv()

def read_2x2_table():
    """读取 2x2 表格"""
    doc_token = os.getenv("XMP_DOC_TOKEN", "XMYlw4GkDicYhwk3VWglkF0DgZb")
    
    client = create_doc_client()
    
    print("=" * 80)
    print("读取 2x2 表格的单元格顺序")
    print("=" * 80)
    print()
    
    # 获取文档的所有块
    blocks_result = client.get_block_children(doc_token, doc_token)
    blocks = blocks_result.get("data", {}).get("items", [])
    
    # 找到最后一个表格块（2x2 表格）
    table_block = None
    for block in reversed(blocks):
        if block.get("block_type") == 31:  # table
            table_block = block
            break
    
    if not table_block:
        print("❌ 未找到表格块")
        return
    
    table_id = table_block.get("block_id")
    table_info = table_block.get("table", {})
    table_property = table_info.get("property", {})
    
    print(f"表格信息:")
    print(f"  row_size: {table_property.get('row_size')}")
    print(f"  column_size: {table_property.get('column_size')}")
    print()
    
    # 获取表格的单元格
    cells_result = client.get_block_children(doc_token, table_id)
    cells = cells_result.get("data", {}).get("items", [])
    
    print(f"单元格总数: {len(cells)}")
    print()
    
    # 读取每个单元格的内容
    print("单元格内容顺序:")
    print("-" * 80)
    
    for i, cell in enumerate(cells):
        cell_id = cell.get("block_id")
        
        # 获取单元格的文本块
        cell_children = client.get_block_children(doc_token, cell_id)
        cell_items = cell_children.get("data", {}).get("items", [])
        
        content = ""
        for item in cell_items:
            if item.get("block_type") == 2:  # text
                text_elements = item.get("text", {}).get("elements", [])
                for elem in text_elements:
                    content += elem.get("text_run", {}).get("content", "")
        
        print(f"单元格 {i}: {content or '(空)'}")
    
    print()
    print("分析:")
    print("-" * 80)
    print("如果单元格顺序是: A, B, 1, 2")
    print("  → 说明是按行优先顺序")
    print()
    print("如果单元格顺序是: A, 1, B, 2")
    print("  → 说明是按列优先顺序")
    print()

if __name__ == '__main__':
    read_2x2_table()

