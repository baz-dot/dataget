"""
读取飞书表格的实际结构
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from lark.lark_doc_client import create_doc_client
from dotenv import load_dotenv

load_dotenv()

def read_table_structure():
    """读取表格结构"""
    doc_token = os.getenv("XMP_DOC_TOKEN", "XMYlw4GkDicYhwk3VWglkF0DgZb")
    
    client = create_doc_client()
    
    print("=" * 80)
    print("读取飞书文档中最新表格的结构")
    print("=" * 80)
    print()
    
    # 获取文档的所有块
    blocks_result = client.get_block_children(doc_token, doc_token)
    
    if blocks_result.get("code") != 0:
        print(f"❌ 获取文档块失败: {blocks_result}")
        return
    
    blocks = blocks_result.get("data", {}).get("items", [])
    print(f"文档中共有 {len(blocks)} 个块")
    print()
    
    # 找到最后一个表格块
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
    
    print(f"找到表格块:")
    print(f"  block_id: {table_id}")
    print(f"  row_size: {table_property.get('row_size')}")
    print(f"  column_size: {table_property.get('column_size')}")
    print()
    
    # 获取表格的单元格
    cells_result = client.get_block_children(doc_token, table_id)
    if cells_result.get("code") != 0:
        print(f"❌ 获取单元格失败: {cells_result}")
        return
    
    cells = cells_result.get("data", {}).get("items", [])
    print(f"表格中共有 {len(cells)} 个单元格")
    print()
    
    # 读取每个单元格的内容
    print("读取单元格内容...")
    print("-" * 80)
    
    for i, cell in enumerate(cells[:30]):  # 只读取前30个
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
        
        print(f"单元格 {i:2d}: {content or '(空)'}")
    
    print()
    print("=" * 80)

if __name__ == '__main__':
    read_table_structure()

