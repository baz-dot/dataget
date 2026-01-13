#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
手动数据处理器 - 处理从 DataEye API 手动复制的 JSON 数据
"""

import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows

class ManualDataProcessor:
    """处理手动提供的 DataEye API JSON 数据"""

    def __init__(self):
        self.drama_data = {}
        self.recent_days = 30
        self.lifecycle_days = 730

    def load_json_file(self, file_path: str) -> Dict:
        """加载 JSON 文件"""
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def parse_api_response(self, json_data: Dict, drama_name: str = None) -> Dict:
        """解析 searchCreative API 响应"""
        result = {
            'drama_name': drama_name or 'Unknown',
            'total_records': 0,
            'creatives': [],
            'raw_data': json_data
        }

        # 提取总记录数
        if 'page' in json_data:
            result['total_records'] = json_data['page'].get('totalRecords', 0)

        # 提取素材列表
        if 'content' in json_data and 'searchList' in json_data['content']:
            result['creatives'] = json_data['content']['searchList']

        return result

    def aggregate_drama_data(self, creatives: List[Dict]) -> Dict:
        """聚合单个剧的数据"""
        if not creatives:
            return self._empty_aggregation()

        # 计算时间窗口
        today = datetime.now()
        recent_cutoff = today - timedelta(days=self.recent_days)
        lifecycle_cutoff = today - timedelta(days=self.lifecycle_days)

        # 初始化聚合数据
        recent_data = defaultdict(lambda: {'impressions': 0, 'creatives': set(), 'countries': set(), 'publishers': set()})
        lifecycle_data = defaultdict(lambda: {'impressions': 0, 'creatives': set(), 'countries': set(), 'publishers': set()})

        all_publishers = set()
        all_countries = set()
        first_seen_dates = []
        last_seen_dates = []

        for creative in creatives:
            # 提取基础信息
            first_seen = datetime.strptime(creative['firstSeen'], '%Y-%m-%d')
            last_seen = datetime.strptime(creative['lastSeen'], '%Y-%m-%d')
            impressions = creative.get('exposureNum', 0)
            material_id = creative.get('materialId')

            first_seen_dates.append(first_seen)
            last_seen_dates.append(last_seen)

            # 提取国家
            countries = [c['countryName'] for c in creative.get('countries', [])]
            all_countries.update(countries)

            # 提取发布商
            publisher = creative.get('publisher', {}).get('publisherName', 'Unknown')
            all_publishers.add(publisher)

            # 近30天数据
            if last_seen >= recent_cutoff:
                for date in pd.date_range(max(first_seen, recent_cutoff), last_seen):
                    date_str = date.strftime('%Y-%m-%d')
                    recent_data[date_str]['impressions'] += impressions / (last_seen - first_seen).days if (last_seen - first_seen).days > 0 else impressions
                    recent_data[date_str]['creatives'].add(material_id)
                    recent_data[date_str]['countries'].update(countries)
                    recent_data[date_str]['publishers'].add(publisher)

            # 近2年数据
            if last_seen >= lifecycle_cutoff:
                lifecycle_data['total']['impressions'] += impressions
                lifecycle_data['total']['creatives'].add(material_id)
                lifecycle_data['total']['countries'].update(countries)
                lifecycle_data['total']['publishers'].add(publisher)

        # 计算生命周期
        lifecycle_days = (max(last_seen_dates) - min(first_seen_dates)).days if first_seen_dates else 0

        return {
            'recent_30d': recent_data,
            'lifecycle_2y': lifecycle_data,
            'first_seen': min(first_seen_dates) if first_seen_dates else None,
            'last_seen': max(last_seen_dates) if last_seen_dates else None,
            'lifecycle_days': lifecycle_days,
            'total_publishers': list(all_publishers),
            'total_countries': list(all_countries),
            'total_creatives': len(creatives)
        }

    def _empty_aggregation(self) -> Dict:
        """返回空的聚合数据"""
        return {
            'recent_30d': {},
            'lifecycle_2y': {},
            'first_seen': None,
            'last_seen': None,
            'lifecycle_days': 0,
            'total_publishers': [],
            'total_countries': [],
            'total_creatives': 0
        }
