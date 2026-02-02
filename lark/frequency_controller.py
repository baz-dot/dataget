"""
频次控制模块 - 建议降噪
连续3小时相同建议则不再重复推送
"""
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# 历史记录文件路径
HISTORY_FILE = os.path.join(os.path.dirname(__file__), "alert_history.json")


class AlertFrequencyController:
    """建议频次控制器"""

    def __init__(self, history_path: str = None, suppress_hours: int = 3):
        """
        初始化频次控制器

        Args:
            history_path: 历史记录文件路径
            suppress_hours: 连续多少小时相同建议后降噪，默认3小时
        """
        self.history_path = history_path or HISTORY_FILE
        self.suppress_hours = suppress_hours
        self.history = self._load_history()

    def _load_history(self) -> Dict:
        """加载历史记录"""
        if not os.path.exists(self.history_path):
            return {"alerts": {}}

        try:
            with open(self.history_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"alerts": {}}

    def _save_history(self) -> bool:
        """保存历史记录"""
        try:
            with open(self.history_path, "w", encoding="utf-8") as f:
                json.dump(self.history, f, ensure_ascii=False, indent=2)
            return True
        except Exception:
            return False

    def _get_alert_key(self, optimizer: str, campaign_id: str, alert_type: str) -> str:
        """生成建议唯一标识"""
        return f"{optimizer}:{campaign_id}:{alert_type}"

    def _clean_old_records(self):
        """清理超过24小时的旧记录"""
        now = datetime.now()
        cutoff = (now - timedelta(hours=24)).isoformat()

        alerts = self.history.get("alerts", {})
        keys_to_delete = []

        for key, records in alerts.items():
            # 过滤掉超过24小时的记录
            alerts[key] = [r for r in records if r.get("time", "") > cutoff]
            if not alerts[key]:
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del alerts[key]

    def should_suppress(self, optimizer: str, campaign_id: str, alert_type: str) -> bool:
        """
        判断是否应该降噪（不发送）

        Args:
            optimizer: 投手名称
            campaign_id: 计划ID
            alert_type: 建议类型 (stop_loss/scale_up/zombie)

        Returns:
            True = 应该降噪（不发送），False = 应该发送
        """
        key = self._get_alert_key(optimizer, campaign_id, alert_type)
        records = self.history.get("alerts", {}).get(key, [])

        if len(records) < self.suppress_hours:
            return False

        # 检查最近 N 小时是否都有相同建议
        now = datetime.now()
        recent_count = 0

        for i in range(self.suppress_hours):
            hour_start = (now - timedelta(hours=i+1)).isoformat()
            hour_end = (now - timedelta(hours=i)).isoformat()

            has_record = any(
                hour_start <= r.get("time", "") <= hour_end
                for r in records
            )
            if has_record:
                recent_count += 1

        return recent_count >= self.suppress_hours

    def record_alert(self, optimizer: str, campaign_id: str, alert_type: str):
        """
        记录一次建议

        Args:
            optimizer: 投手名称
            campaign_id: 计划ID
            alert_type: 建议类型
        """
        key = self._get_alert_key(optimizer, campaign_id, alert_type)

        if "alerts" not in self.history:
            self.history["alerts"] = {}

        if key not in self.history["alerts"]:
            self.history["alerts"][key] = []

        self.history["alerts"][key].append({
            "time": datetime.now().isoformat(),
            "type": alert_type
        })

        # 清理旧记录并保存
        self._clean_old_records()
        self._save_history()

    def filter_alerts(self, optimizer: str, alerts: List[Dict], alert_type: str) -> List[Dict]:
        """
        过滤建议列表，移除应该降噪的建议

        Args:
            optimizer: 投手名称
            alerts: 建议列表
            alert_type: 建议类型

        Returns:
            过滤后的建议列表
        """
        filtered = []
        for alert in alerts:
            campaign_id = alert.get("campaign_id", "")
            if not self.should_suppress(optimizer, campaign_id, alert_type):
                filtered.append(alert)
                # 记录这次建议
                self.record_alert(optimizer, campaign_id, alert_type)

        return filtered

    def get_suppressed_count(self, optimizer: str, alerts: List[Dict], alert_type: str) -> int:
        """获取被降噪的建议数量"""
        count = 0
        for alert in alerts:
            campaign_id = alert.get("campaign_id", "")
            if self.should_suppress(optimizer, campaign_id, alert_type):
                count += 1
        return count


# 全局实例
_controller = None


def get_frequency_controller() -> AlertFrequencyController:
    """获取全局频次控制器实例"""
    global _controller
    if _controller is None:
        _controller = AlertFrequencyController()
    return _controller
