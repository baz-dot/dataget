"""
个人专属助理 - 统一入口
整合小时级流水账、智能预警、重启提醒
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, Any, List
from lark.lark_bot import LarkBot
from lark.webhook_manager import get_webhook_manager
from lark.frequency_controller import get_frequency_controller
from bigquery_storage import BigQueryUploader
from config.data_source import get_data_source_config


class PersonalAssistant:
    """投手个人专属助理"""

    def __init__(self, project_id: str, dataset_id: str = None):
        # 从配置获取默认数据源
        if dataset_id is None:
            dataset_id = get_data_source_config()["dataset_id"]
        self.bq = BigQueryUploader(project_id, dataset_id)
        self.webhook_manager = get_webhook_manager()
        self.freq_controller = get_frequency_controller()

    def send_to_optimizer(self, optimizer: str, webhook_url: str = None,
                          secret: str = None) -> Dict[str, Any]:
        """
        向指定投手发送个人助理消息

        Args:
            optimizer: 投手名称
            webhook_url: Webhook URL (可选，不传则从配置读取)
            secret: 签名密钥
        """
        # 获取 webhook 配置
        if not webhook_url:
            config = self.webhook_manager.get_webhook(optimizer)
            if not config:
                return {"error": f"未配置 {optimizer} 的 webhook"}
            webhook_url = config.get("webhook_url")
            secret = config.get("secret")

        bot = LarkBot(webhook_url, secret)
        results = {}

        # 1. 小时级流水账
        pacing_data = self.bq.query_optimizer_hourly_pacing(optimizer)
        if pacing_data.get("hourly_data"):
            results["pacing"] = bot.send_optimizer_hourly_pacing(pacing_data)

        # 2. 智能预警 (带频次控制)
        alerts_data = self.bq.query_optimizer_alerts_with_benchmark(optimizer)
        alerts_data = self._apply_frequency_control(optimizer, alerts_data)

        if alerts_data.get("stop_loss_alerts") or alerts_data.get("scale_up_alerts"):
            results["alerts"] = bot.send_optimizer_smart_alerts(alerts_data)

        # 3. 重启提醒
        zombie_data = self.bq.query_optimizer_zombie_alerts(optimizer)
        if zombie_data.get("zombie_alerts"):
            results["zombie"] = bot.send_optimizer_zombie_alerts(zombie_data)

        return results

    def _apply_frequency_control(self, optimizer: str,
                                  alerts_data: Dict) -> Dict:
        """应用频次控制，过滤重复建议"""
        # 过滤止损预警
        stop_loss = alerts_data.get("stop_loss_alerts", [])
        filtered_stop = self.freq_controller.filter_alerts(
            optimizer, stop_loss, "stop_loss"
        )

        # 过滤扩量机会
        scale_up = alerts_data.get("scale_up_alerts", [])
        filtered_scale = self.freq_controller.filter_alerts(
            optimizer, scale_up, "scale_up"
        )

        return {
            "optimizer": optimizer,
            "stop_loss_alerts": filtered_stop,
            "scale_up_alerts": filtered_scale
        }

    def send_to_all_optimizers(self) -> Dict[str, Any]:
        """向所有已配置的投手发送个人助理消息"""
        results = {}
        optimizers = self.webhook_manager.get_all_optimizers()

        for optimizer in optimizers:
            results[optimizer] = self.send_to_optimizer(optimizer)

        return results
