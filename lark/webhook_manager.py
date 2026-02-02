"""
投手 Webhook 配置管理模块
用于个人专属助理推送
"""

import os
import json
from typing import Dict, Optional, Any

# 配置文件路径
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "optimizer_webhooks.json")


class OptimizerWebhookManager:
    """投手 Webhook 配置管理器"""

    def __init__(self, config_path: str = None):
        """
        初始化配置管理器

        Args:
            config_path: 配置文件路径，默认为同目录下的 optimizer_webhooks.json
        """
        self.config_path = config_path or CONFIG_FILE
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """加载配置文件"""
        if not os.path.exists(self.config_path):
            return {"webhooks": {}, "default_webhook": {}}

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[WARNING] 加载 Webhook 配置失败: {e}")
            return {"webhooks": {}, "default_webhook": {}}

    def _save_config(self) -> bool:
        """保存配置文件"""
        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"[ERROR] 保存 Webhook 配置失败: {e}")
            return False

    def get_webhook(self, optimizer_name: str) -> Optional[Dict[str, Any]]:
        """
        获取投手的 Webhook 配置

        Args:
            optimizer_name: 投手名称

        Returns:
            {"webhook_url": "...", "secret": "..."} 或 None
        """
        webhooks = self.config.get("webhooks", {})
        webhook_config = webhooks.get(optimizer_name)

        if webhook_config and webhook_config.get("enabled", True):
            return {
                "webhook_url": webhook_config.get("webhook_url"),
                "secret": webhook_config.get("secret")
            }
        return None

    def get_all_optimizers(self) -> list:
        """获取所有已配置的投手列表"""
        webhooks = self.config.get("webhooks", {})
        return [
            name for name, cfg in webhooks.items()
            if cfg.get("enabled", True) and cfg.get("webhook_url")
        ]

    def add_optimizer(self, optimizer_name: str, webhook_url: str, secret: str = None) -> bool:
        """
        添加投手 Webhook 配置

        Args:
            optimizer_name: 投手名称
            webhook_url: Webhook URL
            secret: 签名密钥（可选）
        """
        if "webhooks" not in self.config:
            self.config["webhooks"] = {}

        self.config["webhooks"][optimizer_name] = {
            "webhook_url": webhook_url,
            "secret": secret,
            "enabled": True
        }
        return self._save_config()

    def remove_optimizer(self, optimizer_name: str) -> bool:
        """移除投手 Webhook 配置"""
        webhooks = self.config.get("webhooks", {})
        if optimizer_name in webhooks:
            del webhooks[optimizer_name]
            return self._save_config()
        return False

    def disable_optimizer(self, optimizer_name: str) -> bool:
        """禁用投手 Webhook"""
        webhooks = self.config.get("webhooks", {})
        if optimizer_name in webhooks:
            webhooks[optimizer_name]["enabled"] = False
            return self._save_config()
        return False


# 全局实例
_manager = None


def get_webhook_manager() -> OptimizerWebhookManager:
    """获取全局 Webhook 管理器实例"""
    global _manager
    if _manager is None:
        _manager = OptimizerWebhookManager()
    return _manager
