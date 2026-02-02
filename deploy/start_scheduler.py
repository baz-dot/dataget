"""
Cloud Run 启动脚本
启动 HTTP 健康检查服务器 + 后台调度器
"""
import os
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class HealthHandler(BaseHTTPRequestHandler):
    """健康检查处理器"""

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK - Scheduler Running')

    def log_message(self, format, *args):
        # 减少日志输出
        pass


def start_http_server():
    """启动 HTTP 服务器"""
    port = int(os.environ.get('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    print(f"[HTTP] 健康检查服务器启动在端口 {port}")
    server.serve_forever()


def start_scheduler():
    """启动调度器"""
    from scheduler import BrainScheduler
    scheduler = BrainScheduler()
    scheduler.start_scheduler(interval_minutes=60, skip_first_daily=True, use_latest_batch=False)


if __name__ == '__main__':
    # 启动 HTTP 服务器（主线程）
    http_thread = threading.Thread(target=start_http_server, daemon=True)
    http_thread.start()

    # 启动调度器（阻塞）
    start_scheduler()
