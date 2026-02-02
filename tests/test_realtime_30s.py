"""
测试实时战报 - 30秒间隔持续播报
"""
import time
from datetime import datetime
from scheduler import BrainScheduler

def main():
    print("="*60)
    print("启动实时战报测试 - 30秒间隔")
    print("="*60)

    scheduler = BrainScheduler()

    # 持续运行，每30秒发送一次
    while True:
        try:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 触发实时播报...")
            result = scheduler.send_realtime_report()

            if result.get("success"):
                print(f"✓ 播报成功")
            else:
                print(f"✗ 播报失败: {result.get('error', result.get('reason', '未知'))}")

            # 等待30秒
            print(f"\n等待30秒后下次播报...")
            time.sleep(30)

        except KeyboardInterrupt:
            print("\n\n用户中断，停止播报")
            break
        except Exception as e:
            print(f"\n[Error] 播报异常: {str(e)}")
            print("等待30秒后重试...")
            time.sleep(30)

if __name__ == "__main__":
    main()
