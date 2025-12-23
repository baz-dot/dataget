# 飞书机器人播报配置指南

## 一、创建群机器人

### 1.1 进入群设置
- 打开飞书客户端
- 进入需要接收数据播报的群聊
- 点击右上角「...」或齿轮图标
- 选择「设置」

### 1.2 添加机器人
- 在设置页面找到「群机器人」
- 点击「添加机器人」
- 选择「自定义机器人」

### 1.3 配置机器人信息
- 机器人名称：`数据播报助手`（可自定义）
- 机器人描述：`用于推送市场监控、投放效果等数据报告`
- 头像：可选上传

### 1.4 获取 Webhook 地址
- 点击「添加」后，系统会生成一个 Webhook 地址
- 格式类似：`https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
- **请妥善保存此地址，不要泄露**

### 1.5 开启签名校验（推荐）
- 在机器人设置页面，开启「签名校验」
- 复制生成的「签名密钥」
- 签名校验可防止 Webhook 地址泄露后被恶意调用

---

## 二、配置项目

### 2.1 更新环境变量
编辑项目根目录下的 `.env` 文件，填入以下配置：

```
# 飞书机器人配置
LARK_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/你的webhook地址
LARK_WEBHOOK_SECRET=你的签名密钥
```

### 2.2 安装依赖
确保已安装必要的 Python 包：

```bash
pip install requests python-dotenv
```

---

## 三、测试机器人

### 3.1 运行测试脚本
```bash
python test_lark_bot.py
```

### 3.2 预期结果
如果配置正确，群内会收到以下测试消息：
1. 一条文本消息
2. 一张市场监控日报卡片
3. 一张投放效果监控卡片
4. 一条告警消息

### 3.3 常见错误

| 错误信息 | 原因 | 解决方案 |
|---------|------|---------|
| `{"code":19021}` | 签名校验失败 | 检查 LARK_WEBHOOK_SECRET 是否正确 |
| `{"code":19024}` | Webhook 地址无效 | 检查 LARK_WEBHOOK_URL 是否完整 |
| `{"code":9499}` | 机器人被禁用 | 在群设置中重新启用机器人 |

---

## 四、使用方法

### 4.1 基础用法

```python
from lark_bot import LarkBot
import os
from dotenv import load_dotenv

load_dotenv()

# 创建机器人实例
bot = LarkBot(
    webhook_url=os.getenv("LARK_WEBHOOK_URL"),
    secret=os.getenv("LARK_WEBHOOK_SECRET")
)

# 发送文本消息
bot.send_text("这是一条测试消息")
```

### 4.2 发送市场监控报告

```python
market_data = {
    "date": "2025-01-15",
    "impressions": 1500000,    # 曝光量
    "clicks": 45000,           # 点击量
    "cost": 12500.50,          # 消耗金额
    "ctr": 0.03,               # 点击率
    "cpc": 0.28,               # 单次点击成本
    "budget": 15000            # 预算（用于判断卡片颜色）
}

bot.send_market_report(market_data)
```

### 4.3 发送投放效果报告

```python
ad_data = {
    "period": "2025-01-08 ~ 2025-01-15",
    "channel": "抖音/快手/腾讯广告",
    "conversions": 3200,       # 转化数
    "cpa": 15.5,               # 转化成本
    "roi": 1.85,               # 投资回报率
    "change": "↑ 12.5%"        # 环比变化
}

bot.send_ad_performance_report(ad_data)
```

### 4.4 发送告警消息

```python
bot.send_alert(
    alert_type="预算告警",
    message="腾讯广告渠道消耗已达预算 90%",
    level="warning"  # info / warning / error
)
```

### 4.5 @所有人

```python
bot.send_text("重要通知：系统维护", at_all=True)
```

---

## 五、@指定人功能（进阶）

### 5.1 获取用户 open_id

要@指定的人，需要获取其 `open_id`。有以下几种方式：

#### 方式一：通过飞书开放平台 API

1. 访问 https://open.feishu.cn/
2. 创建企业自建应用
3. 获取 App ID 和 App Secret
4. 申请权限：`contact:user.id:readonly`
5. 调用接口通过手机号/邮箱查询 open_id

#### 方式二：通过管理后台导出

1. 登录飞书管理后台
2. 导出通讯录，包含用户 open_id

### 5.2 使用 open_id @用户

```python
# @单个用户
bot.send_text("请查看报告", at_user_ids=["ou_xxxxxxxx"])

# @多个用户
bot.send_text("请查看报告", at_user_ids=["ou_xxx", "ou_yyy"])

# 在卡片消息中@用户
bot.send_market_report(market_data, at_user_ids=["ou_xxx"])
```

---

## 六、集成到数据流程

### 6.1 定时播报示例

```python
# 可配合 Cloud Scheduler 或 cron 定时执行
from lark_bot import LarkBot
from bigquery_storage import BigQueryStorage

def daily_report():
    # 1. 从 BigQuery 获取数据
    bq = BigQueryStorage()
    data = bq.query_daily_stats()

    # 2. 发送播报
    bot = LarkBot(webhook_url, secret)
    bot.send_market_report({
        "date": data["date"],
        "impressions": data["impressions"],
        "clicks": data["clicks"],
        "cost": data["cost"],
        "ctr": data["ctr"],
        "cpc": data["cpc"]
    })

if __name__ == "__main__":
    daily_report()
```

### 6.2 异常告警示例

```python
def check_budget_alert():
    # 检查预算消耗
    if current_cost / budget > 0.9:
        bot.send_alert(
            alert_type="预算告警",
            message=f"今日消耗已达预算 {current_cost/budget:.0%}",
            level="warning",
            at_user_ids=["负责人的open_id"]
        )
```

---

## 七、注意事项

1. **安全性**：Webhook 地址相当于机器人的密码，请勿泄露
2. **频率限制**：飞书机器人有发送频率限制，避免短时间内大量发送
3. **消息格式**：卡片消息更美观，适合数据报告；文本消息适合简单通知
4. **签名校验**：生产环境建议开启签名校验

---

## 八、相关链接

- [飞书开放平台](https://open.feishu.cn/)
- [自定义机器人指南](https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot)
- [消息卡片搭建工具](https://open.feishu.cn/tool/cardbuilder)
