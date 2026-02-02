# XMP 多任务 Token 冲突优化方案

## 问题根源

多个 Cloud Run Job 使用同一个 XMP 账号登录，导致 Token 互相挤掉：

1. `xmp-data-scraper`: 每 6 小时 (00:00, 06:00, 12:00, 18:00)
2. `xmp-internal-scraper`: 每 30 分钟
3. `xmp-internal-scraper-daily`: 每天 02:00

**冲突时间**: 14:00 左右，多个任务同时执行导致 Token 失效。

---

## 方案 1: 统一 Token 管理（推荐）

### 核心思路
所有任务共享同一个 Token，存储在 GCS 中，避免重复登录。

### 实现步骤

#### 1.1 修改 Token 存储逻辑

在 `xmp/xmp_scheduler.py` 中添加 GCS Token 管理：

```python
def _get_token_from_gcs(self):
    """从 GCS 读取 Token"""
    try:
        from google.cloud import storage
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'xmp_raw_data_storage')
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob('xmp/token.json')

        if blob.exists():
            token_data = json.loads(blob.download_as_string())
            self.bearer_token = token_data.get('bearer_token')
            self.token_timestamp = token_data.get('timestamp')
            print(f"[XMP] 从 GCS 加载 Token")
            return True
    except Exception as e:
        print(f"[XMP] 从 GCS 读取 Token 失败: {e}")
    return False

def _save_token_to_gcs(self):
    """保存 Token 到 GCS"""
    try:
        from google.cloud import storage
        bucket_name = os.getenv('GCS_BUCKET_NAME', 'xmp_raw_data_storage')
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob('xmp/token.json')

        token_data = {
            'bearer_token': self.bearer_token,
            'timestamp': self.token_timestamp
        }
        blob.upload_from_string(json.dumps(token_data))
        print(f"[XMP] Token 已保存到 GCS")
    except Exception as e:
        print(f"[XMP] 保存 Token 到 GCS 失败: {e}")
```

#### 1.2 修改 Token 加载逻辑

在 `login_and_get_token()` 方法中优先从 GCS 读取：

```python
async def login_and_get_token(self):
    # 1. 先尝试从 GCS 读取
    if self._get_token_from_gcs():
        if not self._should_refresh_token():
            return

    # 2. Token 不存在或已过期，重新登录
    print("[XMP] 需要重新登录获取 Token")
    # ... 原有登录逻辑 ...

    # 3. 登录成功后保存到 GCS
    self._save_token_to_gcs()
```

---

## 方案 2: 错峰调度（快速修复）

### 核心思路
调整任务执行时间，避免冲突。

### 调整方案

#### 2.1 `xmp-data-scraper` (Playwright 爬虫)
- **当前**: 每 6 小时 (00:00, 06:00, 12:00, 18:00)
- **建议**: 改为每天 1 次，凌晨 3:00
- **原因**: 这个任务耗时长，不需要高频执行

```bash
# deploy.sh 第 70 行
--schedule "0 3 * * *"  # 改为每天 03:00
```

#### 2.2 `xmp-internal-scraper` (API 采集)
- **当前**: 每 30 分钟
- **建议**: 保持不变
- **原因**: 需要实时数据

#### 2.3 `xmp-internal-scraper-daily` (T-1 采集)
- **当前**: 每天 02:00
- **建议**: 改为 01:00
- **原因**: 避免与其他任务冲突

```bash
# deploy_xmp_internal.sh 第 133 行
--schedule "0 1 * * *"  # 改为每天 01:00
```

### 调整后的时间表

| 时间 | 任务 | 说明 |
|------|------|------|
| 01:00 | xmp-internal-scraper-daily | T-1 日数据采集 |
| 03:00 | xmp-data-scraper | Playwright 素材爬虫 |
| 每 30 分钟 | xmp-internal-scraper | 实时 API 采集 |

**优点**: 避开 14:00 冲突时间，任务间隔至少 2 小时。

---

## 方案 3: 禁用不必要的任务

### 分析

`xmp-data-scraper` (Playwright 爬虫) 可能已经不需要了，因为：
- `xmp-internal-scraper` 已经通过 API 采集所有数据
- Playwright 爬虫耗时长、不稳定
- 数据重复

### 建议

**暂停 `xmp-data-scraper` 任务**：

```bash
gcloud scheduler jobs pause xmp-data-scraper-scheduler \
    --location asia-northeast3 \
    --project fleet-blend-469520-n7
```

---

## 推荐实施顺序

### 第一步：快速修复（立即执行）
使用**方案 2 + 方案 3**：
1. 暂停 `xmp-data-scraper` 任务
2. 调整 `xmp-internal-scraper-daily` 为 01:00

### 第二步：长期优化（后续实施）
实施**方案 1**：统一 Token 管理

---

## 立即执行的命令

```bash
# 1. 暂停 xmp-data-scraper
gcloud scheduler jobs pause xmp-data-scraper-scheduler \
    --location asia-northeast3 \
    --project fleet-blend-469520-n7

# 2. 调整 xmp-internal-scraper-daily 时间为 01:00
gcloud scheduler jobs update http xmp-internal-scraper-daily-scheduler \
    --location asia-northeast3 \
    --schedule "0 1 * * *" \
    --time-zone "Asia/Shanghai" \
    --project fleet-blend-469520-n7
```
