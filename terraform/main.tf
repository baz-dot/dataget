# GCP 基础设施配置
# 用于 XMP/ADX 数据采集项目

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# 变量定义
variable "project_id" {
  description = "GCP 项目 ID"
  type        = string
}

variable "region" {
  description = "GCP 区域"
  type        = string
  default     = "asia-east1"
}

variable "environment" {
  description = "环境标识 (dev/prod)"
  type        = string
  default     = "dev"
}

# GCS 存储桶 - XMP 素材数据
resource "google_storage_bucket" "xmp_data" {
  name          = "${var.project_id}-xmp-data-${var.environment}"
  location      = var.region
  force_destroy = var.environment == "dev"

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90  # 90天后转为冷存储
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365  # 1年后转为归档存储
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }

  labels = {
    environment = var.environment
    purpose     = "xmp-material-data"
  }
}

# GCS 存储桶 - ADX 素材视频
resource "google_storage_bucket" "adx_videos" {
  name          = "${var.project_id}-adx-videos-${var.environment}"
  location      = var.region
  force_destroy = var.environment == "dev"

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 180  # 180天后转为冷存储
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    environment = var.environment
    purpose     = "adx-video-storage"
  }
}

# BigQuery 数据集
resource "google_bigquery_dataset" "dataget" {
  dataset_id  = "dataget_${var.environment}"
  description = "数据采集项目数据集"
  location    = var.region

  labels = {
    environment = var.environment
  }

  # 默认表过期时间（可选）
  # default_table_expiration_ms = 3600000 * 24 * 365  # 1年
}

# BigQuery 表 - XMP 素材数据
resource "google_bigquery_table" "xmp_materials" {
  dataset_id = google_bigquery_dataset.dataget.dataset_id
  table_id   = "xmp_materials"

  schema = jsonencode([
    { name = "user_material_id", type = "STRING", mode = "NULLABLE" },
    { name = "user_material_name", type = "STRING", mode = "NULLABLE" },
    { name = "xmp_material_id", type = "STRING", mode = "NULLABLE" },
    { name = "channel", type = "STRING", mode = "NULLABLE" },
    { name = "format", type = "STRING", mode = "NULLABLE" },
    { name = "designer_name", type = "STRING", mode = "NULLABLE" },
    { name = "impression", type = "INTEGER", mode = "NULLABLE" },
    { name = "click", type = "INTEGER", mode = "NULLABLE" },
    { name = "conversion", type = "INTEGER", mode = "NULLABLE" },
    { name = "cost", type = "FLOAT", mode = "NULLABLE" },
    { name = "currency", type = "STRING", mode = "NULLABLE" },
    { name = "ecpm", type = "FLOAT", mode = "NULLABLE" },
    { name = "click_rate", type = "FLOAT", mode = "NULLABLE" },
    { name = "conversion_rate", type = "FLOAT", mode = "NULLABLE" },
    { name = "material_create_time", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "fetched_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "page_number", type = "INTEGER", mode = "NULLABLE" },
  ])

  labels = {
    environment = var.environment
  }
}

# BigQuery 表 - QuickBI 数据
resource "google_bigquery_table" "quickbi_data" {
  dataset_id = google_bigquery_dataset.dataget.dataset_id
  table_id   = "quickbi_data"

  schema = jsonencode([
    { name = "report_id", type = "STRING", mode = "NULLABLE" },
    { name = "report_name", type = "STRING", mode = "NULLABLE" },
    { name = "data_json", type = "STRING", mode = "NULLABLE" },
    { name = "fetched_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])

  labels = {
    environment = var.environment
  }
}

# BigQuery 表 - ADX 素材数据
resource "google_bigquery_table" "adx_materials" {
  dataset_id = google_bigquery_dataset.dataget.dataset_id
  table_id   = "adx_materials"

  schema = jsonencode([
    { name = "material_id", type = "STRING", mode = "NULLABLE" },
    { name = "material_name", type = "STRING", mode = "NULLABLE" },
    { name = "video_url", type = "STRING", mode = "NULLABLE" },
    { name = "gcs_path", type = "STRING", mode = "NULLABLE" },
    { name = "duration", type = "FLOAT", mode = "NULLABLE" },
    { name = "resolution", type = "STRING", mode = "NULLABLE" },
    { name = "file_size", type = "INTEGER", mode = "NULLABLE" },
    { name = "fetched_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])

  labels = {
    environment = var.environment
  }
}

# 服务账号
resource "google_service_account" "dataget_sa" {
  account_id   = "dataget-${var.environment}"
  display_name = "DataGet Service Account (${var.environment})"
  description  = "用于数据采集任务的服务账号"
}

# 服务账号权限 - GCS
resource "google_storage_bucket_iam_member" "xmp_data_writer" {
  bucket = google_storage_bucket.xmp_data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataget_sa.email}"
}

resource "google_storage_bucket_iam_member" "adx_videos_writer" {
  bucket = google_storage_bucket.adx_videos.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataget_sa.email}"
}

# 服务账号权限 - BigQuery
resource "google_bigquery_dataset_iam_member" "dataget_editor" {
  dataset_id = google_bigquery_dataset.dataget.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dataget_sa.email}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataget_sa.email}"
}

# 输出
output "xmp_bucket_name" {
  value       = google_storage_bucket.xmp_data.name
  description = "XMP 数据 GCS 存储桶名称"
}

output "adx_bucket_name" {
  value       = google_storage_bucket.adx_videos.name
  description = "ADX 视频 GCS 存储桶名称"
}

output "bigquery_dataset" {
  value       = google_bigquery_dataset.dataget.dataset_id
  description = "BigQuery 数据集 ID"
}

output "service_account_email" {
  value       = google_service_account.dataget_sa.email
  description = "服务账号邮箱"
}
