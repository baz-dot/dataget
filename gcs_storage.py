"""
GCS Storage Module
将数据上传到 Google Cloud Storage
"""

import os
import json
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv


class GCSUploader:
    """Google Cloud Storage 上传器"""

    def __init__(self, bucket_name: str, credentials_path: str = None):
        """
        初始化 GCS 上传器

        Args:
            bucket_name: GCS bucket 名称
            credentials_path: 服务账号密钥文件路径（可选，默认使用环境变量）
        """
        self.bucket_name = bucket_name

        # 设置认证
        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def upload_json(self, data: dict, blob_path: str) -> str:
        """
        上传 JSON 数据到 GCS

        Args:
            data: 要上传的数据（字典）
            blob_path: GCS 中的文件路径

        Returns:
            上传后的 GCS URI
        """
        blob = self.bucket.blob(blob_path)
        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        blob.upload_from_string(json_str, content_type='application/json')

        gcs_uri = f"gs://{self.bucket_name}/{blob_path}"
        print(f"✓ 已上传到 GCS: {gcs_uri}")
        return gcs_uri

    def upload_file(self, local_path: str, blob_path: str) -> str:
        """
        上传本地文件到 GCS

        Args:
            local_path: 本地文件路径
            blob_path: GCS 中的文件路径

        Returns:
            上传后的 GCS URI
        """
        blob = self.bucket.blob(blob_path)
        blob.upload_from_filename(local_path)

        gcs_uri = f"gs://{self.bucket_name}/{blob_path}"
        print(f"✓ 已上传到 GCS: {gcs_uri}")
        return gcs_uri

    def generate_xmp_blob_path(self, data_type: str = "material") -> str:
        """
        生成 XMP 数据的 GCS 路径

        Args:
            data_type: 数据类型 (material, account 等)

        Returns:
            GCS blob 路径
        """
        now = datetime.now()
        return f"xmp/{data_type}/{now.strftime('%Y/%m/%d')}/xmp_{data_type}_{now.strftime('%Y%m%d_%H%M%S')}.json"


def upload_xmp_data_to_gcs(data: dict, bucket_name: str, credentials_path: str = None) -> str:
    """
    便捷函数：上传 XMP 数据到 GCS

    Args:
        data: XMP 数据
        bucket_name: GCS bucket 名称
        credentials_path: 服务账号密钥文件路径

    Returns:
        GCS URI
    """
    uploader = GCSUploader(bucket_name, credentials_path)
    blob_path = uploader.generate_xmp_blob_path("material")
    return uploader.upload_json(data, blob_path)


if __name__ == "__main__":
    # 测试代码
    load_dotenv()

    bucket_name = os.getenv('GCS_BUCKET_NAME')
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

    if not bucket_name:
        print("错误: 请在 .env 文件中设置 GCS_BUCKET_NAME")
        exit(1)

    # 测试上传
    test_data = {
        "code": 200,
        "data": {
            "total": 1,
            "pages": [{"page": 1, "list": [{"test": "data"}]}]
        },
        "message": "test"
    }

    try:
        uri = upload_xmp_data_to_gcs(test_data, bucket_name, credentials_path)
        print(f"测试上传成功: {uri}")
    except Exception as e:
        print(f"测试上传失败: {e}")
