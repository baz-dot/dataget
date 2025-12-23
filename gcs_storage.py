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

    def generate_xmp_blob_path(self, data_type: str = "material", batch_id: str = None) -> str:
        """
        生成 XMP 数据的 GCS 路径

        Args:
            data_type: 数据类型 (material, account 等)
            batch_id: 批次 ID（时间戳格式，如 20251218_171800）

        Returns:
            GCS blob 路径
        """
        now = datetime.now()
        # 如果没有提供 batch_id，生成一个新的
        if not batch_id:
            batch_id = now.strftime('%Y%m%d_%H%M%S')
        # 使用批次 ID 作为子目录
        # 格式: xmp/batch_20251218_171800/data.json
        return f"xmp/batch_{batch_id}/data.json"

    def generate_quickbi_blob_path(self, data_type: str = "campaigns", batch_id: str = None) -> str:
        """
        生成 Quick BI 数据的 GCS 路径

        Args:
            data_type: 数据类型 (campaigns 等)
            batch_id: 批次 ID（时间戳格式，如 20251218_171800）

        Returns:
            GCS blob 路径
        """
        now = datetime.now()
        # 如果没有提供 batch_id，生成一个新的
        if not batch_id:
            batch_id = now.strftime('%Y%m%d_%H%M%S')
        # 使用批次 ID 作为子目录
        # 格式: quickbi/batch_20251218_171800/data.json
        return f"quickbi/batch_{batch_id}/data.json"

    def generate_adx_blob_path(self, data_type: str = "material", filename: str = None, batch_id: str = None) -> str:
        """
        生成 ADX (DataEye) 数据的 GCS 路径

        Args:
            data_type: 数据类型 (material, video 等)
            filename: 文件名（用于视频）
            batch_id: 批次 ID（时间戳格式，如 20251218_171800）

        Returns:
            GCS blob 路径
        """
        now = datetime.now()
        # 如果没有提供 batch_id，生成一个新的
        if not batch_id:
            batch_id = now.strftime('%Y%m%d_%H%M%S')

        # 使用批次 ID 作为子目录，每次抓取的数据存储在独立目录
        # 格式: adx/batch_20251218_171800/video/xxx.mp4
        #       adx/batch_20251218_171800/data.json
        if data_type == "video" and filename:
            return f"adx/batch_{batch_id}/video/{filename}"
        return f"adx/batch_{batch_id}/data.json"

    def upload_adx_video(self, local_path: str, material_id: str, batch_id: str = None) -> str:
        """
        上传 ADX 视频到 GCS

        Args:
            local_path: 本地视频文件路径
            material_id: 素材 ID
            batch_id: 批次 ID

        Returns:
            上传后的 GCS URI
        """
        filename = f"{material_id}.mp4"
        blob_path = self.generate_adx_blob_path("video", filename, batch_id)
        return self.upload_file(local_path, blob_path)

    def upload_adx_videos_batch(self, video_dir: str, batch_id: str = None) -> list:
        """
        批量上传 ADX 视频到 GCS

        Args:
            video_dir: 视频目录路径
            batch_id: 批次 ID（所有视频使用同一个批次）

        Returns:
            上传成功的 GCS URI 列表
        """
        import os
        uploaded = []

        if not os.path.exists(video_dir):
            print(f"视频目录不存在: {video_dir}")
            return uploaded

        # 如果没有提供 batch_id，生成一个新的
        if not batch_id:
            batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')

        video_files = [f for f in os.listdir(video_dir) if f.endswith('.mp4')]
        print(f"找到 {len(video_files)} 个视频文件待上传")
        print(f"批次 ID: {batch_id}")

        for i, filename in enumerate(video_files, 1):
            local_path = os.path.join(video_dir, filename)
            material_id = filename.replace('.mp4', '')

            try:
                print(f"[{i}/{len(video_files)}] 上传 {filename}...")
                uri = self.upload_adx_video(local_path, material_id, batch_id)
                uploaded.append(uri)
            except Exception as e:
                print(f"  ✗ 上传失败: {e}")

        print(f"✓ 共上传 {len(uploaded)} 个视频到 GCS")
        print(f"  批次目录: gs://{self.bucket_name}/adx/batch_{batch_id}/")
        return uploaded


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
