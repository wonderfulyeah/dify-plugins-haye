import io
from typing import Any, Generator

import requests
from Crypto.SelfTest.Cipher.test_CBC import file_name
from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage
from minio import Minio


class MinioWriterTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage, None, dict[str, str] | None]:

        # 从参数中获取内容和对象名称
        file = tool_parameters.get("file")
        print(file)
        if not file:
            yield self.create_json_message({"message": "file are required"})
            return

        # 从运行时凭据中获取MinIO配置
        access_key = tool_parameters.get("access_key")
        print(access_key)

        secret_key = tool_parameters.get("secret_key")
        print(secret_key)

        endpoint = tool_parameters.get("endpoint")
        print(endpoint)
        bucket_name = tool_parameters.get("bucket_name")
        print(bucket_name)
        file_path = tool_parameters.get("path")
        print(file_path)

        try:

            file_url = f"{file.url}"
            file_name = file.filename

            mime_type = file.mime_type

            if file_path:
                file_name = f"{file_path}/{file_name}"

            # 初始化MinIO客户端
            minio_client = Minio(
                endpoint.replace("http://", "").replace("https://", ""),
                access_key=access_key,
                secret_key=secret_key,
                secure=endpoint.startswith("https://")
            )

            # 确保存储桶存在
            if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)

            if not file_url:
                return {"error": "No file URL provided"}

            response = requests.get(file_url, timeout=30)
            response.raise_for_status()  # 抛出 HTTP 错误（如 404、500）
            file_content = response.content  # 二进制内容

            file_stream = io.BytesIO(file_content)
            file_stream.seek(0)  # 重置文件指针到开头

            # 将内容转为字节流并上传
            minio_client.put_object(
                bucket_name,
                file_name,
                file_stream,
                length=len(file_content),
                content_type=mime_type
            )

            message = f"{bucket_name}/{file_name}"

            # 返回成功消息
            yield self.create_text_message(message)
        except Exception as e:
            yield self.create_json_message({"message": f"Write Failed: {str(e)}"})
            return
        return {
            "status": "success"
        }
