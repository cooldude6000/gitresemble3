import os
import subprocess
import boto3
import logging
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def handler(event, context):
    try:
        required_fields = ["input_s3_url", "output_s3_bucket", "output_s3_key"]
        if not all(field in event for field in required_fields):
            raise ValueError(f"Missing required fields. Required: {required_fields}")

        input_s3_url = event["input_s3_url"]
        output_s3_bucket = event["output_s3_bucket"]
        output_s3_key = event["output_s3_key"]

        parsed_url = urlparse(input_s3_url)
        input_bucket = parsed_url.netloc.split('.')[0]
        input_key = parsed_url.path.lstrip('/')

        os.makedirs("/tmp", exist_ok=True)
        input_local = "/tmp/input.wav"
        enhanced_file = "/tmp/input_enhanced.wav"

        s3 = boto3.client('s3')

        logger.info(f"Downloading from s3://{input_bucket}/{input_key}")
        s3.download_file(input_bucket, input_key, input_local)

        logger.info("Processing with resemble-enhance")
        subprocess.run([
            "resemble-enhance",
            input_local,
            "/tmp"
        ], check=True)

        if not os.path.exists(enhanced_file):
            raise FileNotFoundError("Enhanced file was not created")

        logger.info(f"Uploading to s3://{output_s3_bucket}/{output_s3_key}")
        s3.upload_file(enhanced_file, output_s3_bucket, output_s3_key)

        output_url = f"https://{output_s3_bucket}.s3.amazonaws.com/{output_s3_key}"

        os.remove(input_local)
        os.remove(enhanced_file)

        return {
            "status": "success",
            "output_url": output_url
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }