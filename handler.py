import os
import subprocess
import boto3
import shutil
import logging
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def handler(event, context):
    try:
        # Validate input
        required_fields = ["input_s3_url", "output_s3_bucket", "output_s3_key"]
        if not all(field in event for field in required_fields):
            raise ValueError(f"Missing required fields. Required: {required_fields}")

        # Setup directories
        input_dir = "/tmp/input_dir"
        output_dir = "/tmp/output_dir"
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        # Parse S3 URL
        input_s3_url = event["input_s3_url"]
        parsed_url = urlparse(input_s3_url)
        input_bucket = parsed_url.netloc.split('.')[0]
        input_key = parsed_url.path.lstrip('/')

        # Setup paths
        input_file = os.path.join(input_dir, "input.wav")
        
        # Initialize S3
        s3 = boto3.client('s3')

        # Download input file
        logger.info(f"Downloading from s3://{input_bucket}/{input_key}")
        s3.download_file(input_bucket, input_key, input_file)

        # Process with resemble-enhance
        logger.info("Processing with resemble-enhance")
        subprocess.run([
            "resemble-enhance",
            input_dir,
            output_dir
        ], check=True)

        # Find enhanced file
        enhanced_file = os.path.join(output_dir, "input_enhanced.wav")
        if not os.path.exists(enhanced_file):
            raise FileNotFoundError("Enhanced file was not created")

        # Upload result
        logger.info(f"Uploading to s3://{event['output_s3_bucket']}/{event['output_s3_key']}")
        s3.upload_file(enhanced_file, event['output_s3_bucket'], event['output_s3_key'])

        # Generate output URL
        output_url = f"https://{event['output_s3_bucket']}.s3.amazonaws.com/{event['output_s3_key']}"

        # Cleanup
        shutil.rmtree(input_dir)
        shutil.rmtree(output_dir)

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