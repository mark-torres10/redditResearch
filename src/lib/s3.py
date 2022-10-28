"""Basic s3 access functionalities."""
import boto3

def s3_client(aws_access_key_id: str, aws_secret: str):
    """Returns s3 client. Credentials should be stored in .env file.
    
    Args:
    :param aws_access_key_id
    :param aws_secret
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret
    )
    return s3

def download_file(client, bucket, key):
    """Download file from s3."""
    pass


def dump_file(client, file, bucket, key):
    """Dump file to S3."""
    pass
