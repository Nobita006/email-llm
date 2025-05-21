import json
import boto3
import os
import shutil
from pathlib import Path
from datetime import datetime
import zipfile
import logging
from urllib.parse import urlparse

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')

# Environment variables (set these in your Lambda function's configuration)
TARGET_S3_BUCKET_NAME = os.environ.get('TARGET_S3_BUCKET_NAME')
ARCHIVE_S3_PREFIX = os.environ.get('ARCHIVE_S3_PREFIX', 'email_archives') # Optional prefix

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    # Initialize parameters
    s3_uris = []
    query_context = 'general_query' # Default value

    try:
        # Parameters from Bedrock Agent are passed in a list of properties
        properties = event.get('requestBody', {}).get('application/json', {}).get('properties', [])
        logger.info(f"Extracted properties: {json.dumps(properties)}")

        for prop in properties:
            prop_name = prop.get('name')
            prop_value = prop.get('value')
            # prop_type = prop.get('type') # Available for debugging

            if prop_name == 's3_uris':
                # Value for an array type from the agent should be a list directly.
                # If it's a string that *looks* like a list, it's usually an issue
                # with the OpenAPI spec or how the agent constructed the call.
                if isinstance(prop_value, list):
                    s3_uris = [str(uri) for uri in prop_value if isinstance(uri, str)] # Ensure all items are strings
                elif isinstance(prop_value, str): # Fallback: if agent sends it as a JSON string list
                    try:
                        parsed_list = json.loads(prop_value)
                        if isinstance(parsed_list, list):
                            s3_uris = [str(uri) for uri in parsed_list if isinstance(uri, str)]
                        else:
                            logger.warning(f"'s3_uris' string did not decode to a list: {prop_value}")
                    except json.JSONDecodeError:
                        logger.warning(f"Could not decode 's3_uris' string as JSON: {prop_value}")
                else:
                    logger.warning(f"'s3_uris' property is not a list or a decodable string. Type: {type(prop_value)}, Value: {prop_value}")
            
            elif prop_name == 'query_context':
                if isinstance(prop_value, str):
                    query_context = prop_value
                else:
                    logger.warning(f"'query_context' property is not a string. Type: {type(prop_value)}, Value: {prop_value}")
        
        logger.info(f"Parsed s3_uris: {s3_uris}")
        logger.info(f"Parsed query_context: {query_context}")

    except Exception as e:
        logger.error(f"Error parsing input parameters from event's properties: {e}", exc_info=True)
        logger.error(f"Full event structure for parsing error: {json.dumps(event)}")
        return format_agent_response(event, 400, {"error": f"Error parsing input parameters: {str(e)}"})

    if not s3_uris:
        logger.warning("No S3 URIs provided or parsed from the input.")
        return format_agent_response(event, 400, {"error": "No S3 URIs provided to archive."})

    if not TARGET_S3_BUCKET_NAME:
        logger.error("TARGET_S3_BUCKET_NAME environment variable is not set.")
        return format_agent_response(event, 500, {"error": "Lambda configuration error: Target S3 bucket not set."})

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    query_slug = "".join(filter(str.isalnum, query_context))[:30]
    local_tmp_archive_folder_name = f"retrieved_{query_slug}_{timestamp}"
    local_tmp_path = Path("/tmp") / local_tmp_archive_folder_name
    
    try:
        local_tmp_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created temporary local folder: {local_tmp_path}")

        downloaded_files_count = 0
        for s3_uri in s3_uris:
            if not isinstance(s3_uri, str) or not s3_uri.startswith("s3://"):
                logger.warning(f"Invalid S3 URI format encountered: {s3_uri}. Skipping.")
                continue
            try:
                parsed_uri = urlparse(s3_uri)
                source_bucket = parsed_uri.netloc
                source_key = parsed_uri.path.lstrip('/')
                
                if not source_bucket or not source_key:
                    logger.warning(f"Invalid S3 URI structure after parsing: {s3_uri}. Skipping.")
                    continue

                filename = Path(source_key).name
                local_file_path = local_tmp_path / filename
                
                logger.info(f"Downloading s3://{source_bucket}/{source_key} to {local_file_path}")
                s3_client.download_file(source_bucket, source_key, str(local_file_path))
                downloaded_files_count += 1
            except Exception as e:
                logger.error(f"Failed to download {s3_uri}: {e}")
        
        if downloaded_files_count == 0:
            logger.warning("No files were successfully downloaded from the provided URIs.")
            return format_agent_response(event, 404, {"message": "No files were downloaded (either URIs were invalid or download failed), nothing to archive."})

        zip_filename_base = local_tmp_archive_folder_name
        zip_filepath_local = Path("/tmp") / f"{zip_filename_base}.zip"
        
        logger.info(f"Zipping contents of {local_tmp_path} to {zip_filepath_local}")
        with zipfile.ZipFile(zip_filepath_local, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for item in local_tmp_path.rglob('*'):
                if item.is_file():
                    zipf.write(item, item.relative_to(local_tmp_path))
        
        target_s3_key = str(Path(ARCHIVE_S3_PREFIX) / f"{zip_filename_base}.zip").replace("\\", "/")
        logger.info(f"Uploading {zip_filepath_local} to s3://{TARGET_S3_BUCKET_NAME}/{target_s3_key}")
        s3_client.upload_file(str(zip_filepath_local), TARGET_S3_BUCKET_NAME, target_s3_key)
        
        archive_s3_path = f"s3://{TARGET_S3_BUCKET_NAME}/{target_s3_key}"
        success_message = f"Successfully archived {downloaded_files_count} email(s) related to '{query_context}' to {archive_s3_path}"
        logger.info(success_message)
        
        response_data = {
            "statusMessage": success_message,
            "s3ArchivePath": archive_s3_path,
            "filesArchived": downloaded_files_count
        }
        return format_agent_response(event, 200, response_data)

    except Exception as e:
        logger.error(f"An error occurred during processing: {e}", exc_info=True)
        return format_agent_response(event, 500, {"error": f"Internal server error: {str(e)}"})
    finally:
        if 'local_tmp_path' in locals() and local_tmp_path.exists(): # Check if defined before using
            logger.info(f"Cleaning up temporary folder: {local_tmp_path}")
            shutil.rmtree(local_tmp_path, ignore_errors=True) # Add ignore_errors for robustness
        if 'zip_filepath_local' in locals() and zip_filepath_local.exists():
            logger.info(f"Cleaning up temporary zip file: {zip_filepath_local}")
            try:
                os.remove(zip_filepath_local)
            except OSError as e_remove:
                logger.error(f"Error removing zip file {zip_filepath_local}: {e_remove}")


def format_agent_response(event_payload, http_status_code, response_data):
    response_body_content = {
        'application/json': {
            'body': json.dumps(response_data) # Ensure response_data is always JSON serializable
        }
    }
    action_group_name = event_payload.get('actionGroup', 'UnknownActionGroup') # Default if not present
    if isinstance(action_group_name, dict): # sometimes it's a dict with 'actionGroupName'
        action_group_name = action_group_name.get('actionGroupName', 'UnknownActionGroup')

    api_path = event_payload.get('apiPath', 'UnknownApiPath')
    http_method = event_payload.get('httpMethod', 'POST')

    agent_response = {
        'messageVersion': '1.0', 
        'response': {
            'actionGroup': action_group_name,
            'apiPath': api_path,
            'httpMethod': http_method,
            'httpStatusCode': http_status_code,
            'responseBody': response_body_content
        },
        'sessionAttributes': event_payload.get('sessionAttributes', {}),
        'promptSessionAttributes': event_payload.get('promptSessionAttributes', {})
    }
    logger.info(f"Returning agent response: {json.dumps(agent_response)}")
    return agent_response