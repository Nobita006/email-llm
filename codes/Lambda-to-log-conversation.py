import json
import boto3
import os
from pathlib import Path
from datetime import datetime
import logging
import re # For sanitizing conversation ID for S3 path

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')

# Environment variables to be set in Lambda configuration
LOGGING_S3_BUCKET_NAME = os.environ.get('LOGGING_S3_BUCKET_NAME')
LOGGING_S3_BASE_PREFIX = os.environ.get('LOGGING_S3_BASE_PREFIX', 'conversation-logs') # Base prefix

def sanitize_for_s3_path(text, max_length=50):
    """Sanitizes a string to be safe for S3 path components."""
    if not text:
        return "unknown"
    # Remove characters not typically allowed or problematic in S3 paths/filenames
    # Allow alphanumeric, hyphens, underscores, periods. Replace others with underscore.
    sanitized = re.sub(r'[^a-zA-Z0-9_.-]', '_', str(text))
    return sanitized[:max_length]

def lambda_handler(event, context):
    logger.info(f"Received event for logging: {json.dumps(event)}")

    text_to_log = ''
    # Use session ID from agent if available, otherwise generate one for the conversation
    # The agent should ideally pass $sessionId as conversationId
    conversation_id = 'unknown_conversation_' + datetime.now().strftime("%Y%m%d") 
    log_type = 'unknown_log_type' 
    # Using current timestamp for uniqueness within a turn
    timestamp_for_file = datetime.now().strftime("%Y%m%d_%H%M%S_%f") # Microseconds for uniqueness

    try:
        properties = event.get('requestBody', {}).get('application/json', {}).get('properties', [])
        logger.info(f"Extracted properties for logging: {json.dumps(properties)}")

        for prop in properties:
            prop_name = prop.get('name')
            prop_value = prop.get('value')

            if prop_name == 'textToLog':
                if isinstance(prop_value, str):
                    text_to_log = prop_value
            elif prop_name == 'conversationId': # Agent should pass $sessionId here
                if isinstance(prop_value, str) and prop_value.strip():
                    conversation_id = prop_value.strip()
            elif prop_name == 'logType': # "user_question" or "agent_response"
                if isinstance(prop_value, str) and prop_value.strip():
                    log_type = prop_value.strip()
        
        logger.info(f"Parsed textToLog: {'Present' if text_to_log else 'MISSING!'}")
        logger.info(f"Parsed conversationId: {conversation_id}")
        logger.info(f"Parsed logType: {log_type}")

    except Exception as e:
        logger.error(f"Error parsing input parameters for logging: {e}", exc_info=True)
        return format_agent_response(event, 400, {"error": f"Error parsing input parameters: {str(e)}"})

    if not text_to_log:
        logger.warning("No 'textToLog' provided.")
        return format_agent_response(event, 400, {"error": "No text provided to log."})
    if log_type not in ['user_question', 'agent_response']: # Validate logType
        logger.warning(f"Invalid 'logType' provided: {log_type}.")
        return format_agent_response(event, 400, {"error": "logType must be 'user_question' or 'agent_response'."})
    if not LOGGING_S3_BUCKET_NAME:
        logger.error("LOGGING_S3_BUCKET_NAME environment variable is not set.")
        return format_agent_response(event, 500, {"error": "Lambda configuration error: Target S3 bucket for logging not set."})

    sanitized_conv_id = sanitize_for_s3_path(conversation_id)
    sanitized_log_type = sanitize_for_s3_path(log_type)

    filename = f"{sanitized_log_type}_{timestamp_for_file}.txt"
    # S3 Key: base_prefix/sanitized_conversation_id_folder/filename.txt
    s3_key = str(Path(LOGGING_S3_BASE_PREFIX) / sanitized_conv_id / filename).replace("\\", "/")

    try:
        logger.info(f"Saving log to s3://{LOGGING_S3_BUCKET_NAME}/{s3_key}")
        s3_client.put_object(
            Bucket=LOGGING_S3_BUCKET_NAME,
            Key=s3_key,
            Body=text_to_log.encode('utf-8'),
            ContentType='text/plain'
        )
        
        s3_log_path = f"s3://{LOGGING_S3_BUCKET_NAME}/{s3_key}"
        success_message = f"Successfully logged '{log_type}' for conversation '{conversation_id}' to {s3_log_path}"
        logger.info(success_message)
        
        response_data = {
            "statusMessage": success_message,
            "s3LogPath": s3_log_path,
            "loggedConversationId": conversation_id, # Echo back for clarity
            "loggedLogType": log_type
        }
        return format_agent_response(event, 200, response_data)

    except Exception as e:
        logger.error(f"An error occurred while saving log to S3: {e}", exc_info=True)
        return format_agent_response(event, 500, {"error": f"Internal server error during S3 save: {str(e)}"})

def format_agent_response(event_payload, http_status_code, response_data):
    response_body_content = {
        'application/json': {
            'body': json.dumps(response_data)
        }
    }
    action_group_name = event_payload.get('actionGroup', {}) # Get the whole dict
    if isinstance(action_group_name, dict):
        action_group_name = action_group_name.get('actionGroupName') # Extract the name
    if not action_group_name: # Default if still not found
        action_group_name = event_payload.get('actionGroup', 'UnknownActionGroup_Log')


    api_path = event_payload.get('apiPath', '/logConversationTurn') # Default to your expected API path
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
    logger.info(f"Returning agent response for logging action: {json.dumps(agent_response)}")
    return agent_response