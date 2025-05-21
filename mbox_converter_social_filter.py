import mailbox
import os
import re
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime, getaddresses
from datetime import datetime
import logging
from pathlib import Path
import argparse
import hashlib

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Keywords to identify social media emails (can be expanded) ---
# These are checked against the 'From' header (email address and display name)
SOCIAL_MEDIA_KEYWORDS = [
    "facebook.com", "facebookmail.com", "fb.com", "facebook",
    "instagram.com", "instagram",
    "linkedin.com", "linkedin", "linkedinmail.com", "jobalerts-noreply@linkedin.com", "LinkedIn Job Alerts",
    "twitter.com", "x.com", "twitter",
    "pinterest.com", "pinterest",
    "tiktok.com", "tiktok",
    "snapchat.com", "snapchat",
    "reddit.com", "redditmail.com", "reddit",
    "youtube.com", "youtube",
    "quora.com", "quora",
    "nextdoor.com", "nextdoor",
    "flipkart", "amazon India", "amazon.com", "amazon",
    "myntra", "myntra.com", "myntra",
    "gmail", "google", "google Play", "google photos", "google drive",
    "badoo.com", "badoo",
    "internshala", "topmate.io", "internshala.com", "tata", "jobscan",
    "alerts", "monsterindia.com", "foundit", "oracle.com", "careers", "no-reply", "job", "jobs", "india", "noreply", "participate",
    "cuvette", "cuvette.tech", "myworkdayjobs", "myworkday", "myworkday.com",
    "indeed.com", "indeed", "indeed.co.in", "indeed.co.uk", "indeed.co.jp", "tax", "crypto", "binance", "do_not_reply", "contests", "mercer", "hasura", "jobnotification",
    "credit", "CreditMantri", "creditmantri.com", "creditmantri",
    "zomato.com", "zomato", "swiggy.com", "swiggy",
    "unacademy.com", "unacademy", "cesc", "jio.com", "jio",
    "paytm.com", "paytm", "grammarly.com", "grammarly", "irctc.co.in", "irctc", "irctc.co.in",
    "snapdeal.com", "snapdeal", "snapdeal.com", "snapdeal", "toornament", "udemy.com", "udemy",
    "coursera.org", "coursera", "coursera.com", "coursera", "skillshare.com", "skillshare",
    "edx.org", "edx", "edx.com", "edx", "gifting", "gift", "giftcards", "skillshare.com", "skillshare",
    
    # Add more services or specific sender names if needed
    # "notifications@examplecompany.com",
]
# Convert to lowercase for case-insensitive matching
SOCIAL_MEDIA_KEYWORDS_LOWER = [keyword.lower() for keyword in SOCIAL_MEDIA_KEYWORDS]


def get_email_body(msg):
    """
    Extracts the plain text body from an email message.
    Tries to find a 'text/plain' part first.
    If not found, tries to find 'text/html' and convert it to basic plain text.
    """
    body = ""
    preferred_body = ""
    html_body_content = ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))

            if "attachment" not in content_disposition: # Ignore attachments
                if content_type == "text/plain":
                    try:
                        payload = part.get_payload(decode=True)
                        charset = part.get_content_charset() or 'utf-8'
                        preferred_body = payload.decode(charset, errors='replace')
                        # If plain text is found, we prefer it, so break
                        # unless it's an alternative part and html might be richer
                        # For simplicity now, we'll take the first good plain text.
                        break
                    except Exception as e:
                        logging.debug(f"Could not decode text/plain part: {e}") # Debug for less noise
                elif content_type == "text/html":
                    try:
                        payload = part.get_payload(decode=True)
                        charset = part.get_content_charset() or 'utf-8'
                        html_body_content = payload.decode(charset, errors='replace')
                    except Exception as e:
                        logging.debug(f"Could not decode text/html part: {e}") # Debug for less noise
    else: # Not a multipart message
        try:
            payload = msg.get_payload(decode=True)
            charset = msg.get_content_charset() or 'utf-8'
            single_part_body = payload.decode(charset, errors='replace')
            if msg.get_content_type() == "text/plain":
                preferred_body = single_part_body
            elif msg.get_content_type() == "text/html":
                html_body_content = single_part_body
        except Exception as e:
            logging.debug(f"Could not decode single part message: {e}") # Debug for less noise

    if preferred_body: # If plain text was found
        body = preferred_body
    elif html_body_content: # If only HTML was found, convert it
        try:
            # Basic HTML to text conversion
            text_body = re.sub(r'<style(?:\s[^>]*)?>.*?</style>', '', html_body_content, flags=re.DOTALL | re.IGNORECASE)
            text_body = re.sub(r'<script(?:\s[^>]*)?>.*?</script>', '', text_body, flags=re.DOTALL | re.IGNORECASE)
            text_body = re.sub(r'<head(?:\s[^>]*)?>.*?</head>', '', text_body, flags=re.DOTALL | re.IGNORECASE) # Remove head
            text_body = re.sub(r'<[^>]+>', ' ', text_body) # Remove all other tags, replace with space
            text_body = re.sub(r'\s+', ' ', text_body).strip() # Normalize whitespace
            body = text_body
        except Exception as e:
            logging.warning(f"Could not convert HTML to text: {e}")
            body = "" # Fallback to empty if conversion fails
            
    return body.strip()

def clean_filename_component(component_str, max_len=50):
    """Cleans a string to be part of a filename."""
    if not component_str:
        return "unknown"
    cleaned = re.sub(r'[\\/*?:"<>|]', "_", str(component_str)) # Ensure component_str is string
    cleaned = cleaned.replace("\n", "_").replace("\r", "_")
    return cleaned[:max_len].strip()

def is_social_media_email(msg_from_header_full):
    """Checks if the From header suggests a social media email."""
    if not msg_from_header_full:
        return False
    
    from_header_lower = str(msg_from_header_full).lower() # Ensure it's a string
    
    # getaddresses returns list of (realname, email_address)
    # We check both parts
    for realname, email_address in getaddresses([from_header_lower]):
        check_strings = []
        if email_address: check_strings.append(email_address)
        if realname: check_strings.append(realname)
        
        for check_str in check_strings:
            for keyword in SOCIAL_MEDIA_KEYWORDS_LOWER:
                if keyword in check_str:
                    return True
    return False


def process_mbox(mbox_file_path, output_dir):
    """
    Processes an MBOX file, extracts emails, prepends metadata,
    filters out social media and no-body emails, and saves them as .txt files.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    logging.info(f"Output directory: {output_path.resolve()}")
    
    try:
        mbox = mailbox.mbox(mbox_file_path, factory=BytesParser(policy=policy.default).parse)
    except Exception as e:
        logging.error(f"Error opening MBOX file {mbox_file_path}: {e}")
        return

    processed_count = 0
    skipped_no_body_count = 0
    skipped_social_count = 0
    error_count = 0

    for i, msg in enumerate(mbox):
        try:
            from_header_full = msg.get("From", "Unknown Sender") # This should be a string or Header object

            # --- Social Media Filter (applied first) ---
            if is_social_media_email(from_header_full):
                skipped_social_count += 1
                continue

            # --- Body Extraction (applied second) ---
            body = get_email_body(msg)
            if not body:
                skipped_no_body_count += 1
                continue
            
            # --- Header Extraction for Metadata ---
            date_str = msg.get("Date", "")
            email_date = None
            if date_str:
                try: 
                    email_date = parsedate_to_datetime(str(date_str)) # Ensure date_str is string
                except Exception: 
                    logging.debug(f"Could not parse date for email {i+1}") # Debug for less noise

            subject = str(msg.get("Subject", "No Subject"))
            # Ensure from_header_full is properly stringified for metadata
            from_display = str(from_header_full)
            
            to_headers_full_list = msg.get_all("To", [])
            to_display = ", ".join([str(h) for h in to_headers_full_list]) if to_headers_full_list else "Unknown Recipient"
            
            cc_headers_full_list = msg.get_all("Cc", [])
            cc_display = ", ".join([str(h) for h in cc_headers_full_list]) if cc_headers_full_list else ""


            message_id = str(msg.get("Message-ID", ""))

            # --- Metadata Prepending ---
            metadata_header_parts = []
            date_display_str = email_date.strftime('%Y-%m-%d %H:%M:%S %Z') if email_date else "Unknown"
            metadata_header_parts.append(f"Email Date: {date_display_str}")
            metadata_header_parts.append(f"From: {from_display}")
            metadata_header_parts.append(f"To: {to_display}")
            if cc_display: # Only add Cc if present and not empty
                 metadata_header_parts.append(f"Cc: {cc_display}")
            metadata_header_parts.append(f"Subject: {subject}")
            
            metadata_string = "\n".join(metadata_header_parts) + "\n\n--- Email Content ---\n"
            final_content = metadata_string + body

            # --- Filename Creation ---
            date_prefix = email_date.strftime("%Y%m%d_%H%M%S") if email_date else "NODATE"
            
            unique_id_part = clean_filename_component(message_id.strip("<>").replace("@", "_at_"), 70) if message_id else hashlib.md5((subject + body[:200]).encode('utf-8', errors='replace')).hexdigest()[:12]
            
            subject_part = clean_filename_component(subject, 40)
            filename = f"{date_prefix}_{subject_part}_{unique_id_part}.txt"
            filepath = output_path / filename

            try:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(final_content)
                processed_count += 1
                if (processed_count % 500 == 0) and processed_count > 0: # Log every 500 processed
                    logging.info(f"Processed {processed_count} emails matching filters...")
            except Exception as e:
                logging.error(f"Error writing file {filename}: {e}")
                error_count +=1
        except Exception as e:
            logging.error(f"Critical error processing email index {i}: {e}")
            error_count += 1
            # For debugging the error you saw: 'str' object has no attribute 'token_type'
            # This error usually comes from the email.header.decode_header function
            # if it receives a plain string instead of a Header object or bytes.
            # The use of BytesParser(policy=policy.default) should generally prevent this,
            # but let's log the problematic header if it happens.
            if "token_type" in str(e):
                logging.error(f"Problematic 'From' header for email index {i}: {from_header_full} (type: {type(from_header_full)})")


    logging.info(f"--- Processing Complete ---")
    logging.info(f"Successfully processed and saved: {processed_count} emails.")
    logging.info(f"Skipped (social media filter): {skipped_social_count} emails.")
    logging.info(f"Skipped (no body): {skipped_no_body_count} emails.")
    logging.info(f"Errors during processing: {error_count} emails.")
    logging.info(f"Output files are in: {output_path.resolve()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert MBOX to .txt files, filtering out social media and no-body emails.")
    parser.add_argument("mbox_file", help="Path to the MBOX file.")
    parser.add_argument("output_directory", help="Directory to save the processed .txt files.")
    
    args = parser.parse_args()
    
    process_mbox(args.mbox_file, args.output_directory)