import mailbox
import os
import re
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from datetime import datetime
import logging
from pathlib import Path
import argparse
import hashlib # For generating a unique ID if Message-ID is missing

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_email_body(msg):
    """
    Extracts the plain text body from an email message.
    Tries to find a 'text/plain' part first.
    If not found, tries to find 'text/html' and convert it to basic plain text.
    """
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))

            if "attachment" not in content_disposition:
                if content_type == "text/plain":
                    try:
                        payload = part.get_payload(decode=True)
                        charset = part.get_content_charset() or 'utf-8' # Default to utf-8
                        body = payload.decode(charset, errors='replace')
                        break # Prefer plain text
                    except Exception as e:
                        logging.warning(f"Could not decode text/plain part: {e}")
                        continue
                elif content_type == "text/html" and not body: # If plain text not found yet
                    try:
                        payload = part.get_payload(decode=True)
                        charset = part.get_content_charset() or 'utf-8'
                        html_body = payload.decode(charset, errors='replace')
                        # Basic HTML to text conversion (can be improved with BeautifulSoup)
                        text_body = re.sub('<style[^<]+?</style>', '', html_body, flags=re.DOTALL | re.IGNORECASE) # Remove style tags
                        text_body = re.sub('<[^<]+?>', ' ', text_body) # Remove all other tags
                        text_body = re.sub(r'\s+', ' ', text_body).strip() # Normalize whitespace
                        body = text_body
                    except Exception as e:
                        logging.warning(f"Could not decode/convert text/html part: {e}")
                        continue
    else: # Not a multipart message, try to get the payload directly
        try:
            payload = msg.get_payload(decode=True)
            charset = msg.get_content_charset() or 'utf-8'
            body = payload.decode(charset, errors='replace')
            if msg.get_content_type() == "text/html" and "<body" in body.lower(): # if it's html
                text_body = re.sub('<style[^<]+?</style>', '', body, flags=re.DOTALL | re.IGNORECASE)
                text_body = re.sub('<[^<]+?>', ' ', text_body)
                text_body = re.sub(r'\s+', ' ', text_body).strip()
                body = text_body
        except Exception as e:
            logging.warning(f"Could not decode single part message: {e}")

    return body.strip()

def clean_filename_component(component_str, max_len=50):
    """Cleans a string to be part of a filename."""
    if not component_str:
        return "unknown"
    # Remove or replace characters not suitable for filenames
    cleaned = re.sub(r'[\\/*?:"<>|]', "_", component_str)
    cleaned = cleaned.replace("\n", "_").replace("\r", "_")
    return cleaned[:max_len].strip()

def process_mbox(mbox_file_path, output_dir):
    """
    Processes an MBOX file, extracts emails, prepends metadata, and saves them as .txt files.
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
    skipped_count = 0

    for i, msg in enumerate(mbox):
        try:
            date_str = msg.get("Date", "")
            email_date = None
            if date_str:
                try:
                    email_date = parsedate_to_datetime(date_str)
                except Exception as e:
                    logging.warning(f"Could not parse date '{date_str}': {e}. Skipping date for this email.")

            subject = msg.get("Subject", "No Subject")
            sender = msg.get("From", "Unknown Sender")
            to = msg.get("To", "Unknown Recipient")
            message_id = msg.get("Message-ID")

            # Decode headers if they are encoded
            if isinstance(subject, bytes): subject = subject.decode('utf-8', errors='replace')
            if isinstance(sender, bytes): sender = sender.decode('utf-8', errors='replace')
            if isinstance(to, bytes): to = to.decode('utf-8', errors='replace')
            if isinstance(message_id, bytes): message_id = message_id.decode('utf-8', errors='replace')


            body = get_email_body(msg)

            if not body:
                logging.warning(f"Email {i+1} (Subject: {subject}) has no extractable body. Skipping.")
                skipped_count += 1
                continue

            # --- Crucial for RAG: Prepend metadata to the content ---
            metadata_header = []
            if email_date:
                metadata_header.append(f"Email Date: {email_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            else:
                metadata_header.append("Email Date: Unknown")
            metadata_header.append(f"From: {sender}")
            metadata_header.append(f"To: {to}")
            metadata_header.append(f"Subject: {subject}")
            
            # Add a clear separator
            metadata_string = "\n".join(metadata_header) + "\n\n--- Email Content ---\n"
            
            final_content = metadata_string + body

            # --- Create a unique and informative filename ---
            date_prefix = email_date.strftime("%Y%m%d_%H%M%S") if email_date else "NODATE"
            
            unique_id_part = ""
            if message_id:
                # Clean message ID for filename (remove <, >, @)
                unique_id_part = clean_filename_component(message_id.strip("<>").replace("@", "_at_"), 70)
            else:
                # Fallback if no Message-ID: hash a portion of the body and subject
                fallback_hash_content = subject + body[:200]
                unique_id_part = hashlib.md5(fallback_hash_content.encode('utf-8', errors='replace')).hexdigest()[:12]
                logging.warning(f"Message-ID missing for email with subject '{subject}'. Using hash '{unique_id_part}' as part of filename.")

            # Max length for subject part of filename to keep overall length reasonable
            subject_part = clean_filename_component(subject, 40)
            
            filename = f"{date_prefix}_{subject_part}_{unique_id_part}.txt"
            filepath = output_path / filename

            try:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(final_content)
                processed_count += 1
                if (processed_count % 100 == 0):
                    logging.info(f"Processed {processed_count} emails...")
            except Exception as e:
                logging.error(f"Error writing file {filename}: {e}")
                skipped_count +=1

        except Exception as e:
            logging.error(f"Error processing email {i+1}: {e}")
            skipped_count += 1
            # Optionally, save problematic emails for inspection:
            # try:
            #     problem_dir = output_path / "problematic_emails"
            #     problem_dir.mkdir(exist_ok=True)
            #     with open(problem_dir / f"email_{i+1}_error.eml", "wb") as f_err:
            #         f_err.write(msg.as_bytes())
            # except Exception as e_save:
            #     logging.error(f"Could not save problematic email {i+1}: {e_save}")


    logging.info(f"--- Processing Complete ---")
    logging.info(f"Successfully processed and saved: {processed_count} emails.")
    logging.info(f"Skipped emails (no body or error): {skipped_count} emails.")
    logging.info(f"Output files are in: {output_path.resolve()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert MBOX file to individual .txt files for RAG, with metadata prepended.")
    parser.add_argument("mbox_file", help="Path to the MBOX file.")
    parser.add_argument("output_directory", help="Directory to save the processed .txt files.")
    
    args = parser.parse_args()
    
    process_mbox(args.mbox_file, args.output_directory)