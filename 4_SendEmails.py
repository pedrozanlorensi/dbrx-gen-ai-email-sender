# Databricks notebook source
# Send emails for generated email content

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple
import time
import json
import requests
import os

CATALOG_NAME = 'pedroz_catalog'
SCHEMA_NAME = 'email_conversations'
OUTPUT_TABLE_NAME = 'generated_emails'

# Logic App configuration
LOGICAPP_URL = "<TBD-ADD-LOGICAPP-EMAIL-SENDER-FLOW-URL-HERE>"
REQUEST_TIMEOUT = 30

# COMMAND ----------

import re

def send_email(customer_email: str, email_content: str, subject: str = None) -> Tuple[str, bool, str]:
  '''
  Send an email to the customer via Azure Logic App.
  Returns tuple of (email, success: bool, error_message: str)
  '''
  try:
    # Extract subject from content if not provided
    if subject is None:
      if "<title>" in email_content.lower():
        match = re.search(r'<title>(.*?)</title>', email_content, re.IGNORECASE)
        subject = match.group(1) if match else "Your Personalized Update"
      else:
        subject = "Your Personalized Update"
    
    # Determine if content is HTML
    is_html = "<html" in email_content.lower() or "<body" in email_content.lower() or "<p>" in email_content.lower()
    
    payload = {
      "subject": subject,
      "Subject": subject,
      "receiver": [customer_email],
      "message": email_content if not is_html else "Please view this email in an HTML-compatible client.",
      "message_html": email_content if is_html else f"<html><body><p>{email_content}</p></body></html>",
      "BodyEmail": email_content,
      "IsHtml": is_html
    }
    
    headers = {"Content-Type": "application/json"}
    
    resp = requests.post(
      LOGICAPP_URL, 
      headers=headers, 
      data=json.dumps(payload), 
      timeout=REQUEST_TIMEOUT
    )
    
    if resp.status_code in (200, 202):
      return (customer_email, True, "")
    else:
      return (customer_email, False, f"HTTP {resp.status_code}: {resp.text[:200]}")
      
  except requests.exceptions.Timeout:
    return (customer_email, False, "Request timed out")
  except requests.exceptions.RequestException as e:
    return (customer_email, False, str(e))
  except Exception as e:
    return (customer_email, False, str(e))


class AdaptiveEmailSender:
  '''
  Auto-tuning email sender with adaptive concurrency and rate limiting.
  Automatically adjusts based on success/failure rates.
  '''
  def __init__(self):
    self.max_workers = min(32, (os.cpu_count() or 4) * 4)  # Good default for I/O-bound
    self.current_delay = 0.0  # Start with no delay
    self.consecutive_failures = 0
    self.total_sent = 0
    self.total_failed = 0
  
  def _adjust_for_failure(self):
    '''Back off when failures occur'''
    self.consecutive_failures += 1
    if self.consecutive_failures >= 3:
      # Reduce workers and increase delay
      self.max_workers = max(2, self.max_workers // 2)
      self.current_delay = min(2.0, self.current_delay + 0.2)
  
  def _adjust_for_success(self):
    '''Speed up when things are going well'''
    self.consecutive_failures = 0
  
  def send_batch(self, emails: List[Tuple]) -> Tuple[List[str], List[Tuple[str, str]]]:
    '''Send a batch of emails with current settings'''
    successful = []
    failed = []
    
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
      future_to_email = {}
      for item in emails:
        # Apply current delay
        if self.current_delay > 0:
          time.sleep(self.current_delay / self.max_workers)
        
        if len(item) == 3:
          email, content, subject = item
          future = executor.submit(send_email, email, content, subject)
        else:
          email, content = item
          future = executor.submit(send_email, email, content)
        future_to_email[future] = email
      
      for future in as_completed(future_to_email):
        email, success, error = future.result()
        if success:
          successful.append(email)
          self._adjust_for_success()
        else:
          failed.append((email, error))
          self._adjust_for_failure()
    
    self.total_sent += len(successful)
    self.total_failed += len(failed)
    return successful, failed
  
  def get_optimal_batch_size(self, total_emails: int) -> int:
    '''Calculate optimal batch size based on total emails'''
    if total_emails <= 10:
      return total_emails
    elif total_emails <= 100:
      return min(25, total_emails)
    else:
      return 50  # Good balance between progress updates and efficiency


def update_sent_status(emails: List[str]) -> None:
  '''Update sent_at timestamp for a batch of emails.'''
  if not emails:
    return
  
  # Escape single quotes in emails and build IN clause
  emails_list = ", ".join([f"'{email.replace(chr(39), chr(39)+chr(39))}'" for email in emails])
  
  spark.sql(f"""
    UPDATE {CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}
    SET sent_at = current_timestamp()
    WHERE customer_email IN ({emails_list})
      AND sent_at IS NULL
  """)

# COMMAND ----------

# Read only UNSENT emails (sent_at IS NULL)
print("Checking for unsent emails...")

# First, check available columns in the table
table_columns = [col.name.lower() for col in spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}").schema]
has_subject = 'email_subject' in table_columns

# Build query with optional subject column
select_cols = "customer_email, email_content"
if has_subject:
  select_cols += ", email_subject"

unsent_emails_df = spark.sql(f"""
  SELECT {select_cols}
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}
  WHERE sent_at IS NULL
""")

total_count = unsent_emails_df.count()

if total_count == 0:
  print("✓ No unsent emails to process.")
else:
  # Initialize adaptive sender (auto-configures workers, batch size, rate limiting)
  sender = AdaptiveEmailSender()
  batch_size = sender.get_optimal_batch_size(total_count)
  
  print(f"Processing {total_count} emails...")
  print(f"Auto-configured: {sender.max_workers} workers, batch size {batch_size}")
  if has_subject:
    print("✓ Using email_subject column from table")
  print("-" * 50)
  
  # Convert to list of tuples for processing
  if has_subject:
    unsent_emails = [(row['customer_email'], row['email_content'], row['email_subject']) 
                     for row in unsent_emails_df.collect()]
  else:
    unsent_emails = [(row['customer_email'], row['email_content']) 
                     for row in unsent_emails_df.collect()]
  
  all_failed_emails = []
  start_time = time.time()
  
  # Process in batches with incremental DB updates
  for batch_start in range(0, len(unsent_emails), batch_size):
    batch_end = min(batch_start + batch_size, len(unsent_emails))
    batch = unsent_emails[batch_start:batch_end]
    batch_num = (batch_start // batch_size) + 1
    total_batches = (len(unsent_emails) + batch_size - 1) // batch_size
    
    print(f"Batch {batch_num}/{total_batches}: Sending {len(batch)} emails...", end=" ")
    batch_start_time = time.time()
    
    # Send batch with adaptive settings
    successful, failed = sender.send_batch(batch)
    
    # Immediately update DB for successful sends (fault tolerance)
    update_sent_status(successful)
    
    batch_time = time.time() - batch_start_time
    all_failed_emails.extend(failed)
    
    status = f"✓ {len(successful)} sent"
    if failed:
      status += f", {len(failed)} failed"
    if sender.current_delay > 0:
      status += f" (throttled: {sender.current_delay:.1f}s delay)"
    print(f"{status} ({batch_time:.2f}s)")
  
  # Summary
  elapsed = time.time() - start_time
  print("-" * 50)
  print(f"✓ Complete: {sender.total_sent} sent, {sender.total_failed} failed in {elapsed:.2f}s")
  if elapsed > 0:
    print(f"  Throughput: {sender.total_sent / elapsed:.1f} emails/second")
  
  if all_failed_emails:
    print(f"\n⚠ Failed emails:")
    for email, error in all_failed_emails[:10]:
      print(f"  ✗ {email}: {error}")
    if len(all_failed_emails) > 10:
      print(f"  ... and {len(all_failed_emails) - 10} more")

# COMMAND ----------

# Show email status summary
display(spark.sql(f"""
  SELECT 
    CASE WHEN sent_at IS NULL THEN 'Pending' ELSE 'Sent' END AS status,
    COUNT(*) AS count
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}
  GROUP BY CASE WHEN sent_at IS NULL THEN 'Pending' ELSE 'Sent' END
"""))
