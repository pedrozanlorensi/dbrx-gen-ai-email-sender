# Databricks notebook source
!pip install databricks-sdk -U

# COMMAND ----------

from pyspark.sql.functions import expr, col, concat_ws, lit, current_timestamp
from pyspark.sql.types import TimestampType

# COMMAND ----------

CATALOG_NAME = 'pedroz_catalog'
SCHEMA_NAME = 'email_conversations'
CONVERSATIONS_TABLE_NAME = 'musician_conversations'
OUTPUT_TABLE_NAME = 'generated_emails'
LLM_ENDPOINT = 'databricks-gpt-5-2'
CHECKPOINT_VOLUME = '/Volumes/pedroz_catalog/email_conversations/checkpoints/email_generation_streaming'

# COMMAND ----------

SYSTEM_PROMPT_PART_1 = """
  You are a helpful agent that is able to generate emails to customers based on their conversations with salespeople. The customer is always a music artist and the salesperson is always a music industry professional. The email should be written in a friendly tone and should be written in the first person. The email must include the customer's name and the name and URL of the products that were recommended by the salesperson during the conversation. 
  Look at the examples of emails below so that you know what we expect as a great email:
"""

FEW_SHOTS = [
  """
    Dear Pedro,

    It was a pleasure speaking with you about your upcoming album. Based on our conversation, I recommend checking out the Stradivarius Violin (https://bestplayers.com/stradivarius-violin) and the Yamaha Grand Piano (https://bestplayers.com/yamaha-grand-piano) to help you achieve the sound you're looking for. If you have any questions or need more information, feel free to reach out.

    Best,
    Mike Stradivarius
    BestPlayers Music Store
  """,
  """
    Dear Garth,

    I enjoyed discussing your tour plans! I think the Fender Jazz Bass (https://bestplayers.com/fender-jazz-bass) and the Shure SM7B Microphone (https://bestplayers.com/shure-sm7b-microphone) would be perfect for your performances. Let me know if you'd like a demo or more details.

    Best,
    Anakin Skywalker
    BestPlayers Music Store
  """,
  """
    Dear Logan,

    Thanks for chatting about your studio setup. I recommend the Ableton Live Suite (https://bestplayers.com/ableton-live-suite) and the Neumann U87 Microphone (https://bestplayers.com/neumann-u87-microphone) to elevate your recordings. Reach out anytime if you need support or have questions.

    Best,
    Miguel Dias
    BestPlayers Music Store
  """,
]

SYSTEM_PROMPT_PART_2 = """
  Now, write an email to the following customer, given that he talked to the following salesperson, and given its full conversation:
"""

SYSTEM_PROMPT = SYSTEM_PROMPT_PART_1 + '\\n'.join(FEW_SHOTS) + SYSTEM_PROMPT_PART_2

# Escape single quotes for SQL
SYSTEM_PROMPT_SQL = SYSTEM_PROMPT.replace("'", "''")

# COMMAND ----------

# Read from conversations table as a stream
conversations_stream = spark.readStream \
  .format("delta") \
  .table(f"{CATALOG_NAME}.{SCHEMA_NAME}.{CONVERSATIONS_TABLE_NAME}")

# COMMAND ----------

# Apply AI_QUERY transformation to generate emails
emails_stream = conversations_stream.withColumn(
  "email_content",
  expr(f"""
    AI_QUERY(
      '{LLM_ENDPOINT}',
      CONCAT(
        '{SYSTEM_PROMPT_SQL}', '\\n',
        'Customer Name: ', customer_name, '\\n',
        'Salesperson Name: ', salesperson_name, '\\n',
        'Conversation:\\n', conversation
      )
    )
  """)
).withColumn(
  "sent_at", lit(None).cast(TimestampType())  # NULL until email is sent
).withColumn(
  "prompt", expr(f"""
    CONCAT(
      '{SYSTEM_PROMPT_SQL}', '\\n',
      'Customer Name: ', customer_name, '\\n',
      'Salesperson Name: ', salesperson_name, '\\n',
      'Conversation:\\n', conversation
    )
  """
  )
).select(
  "conversation_id",
  "customer_email",
  "prompt",
  "email_content",
  "sent_at"
)

# COMMAND ----------

# Write the stream to the output table
print(f"Starting streaming job...")
print(f"  Input:  {CATALOG_NAME}.{SCHEMA_NAME}.{CONVERSATIONS_TABLE_NAME}")
print(f"  Output: {CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}")
print(f"  Checkpoint: {CHECKPOINT_VOLUME}")

query = emails_stream.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", CHECKPOINT_VOLUME) \
  .trigger(availableNow=True) \
  .toTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}")

# Wait for completion
query.awaitTermination()

print("✓ Processing complete!")

# COMMAND ----------

# Show recent results
display(spark.sql(f"""
  SELECT conversation_id, customer_email, prompt, email_content, sent_at
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}
  ORDER BY conversation_id DESC
  LIMIT 5
"""))

# COMMAND ----------

# Show pending emails count
pending_count = spark.sql(f"""
  SELECT COUNT(*) as count
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}
  WHERE sent_at IS NULL
""").first()['count']

print(f"📧 {pending_count} emails pending to be sent")
print(f"   Run the 3_MLFlowEvaluate notebook to evaluate them.")
print(f"   Run the 4_SendEmails notebook to send them.")
