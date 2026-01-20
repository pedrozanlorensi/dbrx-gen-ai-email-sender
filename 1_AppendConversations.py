# Databricks notebook source
!pip install faker
dbutils.library.restartPython()

# COMMAND ----------

import json
import random
from faker import Faker
import pandas as pd
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

fake = Faker()

SALESPERSONS = [
    "Alex Rivera", "Jordan Lee", "Sara Kim", "Taylor Brooks",
    "Morgan Chen", "Jamie Patel", "Riley Stone", "Cameron Davis",
    "Pedro Zanlorensi", "Garth Grawburg", "Logan Boyd", "Tayo Olabumuyi"
]

LOCATIONS = [
    "New York, NY", "Los Angeles, CA", "Chicago, IL",
    "Miami, FL", "Seattle, WA", "Austin, TX", "Curitiba, PR", 
    "Sao Paulo, SP", "Rio de Janeiro, RJ"
]

# COMMAND ----------

CATALOG_NAME = 'pedroz_catalog'
SCHEMA_NAME = 'email_conversations'
OUTPUT_TABLE_NAME = 'musician_conversations'
N_ROWS_TO_APPEND = 1

# COMMAND ----------

def generate_conversation():
    customer_first = fake.first_name()
    # basic persona
    tastes = random.choice([
        "warm, mellow tones for jazz",
        "bright, cutting tones for rock",
        "versatile instruments that fit many styles",
        "lighter instruments that are easy to travel with",
        "classic instruments with a vintage sound"
    ])

    products_intro = random.choice([
        "a new instrument and a case",
        "practice amps and pedals",
        "maintenance kits and cleaning tools",
        "strings and basic accessories",
        "gear that helps with recording at home"
    ])

    rec_instrument = random.choice([
        "a mid‑range hollow‑body electric guitar",
        "a compact digital piano with weighted keys",
        "a studio‑quality acoustic guitar",
        "an entry‑level drum kit with mesh heads",
        "a versatile 5‑string bass guitar"
    ])
    rec_accessory_1 = random.choice([
        "a padded gig bag",
        "a hard shell case",
        "a multi‑guitar stand",
        "a sturdy keyboard stand",
        "a double‑braced drum throne"
    ])
    rec_accessory_2 = random.choice([
        "a clip‑on tuner",
        "a set of coated strings",
        "a low‑noise practice amp",
        "a pair of closed‑back headphones",
        "a basic USB audio interface"
    ])

    convo = []

    # 1: customer opens
    convo.append({
        "speaker": "customer",
        "timestamp": fake.iso8601(),
        "message": (
            f"Hi, I’m looking for a new musical instrument and I like {tastes}. "
            f"I also need advice on {products_intro}."
        )
    })

    # 2: salesperson asks about experience + preferences
    convo.append({
        "speaker": "salesperson",
        "timestamp": fake.iso8601(),
        "message": (
            "Hi! I’d love to help. What do you play now, "
            "and are you looking for something beginner‑friendly or more advanced?"
        )
    })

    # 3: customer describes experience and practical needs
    convo.append({
        "speaker": "customer",
        "timestamp": fake.iso8601(),
        "message": (
            "I currently play a basic beginner guitar and I practice at home, "
            "so I need something that sounds better but still fits a small space "
            "and isn’t too hard to maintain."
        )
    })

    # 4: salesperson recommends general direction
    convo.append({
        "speaker": "salesperson",
        "timestamp": fake.iso8601(),
        "message": (
            "In that case I recommend an instrument with good build quality that matches "
            f"your taste for {tastes}. It will feel more responsive and still be comfortable to use."
        )
    })

    # 5: customer asks specifically about products
    convo.append({
        "speaker": "customer",
        "timestamp": fake.iso8601(),
        "message": (
            "That sounds great. Which specific instrument and accessories should I buy "
            "to get a good sound and keep everything in good shape?"
        )
    })

    # 6: salesperson gives product recommendations
    convo.append({
        "speaker": "salesperson",
        "timestamp": fake.iso8601(),
        "message": (
            f"I recommend {rec_instrument} with {rec_accessory_1} for protection, plus "
            f"{rec_accessory_2} so you can practice comfortably and stay in tune."
        )
    })

    # 7: customer asks about routine / usage
    convo.append({
        "speaker": "customer",
        "timestamp": fake.iso8601(),
        "message": (
            "How often should I replace strings or other parts, "
            "and how much should I practice each week?"
        )
    })

    # 8: salesperson closes with routine
    convo.append({
        "speaker": "salesperson",
        "timestamp": fake.iso8601(),
        "message": (
            "Change strings every few months if you play regularly, "
            "wipe the instrument down after each session, and aim for at least "
            "20–30 minutes of focused practice a day to see steady progress."
        )
    })

    return convo

# COMMAND ----------

def generate_dataset(n_rows=100):
    rows = []
    for _ in range(n_rows):
        salesperson_name = random.choice(SALESPERSONS)
        customer_name = fake.name()
        location = random.choice(LOCATIONS)
        # make customer email from name for realism
        first, last = customer_name.split(" ")[0], customer_name.split(" ")[-1]
        customer_email = f"{first.lower()}.{last.lower()}@{fake.free_email_domain()}"

        conversation = generate_conversation()

        rows.append({
            "salesperson_name": salesperson_name,
            "customer_name": customer_name,
            "location": location,
            "customer_email": customer_email,
            # store as JSON string so it fits nicely in CSV / tables
            "conversation": json.dumps(conversation, ensure_ascii=False)
        })
    return rows

# COMMAND ----------

data = generate_dataset(N_ROWS_TO_APPEND)

# to pandas DataFrame
df = pd.DataFrame(data)

df.head()

# COMMAND ----------

row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}")
display(row_count)

# COMMAND ----------

spark_df = spark.createDataFrame(df)

# COMMAND ----------

# Get latest ID from existing table
max_id_df = spark.sql(f"SELECT MAX(conversation_id) AS max_id FROM {CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}")
max_id = max_id_df.collect()[0]['max_id'] or 0

# Add unique ID continuing from the last one available
spark_df = spark_df.withColumn("conversation_id", monotonically_increasing_id() + max_id + 1)

# COMMAND ----------

spark_df.write.mode("append").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}")

# COMMAND ----------

row_count = spark.sql(f"SELECT COUNT(*) AS row_count FROM {CATALOG_NAME}.{SCHEMA_NAME}.{OUTPUT_TABLE_NAME}")
display(row_count)