"""
project_consumer_wilcox.py

Consume JSON messages from a Kafka topic and visualize:
- Author message counts (bar chart)
- Average time between messages per author (line chart)
"""

#####################################
# Import Modules
#####################################

import os
import json
import time
from collections import defaultdict
from datetime import datetime

from dotenv import load_dotenv
import matplotlib.pyplot as plt

from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

#####################################
# Set up data structures
#####################################

author_counts = defaultdict(int)
last_seen_timestamps = {}
average_deltas = {}

#####################################
# Set up live visuals (2 charts)
#####################################

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
plt.ion()

def update_charts():
    """Update the bar and line charts."""
    # ------------------------------
    # Chart 1: Author Message Counts
    # ------------------------------
    ax1.clear()
    authors = list(author_counts.keys())
    counts = list(author_counts.values())
    ax1.bar(authors, counts, color="skyblue")
    ax1.set_xlabel("Authors")
    ax1.set_ylabel("Message Counts")
    ax1.set_title("Author Message Counts")
    ax1.set_xticklabels(authors, rotation=45, ha="right")

    # ------------------------------
    # Chart 2: Average Time Between Messages
    # ------------------------------
    ax2.clear()
    avg_times = [average_deltas[author] for author in authors]
    ax2.plot(authors, avg_times, marker='o', color='orange', label="Avg Time (s)")
    ax2.set_xlabel("Authors")
    ax2.set_ylabel("Avg Time Between Messages (s)")
    ax2.set_title("Average Time Between Messages per Author")
    ax2.set_xticklabels(authors, rotation=45, ha="right")
    ax2.legend()

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

#####################################
# Process Message Function
#####################################

def process_message(message: str) -> None:
    try:
        logger.debug(f"Raw message: {message}")
        message_dict: dict = json.loads(message)
        logger.info(f"Processed JSON message: {message_dict}")

        if isinstance(message_dict, dict):
            author = message_dict.get("author", "unknown")
            author_counts[author] += 1

            # Track timing
            current_time = time.time()
            if author in last_seen_timestamps:
                time_diff = current_time - last_seen_timestamps[author]
                prev_avg = average_deltas.get(author, time_diff)
                new_avg = (prev_avg + time_diff) / 2
                average_deltas[author] = round(new_avg, 2)
            else:
                average_deltas[author] = 0.0  # First message

            last_seen_timestamps[author] = current_time

            update_charts()
            logger.info(f"Chart updated successfully for author: {author}")

        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Main Function
#####################################

def main() -> None:
    logger.info("START consumer.")
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

#####################################
# Run the script
#####################################

if __name__ == "__main__":
    main()
    plt.ioff()
    plt.show()