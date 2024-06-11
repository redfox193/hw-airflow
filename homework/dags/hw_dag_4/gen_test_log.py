import random
from datetime import datetime, timedelta

log_file_path = './logs/sample.log'

log_levels = ['INFO', 'WARNING', 'ERROR']
log_messages = [
    "User logged in",
    "File not found",
    "Database connection established",
    "Error in processing request",
    "User logged out",
    "Memory usage high",
    "New order created",
    "Order processing failed"
]

with open(log_file_path, 'w') as log_file:
    start_time = datetime.now() - timedelta(days=10)
    for _ in range(100):
        timestamp = start_time + timedelta(seconds=random.randint(0, 864000))
        log_level = random.choice(log_levels)
        log_message = random.choice(log_messages)
        log_file.write(f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')} - {log_level} - {log_message}\n")

print(f"Log file generated at: {log_file_path}")
