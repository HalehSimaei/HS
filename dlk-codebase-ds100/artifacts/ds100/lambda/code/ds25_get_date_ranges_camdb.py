import json
from datetime import datetime, timedelta

def lambda_handler(event, context):
    start_date = "2022-01-01"
    end_date = "2025-01-19"

    # Example of how date ranges can be generated
    date_ranges = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date:
        next_date = current_date + timedelta(days=2)
        date_ranges.append({
            "start_date": current_date.strftime("%Y-%m-%d"),
            "end_date": (next_date - timedelta(days=1)).strftime("%Y-%m-%d")
        })
        current_date = next_date

    # Return the ranges in the required format
    return {
        "date_ranges": date_ranges
    }
