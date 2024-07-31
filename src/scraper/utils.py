from datetime import datetime, timezone
from src.scraper.logger import logging
from src.scraper.exception import CustomException
import sys
from typing import Tuple

def get_current_utc_datetime() -> datetime:
    """
    Retrieves the current date and time in UTC.    
    """
    try:
        current_utc_datetime = datetime.now(timezone.utc)
        logging.info("current date time collected successfully")
        return current_utc_datetime
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e,sys)


def extract_utc_date_and_time(utc_datetime: datetime) -> Tuple[str, str]:
    """
    This function takes a datetime object in UTC format and extracts the date
    and time as separate string values.

    Args:
        utc_datetime (datetime): A datetime object in UTC format.

    Returns:
        tuple[str, str]: A tuple containing the date (as 'YYYY-MM-DD') and 
                         the time (as 'HH:MM:SS').
    """
    try:
        utc_date = utc_datetime.strftime('%Y-%m-%d')
        utc_time = utc_datetime.strftime('%H:%M:%S')
        logging.info("UTC date and UTC time colleceted")
        return utc_date, utc_time
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e,sys)


if __name__ == "__main__":
    utc_datetime = get_current_utc_datetime()
    date, time = extract_utc_date_and_time(utc_datetime)
    print(date, time)