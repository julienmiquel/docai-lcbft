import re

import dateparser  # $ pip install dateparser
from dateutil import parser
import dateutil


def convert_date(date_string) -> str :
    try:
        return dateparser.parse(date_string).date().strftime('%Y-%m-%d')
    except:
        print(f"ERROR convert_date using dateparser: {date_string}")
    
    try:
        date_string = date_string.replace(' ', '')
        return parser.parse(date_string).date().strftime('%Y-%m-%d')
        
    except dateutil.parser._parser.ParserError as err:
        print(f"ERROR convert_date using dateutil: {date_string}")

    return None


def cleanEntityType(entity_type):
    if entity_type is None:
        return ""
    return str(entity_type).replace(' ', '_').replace('/', '_').replace('-', '_').lower()