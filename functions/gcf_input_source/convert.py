import datetime
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

#FIXME: very ugly code
def convert_str_to_float(line : str) -> float:  
    """[summary]
    convert string to float by finding the right pattern to apply.
    Strategy:
        1) no convertion
        2) remove blank 
        3) remove blank and change , by .
        4) regex 

    Args:
        line (str): string to convert

    Returns:
        float: float converted from string
    """
    try:                        
        return float(line)
    except ValueError  as err:
        print("warning dummy float cast: " + line)

    try:                        
        return float(line.replace(" ",""))
    except ValueError  as err:
        print("warning float cast: " + line)
    
    try:                        
        return  float(line.replace(" ","").replace(',','.') )
    except ValueError  as err:
        print("warning float cast trying regex methods: " + line)

    try:                        
        output = []
        p = '-?[\d]+[.,\d]+|[\d]*[.][\d]+|[\d]+'
        repl_str = re.compile(p)
        line = line.split()
        for word in line:
            match = re.search(repl_str, word)
            if match:
                output.append(float(match.group()))

        if len(output) == 0:
            return 0.0
        return output[0]
    except ValueError  as err:
        print("ERROR float cast after using ugly code : " + line)
        return 0.0


def cleanEntityType(entity_type):
    if entity_type is None:
        return ""
    return str(entity_type).replace(' ', '_').replace('/', '_').replace('-', '_').lower()