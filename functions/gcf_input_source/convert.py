import re

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
