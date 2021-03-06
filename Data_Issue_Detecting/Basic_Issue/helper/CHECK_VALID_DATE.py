import datetime
import re

def ifIsNotValidDateString(str):
    if not re.match(r"^(0?[1-9]|1[012])/(0?[1-9]|[12][0-9]|3[01])/\d{4}$", str):
        return True;
    array = str.split("/")
    ifCorrectDate = True
    try:
        newDate = datetime.datetime(int(array[2]),int(array[0]),int(array[1]))
    except ValueError:
        ifCorrectDate = False
    finally:
        return not ifCorrectDate

def ifIsNotValidTimeString(str):
    if re.match(r"^(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$", str):
        return False
    else :
        return True