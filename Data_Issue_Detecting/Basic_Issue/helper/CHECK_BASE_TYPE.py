import re

def checkBaseType(x):
    if x.isdigit():
        return "INT"
    elif unicode(x).isdecimal():
        return "DECIMAL"
    elif re.match(r"^(0?[1-9]|1[012])/(0?[1-9]|[12][0-9]|3[01])/\d{4}$", x) \
        or re.match(r"^(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$", x):
        return "DATETIME"
    else:
        return "TEXT"
def isIntOrNot(x):
	try:
		int(x)
		return True
	except:
		return False
def isFloatOrNot(x):
    try:
        float(x)
        return True
    except:
        return False