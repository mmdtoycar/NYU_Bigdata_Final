
def ifNotValidThreeDigitString(str):
    if str and len(str) != 3:
        return True;
    return not str.isdigit()