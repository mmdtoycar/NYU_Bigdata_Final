
def ifInvalidMappingRecord(key, value, map):
	if (key in map.keys()):
		list = map.get(key)
		return value in list
	else:
		return False

def ifInvalidIndexRecord(index, list):
	return index in list