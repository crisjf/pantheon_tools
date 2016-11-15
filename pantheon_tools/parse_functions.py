def drop_comments(value):
	'''Drops wikimarkup comments from the provided string.'''
	while '<!--' in value:
		comment = value[value.find('<!--'):].split('-->')[0]+'-->'
		value = value.replace(comment,'')
	return value

def find_nth(haystack, needle, n):
	'''Returns the index of the nth occurrence of needle in haystack.
	
	Parameters
	----------
	haystack : str
		String to search.
	needle : str
		Pattern to find.
	n : int
		Occurrence to return.
	'''
	start = haystack.find(needle)
	while start >= 0 and n > 1:
		start = haystack.find(needle, start+len(needle))
		n -= 1
	return start
