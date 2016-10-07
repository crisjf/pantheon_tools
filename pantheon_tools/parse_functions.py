def drop_comments(value):
	'''Drops wikimarkup comments from the provided string.'''
	while '<!--' in value:
		comment = value[value.find('<!--'):].split('-->')[0]+'-->'
		value = value.replace(comment,'')
	return value