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



import mwparserfromhell
import re
from crisjfpy.pyparse import parse_ints,first_month,drop_nowrap
def parse_date(t):
    t = drop_nowrap(t)
    t = re.sub(r'\{\{[^\|]*[Cc]irca[^\}]*\}\}','',t).strip()
    template = mwparserfromhell.parse(t).filter_templates()
    yy,mm,dd = 'NA','NA','NA'
    if len(template)!=0:
        template = template[0]
        tag = re.sub(r'_+','_',template.name.lower().strip().replace(' ','_'))
        if tag in set(['birth_date','death_date_and_age','death_date','dda','age_in_years_and_days','death_date_and_given_age']):
            ii = -1
            while yy == 'NA':
                ii+=1
                try:
                    yy = int(template.params[ii].value.strip())
                except:
                    if ii>len(template.params):
                        break
            while mm == 'NA':
                ii+=1
                try:
                    mm = int(template.params[ii].value.strip())
                except:
                    if ii>len(template.params):
                        break
            while dd == 'NA':
                ii+=1
                try:
                    dd = int(template.params[ii].value.strip())
                except:
                    if ii>len(template.params):
                        break

        elif tag in set(['death_year_and_age']):
            yy = template.params[0]
            if len(template.params)>2:
                mm = template.params[2]            
        elif tag in set(['oldstyledate']):
            yy = int(template.params[1].value.strip())
            dd = parse_ints(template.params[0].value.strip())[0]
            mm = first_month(template.params[0].value.strip(),as_num=True)
        elif tag in set(['birth-date','death-date_and_age','death-date']):
            ii = 0
            while True:
                try:
                    val = template.params[ii].value.strip()
                    nums = parse_ints(val)
                    if len(nums) !=0:
                        break
                    else:
                        ii+=1
                except:
                    ii+=1
                if ii>len(template.params):
                    break                
            if len(nums) !=0:
                dd = nums[0]
                yy = nums[1]
                mm = first_month(val,as_num=True)
        elif tag in set(['birthdeathage']):
            ii = len(template.params)
            while dd == 'NA':
                ii-=1
                try:
                    dd = int(template.params[ii].value.strip())
                except:
                    if ii<0:
                        break
            while mm == 'NA':
                ii-=1
                try:
                    mm = int(template.params[ii].value.strip())
                except:
                    if ii>0:
                        break
            while yy == 'NA':
                ii-=1
                try:
                    yy = int(template.params[ii].value.strip())
                except:
                    if ii>0:
                        break
        elif tag in set(['cite_news','cite_web','refn']):
            pass
        else:
            raise NameError('Unrecognized tag '+tag)
    return str(yy),str(mm),str(dd)