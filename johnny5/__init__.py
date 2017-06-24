"""Simple tools to query the Wikipedia based on requests"""
try:
	import future
except:
	import platform
	v = platform.python_version()
	if v.split('.')=='2':
		print('Warning: You might need future module.')

from .functions import extract,infobox,image_url,langlinks,wd_data,wp_data,country,chunker,download_latest,latest_wddump,wd_instances,all_wikipages,check_wpdump,dumps_path,check_wddump,wd_subclasses,dumps_path
from .parse_functions import drop_comments,correct_titles,get_links,first_month,parse_ints
from .classes import article,Occ,CTY,biography,place,song,band
from .query import wp_q,wd_q