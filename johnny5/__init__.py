"""Simple tools to query the Wikipedia based on requests"""

#from functions import get_curid,get_title,get_extract,get_wdid,get_wdprop,get_L,wiki_API,wikidata_API,get_wd_name,get_wd_coords
from functions import extract,infobox,image_url,langlinks,wd_data,wp_data,country,chunker,download_latest,latest_wddump,wd_instances,all_wikipages,check_wpdump,dumps_path
from parse_functions import drop_comments,permute,correct_titles,get_links
from classes import article,Occ,CTY,biography,place,song,read_article
from query import wp_q,wd_q