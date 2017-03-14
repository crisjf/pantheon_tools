import os,time,codecs
from parse_functions import drop_comments
from crisjfpy import chunker
from pandas import DataFrame
from query import wd_q,wp_q,chunker,rget
from itertools import chain
import urllib2
from urllib import urlretrieve
from bs4 import BeautifulSoup

wiki_API = 'https://en.wikipedia.org/w/api.php?action=query&format=json'
wikidata_API = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json'

def _wd_id(trigger):
    trigger = unicode(trigger).strip()
    if trigger[0]!='Q':
        trigger = 'Q'+trigger
    return trigger

def get_wd_name(prop,as_df=False):
	it = hasattr(prop,'__iter__')
	if it:
		out = dict(zip(prop,['NA']*len(prop)))
		for chunk in chunker(prop,50):
			url = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&languages=en&ids='+'|'.join(prop)
			r = rget(url).json()
			for page in r['entities'].values():
				out[page['id']] = page['labels']['en']['value']
		return DataFrame(out.items(),columns=['wd_id','name']) if as_df else out
	else:
		url = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&languages=en&ids='+prop
		r = rget(url).json()
		return r['entities'][prop]['labels']['en']['value']

def get_wd_coords(wdids,prop = 'P625',as_df=False):
	it = hasattr(wdids, '__iter__')
	wdids = [_wd_id(wdids)] if not it else [_wd_id(wdid) for wdid in wdids]
	results = {}
	for wd_ids in chunker(wdids,50):
		url = wikidata_API+'&languages=en&ids='+'|'.join(wd_ids)
		r = rget(url)
		for wdid in wd_ids:
			wdid_data = r.json()[u'entities'][wdid][u'claims']
			if prop in wdid_data.keys():
				mainsnak = wdid_data[prop][0][u'mainsnak']
				if 'datavalue' in mainsnak.keys():
					prop_wdid = (mainsnak[u'datavalue'][u'value'][u'latitude'],mainsnak[u'datavalue'][u'value']['longitude'],mainsnak[u'datavalue'][u'value'][u'precision'])
				else:
					prop_wdid = ('NA','NA','NA')
			else:
				prop_wdid = ('NA','NA','NA')
			results[wdid] = prop_wdid
	if not it:
		return results.values()[0]
	else:
		return DataFrame([(k,i[0],i[1],i[2]) for  k,i in results.items()],columns=['wd_id','lat','lon','precision']) if as_df else results



def get_wdprop(wdids,prop,as_df=False,names=False,date=False):
	'''
	Queries Wikidata for the property passed as prop for all the provided Wikidata Ids.
	
	Parameters
	----------
	wdids : list, str or int
		Wikidata Ids of pages to get the property for.
	prop : str or int
		Property code.
		Examples: 'P106' (occupation), 'P569' (birth date), 'P21' (gender)
	as_df : boolean (False)
		If True, returns the properties as a pandas DataFrame.
	names : boolean (False)
		If True, returns the name of the property vakye rather than the code of the property value.
		For example, it will return 'male' instead of 'Q6581097'
	date : boolean (False)
		Must be passed as True if the required property is a date.
		For example, P569 must be requested using date=True.

	Returns
	-------
	results : str, tuple, dictionary, or DataFrame
		Values for the requested property.
		If wdids is not a list, it will return a string when date=False and a tuple with (time,calendarmodel,precision) when date=True.
		If as_df=True and wdids is a list, then it will return a pandas DataFrame.
	'''
	it = hasattr(wdids, '__iter__')
	wdids = [_wd_id(wdids)] if not it else [_wd_id(w) for w in wdids]
	results = {}
	values = set([])
	for wd_ids in chunker(wdids,50):
		url = wikidata_API+'&languages=en&ids='+'|'.join(wd_ids)
		r = rget(url)
		for wdid in wd_ids:
			wdid_data = r.json()[u'entities'][wdid][u'claims']
			if prop in wdid_data.keys():
				if date:
					mainsnak = wdid_data[prop][0][u'mainsnak']
					if 'datavalue' in mainsnak.keys():
						prop_wdid = (mainsnak[u'datavalue'][u'value'][u'time'],mainsnak[u'datavalue'][u'value']['calendarmodel'].split('/')[-1],mainsnak[u'datavalue'][u'value'][u'precision'])
					else:
						prop_wdid = ('NA','NA','NA')
				else:
					prop_wdid = []
					for p in r.json()[u'entities'][wdid][u'claims'][prop]:
						mainsnak = p[u'mainsnak']
						if 'datavalue' in mainsnak.keys():
							prop_wdid.append(_wd_id(mainsnak[u'datavalue'][u'value'][u'id']))
					prop_wdid = '|'.join(prop_wdid) if len(prop_wdid) != 0 else 'NA'
			else:
				if date:
					prop_wdid = ('NA','NA','NA')
				else:
					prop_wdid = 'NA'
			results[wdid] = prop_wdid
			if date:
				values.add(prop_wdid[1])
			else:
				values.add(prop_wdid)
	if names:
		values.discard('NA')
		val_names = get_wd_name(list(values))
		val_names['NA'] = 'NA'
		if date:
			results = {k:(results[k][0],val_names[results[k][1]],results[k][2]) for k in results}
		else:
			results = {k:val_names[results[k]] for k in results}
	if not it:
		return results.values()[0]
	else:
		if date:
			return DataFrame([(k,i[0],i[1],i[2]) for  k,i in results.items()],columns=['wd_id','time','calendarmodel','precision']) if as_df else results
		else:
			return DataFrame(results.items(),columns=['wd_id',get_wd_name(prop)]) if as_df else results


def langlinks(articles,ret=False,use='curid'):
	'''
	Gets the langlinks for the provided set of articles.

	Parameters
	----------
	articles : list 
		List of wiki_tool article objects to query.
	ret : boolean (False)
		If True it will return a dictionary with curids as keys and langlinks as values.
	use : str (default='curid')
		What identifier to use: 'curid' or 'title'

	Returns
	-------
	langlinks : dict
		Dictionary with curids as keys and langlinks as values.
	'''
	if use=='curid':
		pages = [a.curid() for a in articles if (a._langlinks_dat is None)]
	elif use == 'title':
		pages = [a.curid() for a in articles if (a._langlinks_dat is None)]
	if len(pageids) != 0:
		if use=='curid':
			r = wp_q({'prop':'langlinks','lllimit':500,'pageids':pages})
		elif use == 'title':
			r = wp_q({'prop':'langlinks','lllimit':500,'titles':pages})
		for i,a in enumerate(articles):
			if a._langlinks_dat is None:
				if 'langlinks' in r['query']['pages'][str(a.curid())].keys():
					articles[i]._langlinks_dat = r['query']['pages'][str(a.curid())]['langlinks']
				else:
					articles[i]._langlinks_dat = []
			if ('en' not in a._langlinks.keys())&(a.title() is not None):
				a._langlinks['en'] = a.title()
	if ret:
		return {a.curid():a.langlinks() for a in articles}

def extract(articles,ret=False):
	'''
	Gets the extracts of a set of articles.
	It only queries the extracts for the articles without extracts.

	Parameters
	----------
	articles : list 
		List of wiki_tool article objects to query.
	ret : boolean (False)
		If True it will return a dictionary with curids as keys and extracts as values.

	Returns
	-------
	exs : dict
		If ret=True it returns a dictionary with curids as keys and extracts as values.
	'''
	pageids = [a.curid() for a in articles if (a._ex is None)]
	if len(pageids) != 0:
		r = wp_q({'prop':'extracts',"exintro":'',"explaintext":"",'exlimit':20,'pageids':pageids})
		for i,a in enumerate(articles):
			if a._ex is None:
				articles[i]._ex = r['query']['pages'][str(a.curid())]['extract']
	if ret:
		return {a.curid():a._ex for a in articles}


def infobox(articles,ret=False):
	'''
	Gets the infobox of the provided article or list of articles.
	It only queries the infoboxes for the articles without infobox.
	It handles redirects by resetting the article to point to the correct page.

	Parameters
	----------
	article : wiki_tool article or list of wiki_tool article
		Article or articles to get the extracts for.
	ret : boolean (False)
		If True it will return a dictionary with curids as keys and infoboxes as json objects as values.

	Returns
	-------
	iboxs : dict
		If ret=True it returns a dictionary with curids as keys and infoboxes as json objects as values.
	'''
	pageids = [a.curid() for a in articles if (a.raw_box is None)]
	redirect_list = []
	if len(pageids) != 0:
		r = wp_q({'prop':"revisions",'rvprop':'content','rvsection':0,'pageids':pageids})
		for i,a in enumerate(articles):
			if a.raw_box is None:
				rp = r['query']['pages'][str(a.curid())]
				rb = rp['revisions'][0]['*']
				if '#redirect' in rb.lower(): 
					title = rb.split('[[')[-1].split(']]')[0].strip()
					articles[i].__init__(title,Itype='title')
					redirect_list.append(i)
				else:
					articles[i].raw_box = rb
	if len(redirect_list) != 0:
		pageids = [a.curid() for a in articles if (a.raw_box is None)]
		if len(pageids) != 0:
			r = wp_q({'prop':"revisions",'rvprop':'content','rvsection':0,'pageids':pageids})
			for i,a in enumerate(articles):
				if a.raw_box is None:
					rb = r['query']['pages'][str(a.curid())]['revisions'][0]['*']
					articles[i].raw_box = rb
	if ret:
		return {a.curid():a.infobox() for a in articles}


def image_url(article,ret=False):
	'''
	Gets the list of urls for the infobox images for the given article or set of articles.
	It start by getting the infobox of the missing articles.
	It only queries the articles without the image url.

	Parameters
	----------
	article : wiki_tool article or list of wiki_tool article
		Article or articles to query.
	ret : boolean (False)
		If True it will return a dictionary with curids as keys and the urls as a list as values.

	Returns
	-------
	urls : dict
		If ret=True it returns a dictionary with curids as keys and list of urls as values.
	'''

	it = hasattr(article,'__iter__')
	if not it:
		url = article.image_url()
		if ret:
			return {article.curid():url}
	else:
		infobox(article)
		I = [i for i,a in enumerate(article) if (a._image_url is None)]
		if len(I) != 0:
			img = {}
			for i in I:
				images = []
				ibox = article[i].infobox()
				for btype in ibox:
					box = ibox[btype]
					for tag in ['image','image_name','img','smallimage']:
						if tag in box.keys():
							images.append(box[tag])
				images = ['Image:'+image for image in images]
				img[i] = images
			r = wp_q({'titles':list(chain.from_iterable(img.values())),'prop':'imageinfo','iiprop':'url','iilimit':1},continue_override=True)
			norm = {}
			if 'normalized' in r['query'].keys(): #This is to keep the order
				norm = {val['from']:val['to'] for val in r['query']['normalized']}
			pages = {val['title']:val['imageinfo'][0]['url'] for val in r['query']['pages'].values()}
			for i in I:
				images = img[i]
				results = []
				for image in images:
					if image in norm.keys():
						image = norm[image]
					results.append(pages[image])
				article[i]._image_url = results
		if ret:
			return {a.curid():a._image_url for a in article}

def wp_data(articles,ret=False,full=True):
	'''
	Gets all Wikipedia information about the provided list of articles.
	It gets title, curid and wdid, as well as the extract and the infobox.

	Parameters
	----------
	articles : list 
		List of wiki_tool article objects to query.
	ret : boolean (False)
		If True it will return a dictionary with curids as keys and the data as values.
	full : boolean (True)
		If True it gets the infobox an extract, if False it only gets the page metadata.
	'''

	titles = [a.I['title'] for f in articles if a.I['curid'] is None]
	#Get the curids for the provided titles (and normalize the title if necessary)

	pageids = [a.curid() for a in articles if (a._data['wp'] is None)&(a.curid()!='NA')]
	if len(pageids) != 0:
		r = wp_q({'prop':'pageprops','ppprop':'wikibase_item','pageids':pageids})
		for i,a in enumerate(articles):
			if (a._data['wd'] is None)&(a.curid()!='NA'):
				articles[i]._data['wp'] = r['query']['pages'][str(a.curid())]
	if full:
		infobox(articles)
		extract(articles)
	if ret:
		return {a.curid():a._data['wp'] for a in articles}

def wd_data(articles,ret=False):
	'''
	Gets the Wikidata Data for the provided articles.
	If the article does not have a wdid it will get it using the wp_data() function.

	Parameters
	----------
	article : wiki_tool article or list of wiki_tool article
		Article or articles to query.
	ret : boolean (False)
		If True it will return a dictionary with curids as keys and the data objects as values.
	'''
	if any([(a.I['wdid'] is None) for a in articles]):
		wp_data(articles)
	wdids = [a.wdid() for a in articles if (a._data['wd'] is None)&(a.wdid()!='NA')]

	if len(wdids) != 0:
		r = wd_q({'languages':'en','ids':wdids})
		for i,a in enumerate(articles):
			if (a._data['wd'] is None)&(a.curid()!='NA'):
				articles[i]._data['wd'] = r['entities'][a.wdid()]
	if ret:
		return {a.curid():a._data['wd'] for a in articles}



def get_multiple_image(curid):
	'''
	Gets the first image that appears in the site (it is often the character's image).

	NEEDS TO BE UPDATED
	'''
	API_url  = 'https://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvprop=content&format=json&pageids='+str(curid)+'&rvsection=0'
	result = rget(API_url).json()[u'query'][u'pages']
	r = result[unicode(curid)][u'revisions'][0][u'*']
	wikicode = mwparserfromhell.parse(r)
	templates = wikicode.filter_templates()
	box = {}
	for template in templates:
		name = template.name.lstrip().rstrip().lower()
		if 'image' in name:
			box_ = {}
			for param in template.params:
				key = drop_comments(param.name).strip().lower().replace(' ','_')
				value = drop_comments(param.value).strip()
				box_[key] = value
			box['image'] = box_
			break #Grab only the first one
	return box


def country(coords,path='',save=True,GAPI_KEY=None):
	'''
	Uses the Google geocode API to get the country of a given geographical point.

	Parameters
	----------
	coords : (lat,lon) tuple
		Coordinates to query.
	path : string
		Path to save the json file containing the query result.
	save : boolean (True)
		If True it will save the result of the query as 'lat,lon.json'
	GAPI_KEY : string (None)
		Name of the environment variable holding the Google API key.

	Returns
	-------
	country : (name,code) tuple
		Country name and 2-digit country code.
	'''
	try:
		key = os.environ[GAPI_KEY]
	except:
		key = None
	latlng = str(coords[0])+','+str(coords[1])
	if key is not None:
		url = 'https://maps.googleapis.com/maps/api/geocode/json?latlng='+latlng+'&project=Pantheon&key='+key
	else:
		url = 'https://maps.googleapis.com/maps/api/geocode/json?latlng='+latlng
	r = rget(url).json()

	ZERO_RESULTS = False
	if r['status'] == 'OVER_QUERY_LIMIT':
		time.sleep(1)
		r = rget(url).json()
	if r['status'] != 'OK':
		if r['status'] == 'OVER_QUERY_LIMIT':
			raise NameError('Query limit reached')
		elif r['status'] == 'ZERO_RESULTS':
			ZERO_RESULTS = True
		else:
			print r
			raise NameError('Unrecognized error')
	if save:
		f = open(path+latlng+'.json','w')
		json.dump(r,f)
		f.close()
	country = ('NULL','NULL')
	if not ZERO_RESULTS:
		for res in r['results']:
			for rr in  res[u'address_components']:
				if ('country' in  rr['types']):
					country = (rr['long_name'],rr['short_name'])
				if country != ('NULL','NULL'):
					break
			if country != ('NULL','NULL'):
				break
	return country


def chunker(seq, size):
	return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def dms2dd(lat):
    direc = lat[-1].lower()
    degs,mins,secs = (map(float,map(str,lat[:-1]))+[0.,0.])[:3]
    dd = degs+mins/60.+secs/(3600.)
    if (direc == 's')|(direc == 'w'):
        dd *=-1
    return dd


def latest_wddump():
	'''Gets the latest Wikidata RDF dump.'''
	url = 'http://tools.wmflabs.org/wikidata-exports/rdf/exports.html'
	conn = urllib2.urlopen(url)
	html = conn.read()
	soup = BeautifulSoup(html, "html.parser")
	for tag in soup.find_all('a'):
		link = tag.get('href',None)
		if link is not None:
			if link.split('/')[0] == 'exports':
				top_date = link.split('/')[1]
				break
	url = 'http://tools.wmflabs.org/wikidata-exports/rdf/exports/'+top_date+'/wikidata-statements.nt.gz'
	return url,top_date


def dumps_path():
	path = os.path.split(__file__)[0]+'/data/'
	files = os.listdir(path)
	if 'dumps.txt' in files:
		path = open(path+'dumps.txt').read().split('\n')[0]
	return path

def check_wddump():
	url,top_date = latest_wddump()
	path = dumps_path()
	files = os.listdir(path)
	filename = [f for f in files if 'wikidata-statements' in f]
	if len(filename) == 0:
		raise 'No dump found, please run:\n\t>>> download_latest()'
		return True
	else:
		filename=filename[0]
	if filename.split('-')[-1].split('.')[0] == top_date:
		print 'Wikidata dump is up to date'
		return False
	else:
		print 'Wikidata dump is outdated, please update\n:>>> download_latest()'
		return True


def _path(path):
	path_os = path[:]
	for c in [' ','(',')']:
		path_os = path_os.replace(c,'\\'+c)
	return path_os

def download_latest():
	'''
	Downloads the latest Wikidata RDF dump.
	
	If the dump is updated, it will delete all the instances files.
	'''
	url,top_date = latest_wddump()
	print "Downloading file from:",url
	filename = url.split('/')[-1]
	filename = filename.split('.')[0]+'-'+top_date+'.nt.gz'
	path = dumps_path()

	drop_instances=False
	if (filename.replace('.gz','') not in set(os.listdir(path)))&(filename not in set(os.listdir(path))):
		print "Saving file into",path+filename
		urlretrieve(url, path+filename)
	else:
		print "Download aborted, file already exists"

	if filename in set(os.listdir(path)):
		print "Unzipping file"
		path_os = _path(path)
		os.system('gunzip '+path_os+filename)
		drop_instances=True

	remove = [f for f in os.listdir(path) if ('wikidata-statements' in f)&(f != filename.replace('.gz',''))]
	if (len(remove) != 0)|drop_instances:
		print 'Cleaning up'
	for f in remove:
		os.remove(path+f)
	if drop_instances:
		remove = os.listdir(path+'instances/')
		for f in remove:
			os.remove(path+'instances/'+f)

def wd_instances(cl):
	'''
	Gets all the instances of the given class.

	Example
	-------
	To get all universities:
	>>> wd_instances('Q3918')
	To get all humans:
	>>> wd_instances('Q5')

	Returns
	-------
	instances : set
		wd_id for every instance of the given class.
	'''
	path = dumps_path()
	path_os = _path(path)
	files = os.listdir(path)
	instances = os.listdir(path+'instances/')
	if cl+'.nt' not in instances:
		filename = [f for f in files if 'wikidata-statements' in f]
		if len(filename) == 0:
			raise NameError('No dump found, please run:\n\t>>> download_latest()')
		else:
			filename=filename[0]
		print 'Parsing the dump ',filename
		os.system("grep 'P31[^\.]>.*"+cl+"' "+path_os+filename+"  > "+path_os+'instances/'+cl+".nt")

	lines = open(path+'instances/'+cl+".nt").read().split('\n')
	instances = set([line.split(' ')[0].split('/')[-1].split('>')[0].split('S')[0] for line in lines if line != ''])
	return instances

def all_wikipages(update=False):
	'''Downloads all the names of the Wikipedia articles'''
	path = dumps_path()
	files = os.listdir(path)
	if ('enwiki-allarticles.txt' not in files)|update:
		if ('enwiki-latest-abstract.xml' not in files)|update:
			print 'Downloading dump'
			urlretrieve('https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml',path+'enwiki-latest-abstract.xml')
		if ('enwiki-latest-titles.xml' not in files)|update:
			print 'Parsing titles from dump'
			os.system("grep '<title>'  "+_path(path)+"enwiki-latest-abstract.xml > "+_path(path)+"enwiki-latest-titles.xml")
		print 'Cleaning titles'
		f = codecs.open(path+'enwiki-latest-titles.xml',encoding='utf-8')
		g = open(path+'enwiki-allarticles.txt',mode='w')
		while True:
			line = f.readline()
			line = line[17:-9].strip()
			g.write((line+'\n').encode('utf-8'))
			if not line: break
		f.close()
		g.close()
		print 'Cleaning up'
		os.remove(path+'enwiki-latest-titles.xml')
	titles = set(codecs.open(path+'enwiki-allarticles.txt',encoding='utf-8').read().split('\n'))
	titles.discard('')
	return titles

def check_wpdump():
	path = dumps_path()
	dt = time.ctime(os.path.getmtime(path+'enwiki-latest-abstract.xml'))
	print 'Dump downloaded on:'
	print '\t'+dt.split(' ')[1]+' '+dt.split(' ')[3]+' '+dt.split(' ')[-1]
	print 'To update run:\n\t>>> all_wikipages(update=True)'


