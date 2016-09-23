import requests
from crisjfpy import chunker
from pandas import DataFrame
from query import wd_q,wp_q,chunker
from itertools import chain
import os
import json

wiki_API = 'https://en.wikipedia.org/w/api.php?action=query&format=json'
wikidata_API = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json'

def get_curid(title,as_df=False):
	"""Gets the page curid given its title
	DEPRECATED!!!
	"""
	print 'WARNING: This function is deprecated'
	it = hasattr(title,"__iter__")
	if it:
		title = [t for t in title if t != 'NA']
		results = {}
		norm = {}
		out = {}
		for chunk in chunker(title,50):
			API_URL = wiki_API + '&titles='+'|'.join(chunk)
			r = requests.get(API_URL).json()
			pages = r['query']['pages']
			if 'normalized' in r['query'].keys():
				normalized = r['query']['normalized']
			else:
				normalized = []
			for t in normalized:
				norm[normalized[t]['from']] = normalized[t]['from']
			for c in pages:
				results[pages[c]['title']] = c

		for t in title:
			if t in norm.keys():
				out[t] = (norm[t],results[norm[t]])
			else:
				out[t] = (t,results[t])
		return DataFrame([(k,out[k][0],out[k][1]) for k in out],columns=['title','normalized','en_curid']) if as_df else out
	else:
		API_URL = wiki_API + '&titles='+title
		r = requests.get(API_URL).json()
		return int(r['query']['pages'].values()[0]['pageid'])

def get_title(curid,wdid=None,as_df=False):
	"""
	Gets the title of the page given its curid.
	DEPRECATED!!!

	Parameters
	----------
	curid : int or list
		Curid of the page to query or list of curids. 
	as_df : boolean
		If True it will return a pandas DataFrame when asked for a list of pages.

	Returns
	-------
	title : string, dict, or pandas DataFrame
		If asked for a single page, it will return a string.
		If asked by a list of pages it will return a dictionary by default or a pandas DataFrame if as_df=True.
	"""
	print 'WARNING: This function is deprecated'
	if wdid is not None:
		it = hasattr(wdid, "__iter__")
		wdid = [_wd_id(wdid)] if not it else [_wd_id(w) for w in wdid]
		out = {}
		for chunk in chunker(wdid,50):
			url = wikidata_API+'&languages=en&ids='+'|'.join(chunk)
			r = requests.get(url).json()[u'entities']
			for w in chunk:
				try:
					out[w] = r[w]['sitelinks']['enwiki']['title']
				except:
					out[w] = 'NA'
		if not it:
			return out.values()[0]
		else:
			return DataFrame(out.items(),columns=['wd_id','title']) if as_df else out
	else:
		it = hasattr(curid, "__iter__")
		if it:
			out = dict(zip(curid,['NA']*len(curid)))
			for chunk in chunker(curid,50):
				API_URL = wiki_API+'&pageids='+'|'.join([str(c) for c in chunk])
				r = requests.get(API_URL).json()
				for page in r['query']['pages'].values():
					out[page['pageid']] = page['title']
			return DataFrame(out.items(),columns=['en_curid','title']) if as_df else out
		else:
			API_URL = wiki_API+'&pageids='+str(curid)
			r = requests.get(API_URL).json()
			return r['query']['pages'].values()[0]['title']



def get_extract(title='',curid=None):
	'''
	This function is deprecated.
	'''
	print 'WARNING: This function is deprecated'
	if (title == '' and curid is None):
		raise NameError("Either title or curid must be provided")
	try:
		if curid is not None:
			url = wiki_API+'&prop=extracts&exintro=&explaintext=&pageids='+str(curid)
		else:
			url = wiki_API+'&prop=extracts&exintro=&explaintext=&titles='+title
		r = requests.get(url)
		ex = r.json()[u'query'][u'pages'][str(curid)][u'extract']
	except:
		ex = ''
	return ex

def get_L(title=None,curid=None,as_df=False):
	"""
	Returns the number of language editions for the given curid or title.
	If both title and curid are provided, it will disregard the title.
	DEPRECATED!!!

	Parameters
	----------
	title : string
		Title of the page to query.
	curid : int or list
		Curid of the page to query or list of curids. 
	as_df : boolean
		If True it will return a pandas DataFrame when asked for a list of pages.

	Returns
	-------
	L : int, dict, or pandas DataFrame
		If asked for a single page, it will return an integer.
		If asked by a list of pages (only as curids) it will return a dictionary by default or a pandas DataFrame if as_df=True.
	"""
	print 'WARNING: This function is deprecated'
	if (title is None) & (curid is None):
		raise NameError("Either title or curid must be provided")
	if hasattr(title,"__iter__"):
		raise NameError('Cannot provide list of titles, only list of curids.')
	it = hasattr(curid, "__iter__")
	if not it:
		API_URL = wiki_API+'&prop=langlinks&lllimit=500&pageids='+str(curid) if (curid is not None) else wiki_API+'&prop=langlinks&lllimit=500&titles='+title
	
	if it:
		out = dict(zip(curid,[1]*len(curid)))
		for chunk in chunker(curid,50):
			API_URL = wiki_API+'&prop=langlinks&lllimit=500&pageids='+'|'.join([str(c) for c in chunk])
			r = requests.get(API_URL).json()
			while True:
				pages = r['query']['pages']
				for c in pages:
					page = pages[c]
					if 'langlinks' in page:
						out[int(c)] += len(page['langlinks'])
				if ('continue' in r.keys()):
					llcontinue = r['continue']['llcontinue']
					r = requests.get(API_URL+'&llcontinue='+llcontinue).json()
				else:
					break
		return DataFrame(out.items(),columns=['en_curid','L']) if as_df else out
	else:
		r = requests.get(API_URL).json()
		r = r['query']['pages'].values()[0]
		return len(r['langlinks'])+1 if ('langlinks' in r.keys()) else 1


def get_wdid(title='',curid=None,as_df=False):
	"""
	DEPRECATED!!!
	"""
	print 'WARNING: This module is deprecated'
	if (title == '' and curid is None):
		raise NameError("Either title or curid must be provided")
	it = hasattr(curid, "__iter__")
	if (curid !=None)&(not it):
		url = wiki_API+'&prop=pageprops&ppprop=wikibase_item&pageids='+str(curid)
	else:
		url = wiki_API+'&prop=pageprops&ppprop=wikibase_item&titles='+title

	if it:
		out = dict(zip(curid,['NA']*len(curid)))
		for chunk in chunker(curid,50):
			url = wiki_API+'&prop=pageprops&ppprop=wikibase_item&pageids='+'|'.join([str(c) for c in chunk])
			r = requests.get(url).json()
			for page in r['query']['pages'].values():
				out[page['pageid']] = page['pageprops'][u'wikibase_item']
		return DataFrame(out.items(),columns=['en_curid','wd_id']) if as_df else out
	else:
		r = requests.get(url).json()
		wd_id = r[u'query'][u'pages'].values()[0][u'pageprops'][u'wikibase_item']
		return wd_id





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
			r = requests.get(url).json()
			for page in r['entities'].values():
				out[page['id']] = page['labels']['en']['value']
		return DataFrame(out.items(),columns=['wd_id','name']) if as_df else out
	else:
		url = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&languages=en&ids='+prop
		r = requests.get(url).json()
		return r['entities'][prop]['labels']['en']['value']

def get_wd_coords(wdids,prop = 'P625',as_df=False):
	it = hasattr(wdids, '__iter__')
	wdids = [_wd_id(wdids)] if not it else [_wd_id(wdid) for wdid in wdids]
	results = {}
	for wd_ids in chunker(wdids,50):
		url = wikidata_API+'&languages=en&ids='+'|'.join(wd_ids)
		r = requests.get(url)
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
		r = requests.get(url)
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







def langlinks(articles,ret=False):
	'''
	Gets the langlinks for the provided set of articles.

	Parameters
	----------
	articles : list 
		List of wiki_tool article objects to query.
	ret : boolean (False)
		If True it will return a dictionary with curids as keys and langlinks as values.

	Returns
	-------
	langlinks : dict
		Dictionary with curids as keys and langlinks as values.
	'''
	pageids = [a.curid() for a in articles if (a._langlinks_dat is None)]
	if len(pageids) != 0:
		r = wp_q({'prop':'langlinks','lllimit':500,'pageids':pageids})
		for i,a in enumerate(articles):
			if a._langlinks_dat is None:
				if 'langlinks' in r['query']['pages'][str(a.curid())].keys():
					articles[i]._langlinks_dat = r['query']['pages'][str(a.curid())]['langlinks']
				else:
					articles[i]._langlinks_dat = []
	if ret:
		return {a.curid():a.langlinks() for a in articles}


def drop_comments(value):
	'''Drops wikimarkup comments from the provided string.'''
	while '<!--' in value:
		comment = value[value.find('<!--'):].split('-->')[0]+'-->'
		value = value.replace(comment,'')
	return value

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


def infobox(article,ret=False):
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
	it = hasattr(article,'__iter__')
	if not it:
		ibox = article.infobox()
		if ret:
			return {article.curid():ibox}
	else:
		pageids = [a.curid() for a in article if (a.raw_box is None)]
		redirect_list = []
		if len(pageids) != 0:
			r = wp_q({'prop':"revisions",'rvprop':'content','rvsection':0,'pageids':pageids})
			for i,a in enumerate(article):
				if a.raw_box is None:
					rp = r['query']['pages'][str(a.curid())]
					rb = rp['revisions'][0]['*']
					if '#redirect' in rb.lower(): 
						title = rb.split('[[')[-1].split(']]')[0].strip()
						article[i].__init__(title,Itype='title')
						redirect_list.append(i)
					else:
						article[i].raw_box = rb
		if len(redirect_list) != 0:
			pageids = [a.curid() for a in article if (a.raw_box is None)]
			if len(pageids) != 0:
				r = wp_q({'prop':"revisions",'rvprop':'content','rvsection':0,'pageids':pageids})
				for i,a in enumerate(article):
					if a.raw_box is None:
						rb = r['query']['pages'][str(a.curid())]['revisions'][0]['*']
						article[i].raw_box = rb
		if ret:
			return {a.curid():a.infobox() for a in article}


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

def wp_data(articles,ret=False):
	'''
	Gets all Wikipedia information about the provided list of articles.
	It gets title, curid and wdid, as well as the extract and the infobox.

	Parameters
	----------
	articles : list 
		List of wiki_tool article objects to query.
	ret : boolean (False)
		If True it will return a dictionary with curids as keys and the data as values.
	'''
	pageids = [a.curid() for a in articles if (a._data['wp'] is None)&(a.curid()!='NA')]
	if len(pageids) != 0:
		r = wp_q({'prop':'pageprops','ppprop':'wikibase_item','pageids':pageids})
		print r
		for i,a in enumerate(articles):
			if (a._data['wd'] is None)&(a.curid()!='NA'):
				articles[i]._data['wp'] = r['query']['pages'][str(a.curid())]
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
	result = requests.get(API_url).json()[u'query'][u'pages']
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
	r = requests.get(url).json()

	ZERO_RESULTS = False
	if r['status'] == 'OVER_QUERY_LIMIT':
		time.sleep(1)
		r = requests.get(url).json()
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
	country = ('NA','NA')
	if not ZERO_RESULTS:
		for res in r['results']:
			for rr in  res[u'address_components']:
				if ('country' in  rr['types']):
					country = (rr['long_name'],rr['short_name'])
				if country != ('NA','NA'):
					break
			if country != ('NA','NA'):
				break
	return country



def read_article(file_name):
	'''
	Reads an article from a json file created with article.dump().
	'''
	with open(file_name) as data_file:
		data_json = json.load(data_file)
	for i in ['curid','title','wdid']:
		if data_json['I'][i] is not None:
			out = article(data_json['I'][i],Itype=i)
			break
	out.I = data_json['I']
	out._data = data_json['_data']
	out._ex = data_json['_ex']
	out._langlinks_dat = data_json['_langlinks_dat']
	out._langlinks = data_json['_langlinks']
	out._infobox = data_json['_infobox']
	out.raw_box  = data_json['raw_box']
	out._image_url = data_json['_image_url']
	out._wd_claims      = data_json['_wd_claims']
	out._wd_claims_data = data_json['_wd_claims_data']
	out._creation_date = data_json['_creation_date']
	return out


def chunker(seq, size):
	return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))