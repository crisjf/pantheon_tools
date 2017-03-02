import json,os,operator,copy,mwparserfromhell,datetime,codecs
import nltk.data,nltk
import re
from nltk.stem import WordNetLemmatizer
from pandas import DataFrame,read_csv
from functions import country,dms2dd
from geopy.distance import vincenty
try:
    import cPickle as pickle
except:
    import pickle
import multiprocessing
from joblib import Parallel, delayed

from query import wd_q,wp_q,_string,_isnum,rget
from parse_functions import drop_comments,find_nth,parse_date,get_links,correct_titles
from collections import defaultdict


class article(object):
	def __init__(self,I,Itype=None):
		"""
		This is the parent class for all the queries. 
		Note that querying articles separately takes a long time.
		"""
		Itype = id_type(I) if Itype is None else Itype
		if Itype not in ['title','curid','wdid']:
			raise NameError("Unrecognized Itype, please choose between title, curid, or wdid")
		self.I = {'title':None,'curid':None,'wdid':None}
		self.I[Itype] = I
		self._data = {'wp':None,'wd':None}

		self._ex = None

		self._langlinks_dat = None 
		self._langlinks = None

		self._infobox = None
		self.raw_box  = None

		self._image_url = None

		self._wd_claims      = {}
		self._wd_claims_data = {}

		self._content = None

		self._creation_date = {}
		self._feats = None
		self._occ   = None

		self.no_wp = False
		self.no_wd = False

		self._revisions = None
		self._views = {}
		self._daily_views = {}
		self._previous_titles = None

		self._isa_values = None

	def __repr__(self):
		out = ''
		out+= 'curid : '+str(self.I['curid'])+'\n' if self.I['curid'] is not None else 'curid : \n'
		out+= 'title : '+self.I['title']+'\n' if self.I['title'] is not None else 'title : \n'
		out+= 'wdid  : '+self.I['wdid'] if self.I['wdid'] is not None else 'wdid  : '
		return out.encode('utf-8')

	def __str__(self):
		self.redirect()
		out = ''
		self.title(),self.curid(),self.wdid()
		if not self.no_wp:
			out+= 'curid : '+str(self.curid())+'\n'
			out+= 'title : '+self.title()+'\n'
		else:
			out+= 'curid : None\n'
			out+= 'title : None\n'
		if not self.no_wd:
			out+= 'wdid  : '+self.wdid()+'\n'
		else:
			out+= 'wdid  : None\n'
		out+= 'L     : '+str(self.L()) 
		return out.encode('utf-8')

	def _missing_wd(self):
		'''
		This function is used to signal that the article does not correspond to a Wikidata page.
		'''
		self.no_wd = True
		self.I['wdid'] = None
		self._data['wd'] = None

	def _missing_wp(self):
		'''
		This function is used to signal that the article does not correspond to a Wikipedia page.
		'''
		self.no_wp = True
		self.I['title'] = None
		self.I['curid'] = None
		self._data['wp'] = None


	def data_wp(self):
		'''
		Returns the metadata about the Wikipedia page.
		'''
		if (self._data['wp'] is None)&(not self.no_wp):
			if (self.I['curid'] is not None):
				self._data['wp'] = wp_q({'prop':'pageprops','ppprop':'wikibase_item','pageids':self.I['curid']})['query']['pages'].values()[0]
			elif (self.I['title'] is not None):
				self._data['wp'] = wp_q({'prop':'pageprops','ppprop':'wikibase_item','titles':self.I['title']})['query']['pages'].values()[0]
			elif (self.I['wdid'] is not None):
				if self._data['wd'] is None:
					r = wd_q({'languages':'en','ids':self.I['wdid']})
					if 'error' in r.keys():
						print r['error']['info']
						self._missing_wd()
					else:
						self._data['wd'] = r['entities'].values()[0]
				sitelinks = self._data['wd']['sitelinks']
				if 'enwiki' in sitelinks.keys():
					self.I['title'] = sitelinks['enwiki']["title"]
				elif not self.no_wd:
					self._missing_wp()
				if (self._data['wp'] is None)&(self.I['title'] is not None):
					self._data['wp'] = wp_q({'prop':'pageprops','ppprop':'wikibase_item','titles':self.I['title']})['query']['pages'].values()[0]
			else:
				raise NameError('No identifier found.')
			if not self.no_wp:
				if ('missing' in self._data['wp'].keys())|('invalid' in self._data['wp'].keys()):
					self._missing_wp()
		return self._data['wp']

	def data_wd(self):
		'''
		Returns the metadata about the Wikidata page.
		'''
		if (self._data['wd'] is None)&(not self.no_wd):
			if (self.I['wdid'] is None):
				d = self.data_wp()
				d = self._data['wp']
				if 'wikibase_item' in d['pageprops'].keys():
					self.I['wdid'] = d['pageprops'][u'wikibase_item']
				else:
					self._missing_wd()
			if self._data['wd'] is None:
				self._data['wd'] = wd_q({'languages':'en','ids':self.I['wdid']})['entities'].values()[0]
		return self._data['wd']

	def wdid(self):
		'''
		Returns the wdid of the article.
		Will get it if it is not provided.

		To speed up for a list of articles, run:
		>>> wp_data(articles)
		>>> [a.wdid() for a in articles]
		'''
		if (self.I['wdid'] is None)&(not self.no_wd):
			d = self.data_wp()
			if 'pageprops' in d:
				d = self.data_wp()['pageprops']
				if 'wikibase_item' in d.keys():
					self.I['wdid']  = d['wikibase_item']
				else:
					self._missing_wd()
			else:
				self._missing_wd()
		return self.I['wdid']

	def curid(self):
		'''
		Returns the english curid of the article.
		Will get it if it is not provided.
		'''
		if (self.I['curid'] is None)&(not self.no_wp):
			if self.data_wp() is not None:
				self.I['curid'] = self.data_wp()['pageid']
		return self.I['curid']

	def title(self):
		'''
		Returns the title of the article.
		Will get it if it is not provided.

		To speed up for a list of articles, run:
		>>> wp_data(articles)
		>>> [a.wdid() for a in articles]
		'''
		
		if (self.I['title'] is None)&(not self.no_wp):
			if self.data_wp() is not None:
				self.I['title'] = self.data_wp()['title']
		return self.I['title']

	def url(self,wiki='wp',lang='en'):
		if wiki == 'wp':
			if self.no_wp:
				print "No Wikipedia page corresponding to this article"
			if lang == 'en':
				print 'https://en.wikipedia.org/wiki/'+self.title().replace(' ','_')
			else:
				if lang in self.langlinks().keys():
					print 'https://'+lang+'.wikipedia.org/wiki/'+self.langlinks(lang).replace(' ','_')
				else:
					print 'No article in this edition'
		elif wiki =='wd':
			if self.no_wd:
				print "No Wikidata page corresponding to this article"
			print 'https://www.wikidata.org/wiki/'+self.wdid()
		else:
			raise NameError('Wrong wiki')

	def wiki_links(self):
		'''Gets all the wiki links connected from the article'''
		links = mwparserfromhell.parse(self.content()).filter_wikilinks()
		titles = set([link.encode('utf-8').split('|')[0].replace('[[','').replace(']]','').strip() for link in links])
		return titles

	def infobox(self,lang='en',force=False):
		"""
		Returns the infobox of the article.
		By getting the infobox, the class handles the redirect.
		If the raw_box is given, it will only parse the box.
		If raw_box is not given, it will get it.

		Parameters
		----------
		lang : str ('en')
			Language edition to get the infobox from.
		force : boolean (False)
			If True it will 'force' the search for the infobox by getting the template that is the most similar to an Infobox.
			Only used for non english editions.
		"""
		if (self._infobox is None)&(lang == 'en')&(not self.no_wp):
			if self.raw_box is None:
				rbox = '#redirect'
				while '#redirect' in rbox.lower():
					r = wp_q({'prop':"revisions",'rvprop':'content','rvsection':0,'pageids':self.curid()})
					try:
						rbox = r['query']['pages'].values()[0]['revisions'][0]['*']
					except:
						rbox = ''
					if '#redirect' in rbox.lower():
						title = rbox.split('[[')[-1].split(']]')[0].strip()
						self.__init__(title,Itype='title')
				self.raw_box = rbox
			wikicode = mwparserfromhell.parse(self.raw_box)
			templates = wikicode.filter_templates()
			box = {}
			box_pos = 0
			for template in templates:
				name = template.name.strip().lower()
				if 'infobox' in name:
					box_ = {}
					box_type = drop_comments(_string(name).replace('infobox','')).strip()
					for param in template.params:
						key = drop_comments(_string(param.name.strip_code())).strip().lower()
						value = _string(param.value).strip()

						box_[key] = value
					box_['box_pos'] = box_pos
					box_pos+=1
					box[box_type] = box_					
			if box is None:
				self._infobox = {}
			else:
				self._infobox = box
		return self._infobox if lang == 'en' else self._infobox_nonen(lang,force=force)

	def _infobox_nonen(self,lang,force=False):
		'''Gets the infobox for non english wikipedias.'''
		ibox_codebook = defaultdict(lambda:'infobox', {'es':'ficha de','ca':'infotaula de','sv':'infobox',
														'nl':'infobox','fr':'infobox','pl':'infobox','pt':'info',
														'ru':u'\u0433\u043e\u0441\u0443\u0434\u0430\u0440\u0441\u0442\u0432\u0435\u043d\u043d\u044b\u0439'
														})
		self.redirect()
		if lang not in self.langlinks().keys():
			return {}
		r = wp_q({'prop':"revisions",'rvprop':'content','rvsection':0,'titles':self.langlinks(lang)},lang=lang)
		try:
			rbox = r['query']['pages'].values()[0]['revisions'][0]['*']
		except:
			rbox = ''
		ibox_name = ibox_codebook[lang]
		wikicode = mwparserfromhell.parse(rbox)
		templates = wikicode.filter_templates()
		box = {}
		box_pos = 0
		lengths = []
		for template in templates:
			name = template.name.strip().lower()
			lengths.append(len(template.params))
			if ibox_name in name:
				box_ = {}
				box_type = drop_comments(_string(name).replace(ibox_name,'')).strip()
				for param in template.params:
					key = drop_comments(_string(param.name.strip_code())).strip().lower()
					value = _string(param.value).strip()
					box_[key] = value
				box_['box_pos'] = box_pos
				box_pos+=1
				box[box_type] = box_
				
		if (box == {}) & force:
			lengths.append(7) #Infobox should have at least 7 fields
			length = max(lengths)
			for template in templates:
				name = template.name.strip().lower()
				if len(template.params) >= length:
					box_ = {}
					box_type = drop_comments(_string(name)).strip()
					for param in template.params:
						key = drop_comments(_string(param.name.strip_code())).strip().lower()
						value = _string(param.value).strip()
						box_[key] = value
					box_['box_pos'] = box_pos
					box_pos+=1
					box[box_type] = box_
					
		return box

	def extract(self):
		'''
		Returns the pages extract.
		It will get it if it is not provided.
		To speed up the process for multiple articles use:
		>>> extract(articles)
		>>> [a.extract() for a in article]
		'''
		if self._ex is None:
			r = wp_q({'prop':'extracts','exintro':'','explaintext':'','pageids':self.curid()})
			self._ex = r['query']['pages'].values()[0]['extract']
		return self._ex

	def langlinks(self,lang=None):
		"""
		Returns the langlinks of the article.
		Will get them if not provided

		To speed up run:
		>>> langlinks(articles)
		>>> [a.langlinks() for a in articles]

		Parameters
		----------
		lang : str (optional)
			Language to get the link for.

		Returns
		-------
		out : str or dict
			If a language is provided, it will return the name of the page in that language.
			If no language is provided, it will resturn a dictionary with the languages as keys and the titles as values.
		"""
		if self._langlinks is None:
			if self._langlinks_dat is None:
				for Itype in ['curid','title','wdid']:
					if self.I[Itype] is not None:
						if Itype == 'wdid':
							if 'sitelinks' in self.data_wd().keys():
								sitelinks = self.data_wd()[u'sitelinks']
								self._langlinks_dat = []
								for lan in sitelinks:
									lan = lan.strip().lower()
									if (lan[-4:]=='wiki')&(lan!='commonswiki'):
										val = {'lang':lan[:-4],'*':sitelinks[lan]['title']}
										self._langlinks_dat.append(val)
							else:
								self._langlinks_dat = []
						else:
							if Itype == 'curid':
								r = wp_q({'prop':'langlinks','lllimit':500,'pageids':self.I[Itype]})
							elif Itype == 'title':
								r = wp_q({'prop':'langlinks','lllimit':500,'titles':self.I[Itype]})
							if 'langlinks' in r['query']['pages'].values()[0].keys():
								self._langlinks_dat = r['query']['pages'].values()[0]['langlinks']  
							else:
								self._langlinks_dat = []
						break
			self._langlinks = {val['lang']:val['*'] for val in self._langlinks_dat}
			if ('en' not in self._langlinks.keys())&(self.title() is not None):
				self._langlinks['en'] = self.title()
		return self._langlinks if lang is None else self._langlinks[lang]

	def creation_date(self,lang=None):
		'''
		Gets the creation date of the different Wikipedia language editions.
		The Wikipedia API requires this data to be requestes one page at a time, so there is no boost in collecting pages into a list.

		Parameters
		----------
		lang : str (optional)
			Language to get the creation date for.

		Returns
		-------
		timestamp : str or dict
			Timestamp in the format '2002-07-26T04:32:17Z'.
			If lang is not provided it will return a dictionary with languages as keys and timestamps as values.
		'''
		if lang is None:
			for lang in self.langlinks():
				if lang not in self._creation_date.keys():
					title = self.langlinks(lang=lang)
					r = wp_q({'prop':'revisions','titles':title,'rvlimit':1,'rvdir':'newer'},lang=lang,continue_override=True)
					try:
						timestamp = r['query']['pages'].values()[0]['revisions'][0]['timestamp']
					except:
						timestamp = 'NA'
					self._creation_date[lang] = timestamp
			return self._creation_date
		else:
			if lang not in self.langlinks().keys():
				raise NameError('No edition for language: '+lang)
			if (lang not in self._creation_date.keys()):
				title = self.langlinks(lang=lang)
				r = wp_q({'prop':'revisions','titles':title,'rvlimit':1,'rvdir':'newer'},lang=lang,continue_override=True)
				try:
					timestamp = r['query']['pages'].values()[0]['revisions'][0]['timestamp']
				except:
					timestamp = 'NA'
				self._creation_date[lang] = timestamp
			return self._creation_date[lang]

	def L(self):
		'''
		Returns the number of language editions of the article.
		Will get it if not provided.

		To speed up run:
		>>> langlinks(articles)
		>>> [a.L() for a in articles]
		'''
		return len(self.langlinks())

	def previous_titles(self):
		'''
		Gets all the previous titles the page had
		'''
		if self._previous_titles is None:
			r = wp_q({'prop':'revisions','pageids':self.curid(),'rvprop':["timestamp",'user','comment'],'rvlimit':'500'})
			titles = set([])
			for rev in r['query']['pages'].values()[0]['revisions']:
				if 'comment' in rev.keys():
					if 'moved page' in rev['comment']:
						comment = rev['comment']
						titles.add(comment[comment.find('[[')+2:].split(']]')[0])
			self._previous_titles = titles
		return self._previous_titles

	def image_url(self):
		'''
		Gets the url for the image that appears in the infobox.
		It iterates over a list of languages, ordered according to their wikipedia size, until it finds one.
		'''
		self.data_wp()
		if self._image_url is None:
			if not self.no_wp:
				for lang in ['en','sv','ceb','de','nl','fr','ru','it','es','war','pl','vi','ja','pt','zh','uk','ca','fa','no','ar','sh','fi']:
					results = self._image_url_lang(lang)
					if len(results) != 0:
						self._image_url = results[0]
						break
			elif not self.no_wd:
				try:
					images = 'File:'+self.wd_prop('P18')[0]['value'].replace(' ','_')
					r = wp_q({'titles':images,'prop':'imageinfo','iiprop':'url','iilimit':1},continue_override=True)
					self._image_url = r['query']['pages'].values()[0]['imageinfo'][0]['url']
				except:
					pass
			if self._image_url is None:
				self._image_url = 'NULL'
		return self._image_url

	def _image_url_lang(self,lang):
		tags_codebook = defaultdict(
						lambda:['image','img','image_name'],
							{'en':['image','image_name','img','smallimage'],'es':['imagen','img'],
							'pt':['imagem','img'],'nl':['afbeelding'],'fr':['image'],'ja':[u'\u753b\u50cf'],
							'ru':[u'\u0438\u0437\u043e\u0431\u0440\u0430\u0436\u0435\u043d\u0438\u0435'],
							'uk':[u'\u0437\u043e\u0431\u0440\u0430\u0436\u0435\u043d\u043d\u044f'],
							'pl':[u'zdj\u0119cie'],'sv':['bild','img']
							})
		images = []
		tags = tags_codebook[lang]
		ibox = self.infobox(lang=lang,force=True)
		for btype in ibox:
			box = ibox[btype]
			box_pos = box['box_pos']
			if box_pos==0:
				for tag in tags:
					if tag in box.keys():
						images.append(box[tag].strip())
		for btype in ibox:
			box = ibox[btype]
			box_pos = box['box_pos']
			if box_pos==1:
				for tag in tags:
					if tag in box.keys():
						images.append(box[tag].strip())
		imgs = []
		for image in images:
			if image.strip() != '':
				if '[[' in image:
					image = image[image.find('[[')+2:].split(']]')[0].split('|')[0]
				if ((image.lower()[:5]!='file:') & (image.lower()[:6]!='image:')):
					imgs.append('Image:'+image)
				else:
					imgs.append(image)
		images = imgs
		if len(images)!=0:
			try:
				r = wp_q({'titles':images,'prop':'imageinfo','iiprop':'url','iilimit':1},continue_override=True)
				norm = {}
				if 'normalized' in r['query'].keys(): #This is to keep the order
					norm = {val['from']:val['to'] for val in r['query']['normalized']}
				pages = {}
				for val in r['query']['pages'].values():
					try:
						pages[val['title']] = val['imageinfo'][0]['url']
					except:
						pass
				results = []
				for image in images:
					if image in norm.keys():
						image = norm[image]
					results.append(pages[image])
			except:
				results = []
		else:
			results = []
		return results

	def wd_prop(self,prop):
		'''
		Gets the Wikidata propery.
		Is based on the the stored wikidata data, and if the data is not found, it will get it.
		To speed up the process for multiple articles, use:
		>>> wd_data(articles)
		>>> [a.wd_prop(prop) for a in articles]

		Parameters
		----------
		prop : list
			List of all the values provided for the property.
			Each value can be a string, number, or json object.
		'''
		if prop not in self._wd_claims.keys():
			data = self.data_wd()
			if prop in data['claims'].keys():
				vals = data['claims'][prop]
				try:
					out = []
					for val in vals:
						val = val['mainsnak']['datavalue']['value']
						if isinstance(val,dict):
							out.append(defaultdict(lambda:'NA',val))
						else:
							out.append(defaultdict(lambda:'NA',{'value':val}))
					self._wd_claims[prop] = out
				except:
					self._wd_claims[prop] = [defaultdict(lambda:'NA')]
			else:
				self._wd_claims[prop] = [defaultdict(lambda:'NA')]
		return self._wd_claims[prop]

	def dump(self,path='',file_name=None):
		'''
		Dumps the object to a file.
		'''
		out = self.__dict__
		file_name = path+str(self.curid())+'.json' if file_name is None else path +  file_name
		with open(file_name, 'w') as outfile:
			json.dump(out, outfile)

	def content(self):
		if (self._content is None)&(not self.no_wp):
			if self.title() is not None:
				r = wp_q({'titles':self.title(),'prop':'revisions','rvprop':'content'})
				if ('interwiki' in r['query'].keys()):
					self._missing_wp()
					return '#REDIRECT [['+r['query']['interwiki'][0]['title'].strip()+']]'
				elif ('missing' in r['query']['pages'].values()[0].keys())|('invalidreason' in r['query']['pages'].values()[0].keys()):
					self._missing_wp()
				else:
					if not self.no_wp:
						self._content = r['query']['pages'].values()[0]['revisions'][0]['*'] 
		return self._content


	def redirect(self):
		'''
		Handles redirects if the page has one.
		'''
		content = self.content()
		if content is not None:
			if '#redirect' in content.lower():
				red = content[content.lower().find('#redirect')+9:].strip()
				red = red.split(']]')[0].split('[[')[1].strip()
				if '|' in red:
					red = red.split('|')[-1].strip()
				if self.no_wp:
					self.I['title'] = red
				else:
					self.__init__(red,Itype='title')

	def revisions(self,user=True):
		'''
		Gets the timestamps for the edit history of the Wikipedia article.

		Parameters
		----------
		user : boolean (True)
			If True it returns the user who made the edit as well as the edit timestamp.
		'''
		if self._revisions is None:
			r = wp_q({'prop':'revisions','pageids':self.curid(),'rvprop':["timestamp",'user'],'rvlimit':'500'})
			self._revisions = [(rev['timestamp'],rev['user']) for rev in r['query']['pages'].values()[0]['revisions']]
		if user:
			return self._revisions
		else:
			return [val[0] for val in self._revisions]

	def pageviews(self,start_date,end_date=None,agg=False,lang='en',cdate_override=False,daily=False):
		'''
		Gets the pageviews between the provided dates for the given language editions.

		Parameters
		----------
		start_date : str
			Start date in format 'yyyy-mm'.
			If start_date=None is passed, it will get all the pageviews for that edition.
		end_date : str
			End date in format 'yyyy-mm'. If it is not provided it will get pagviews until today.
		lang : str ('en')
			Language edition to get the pageviews for. 
			If lang=None is passed, it will get the pageviews for all language editions.
		daily : boolean (False)
			If True it will return the daily pageviews.
		agg : boolean (False)
			If True it will sum all the pageviews.
		'''
		if lang is None:
			out = {}
			for lang in self.langlinks().keys():
				if agg:
					out[lang] = sum(self._pageviews_lang(start_date,end_date=end_date,lang=lang,cdate_override=cdate_override,daily=daily).values())
				else:
					out[lang] = self._pageviews_lang(start_date,end_date=end_date,lang=lang,cdate_override=cdate_override,daily=daily)
			return out
		else:
			if agg:
				out = self._pageviews_lang(start_date,end_date=end_date,lang=lang,cdate_override=cdate_override)
				return sum(out.values())
			else:
				out = self._pageviews_lang(start_date,end_date=end_date,lang=lang,cdate_override=cdate_override,daily=daily)
				return out

	def _pageviews_lang(self,start_date,end_date=None,lang='en',cdate_override=False,daily=False):
		if start_date is not None:
			y0,m0 = start_date.split('-')
			m0,y0 = int(m0),int(y0)
		else:
			m0,y0 = None,None

		if end_date is None:
			yf = datetime.date.today().year
			mf = datetime.date.today().month
			if mf == 1:
				yf = yf-1
				mf = 12
			else:
				mf = mf-1
		else:
			yf,mf = end_date.split('-')
			mf,yf = int(mf),int(yf)

		if lang not in self._views.keys():
			self._views[lang] = {}

		if not cdate_override:
			timestamp = self.creation_date(lang)
			yy,mm = timestamp.split('-')[:2]
			yy,mm = int(yy),int(mm)

			if (y0 is not None):
				if (((yy==y0)&(mm>m0))|(yy>y0)):
					mi = 1 if (mm == 12) else mm 
				else:
					mi = m0
				if (yy>y0):
					yi = yy+1 if (mm == 12) else yy
				else:
					yi = y0
			else:
				yi,mi = yy,mm
		else:
			yi,mi = y0,m0
		mi = 12 if yi <= 2007 else mi
		yi = 2007 if yi < 2007 else yi

		dates = []
		if yf == yi:
			dates = [(str(yi),('00'+str(mm))[-2:]) for mm in xrange(mi,mf+1)]
		else:
			dates += [(str(yi),('00'+str(mm))[-2:]) for mm in xrange(mi,13)]
			for yy in xrange(yi+1,yf):
				dates += [(str(yy),('00'+str(mm))[-2:]) for mm in xrange(1,13)]
			dates += [(str(yf),('00'+str(mm))[-2:]) for mm in xrange(1,mf+1)]

		rest_start = None
		rest_end   = None
		for y,m in dates:
			if y+'-'+m not in self._views[lang].keys():
				if ((y=='2015')&(int(m)>=7))|(y=='2016'):#Here is the cutoff to go to rest
					rest_start = (y,m) if (rest_start is None) else rest_start
					rest_end   = (y,m)
				else:
					self._pageviews_grok(y,m,lang=lang,daily=daily)
		if (rest_start is not None):
			self._pageviews_rest(rest_start,rest_end,lang=lang,daily=daily)

		if daily:
			dates = set([y+'-'+m for y,m in dates])
			days = [day for day in self._daily_views[lang].keys() if day[:find_nth(day,'-',2)] in dates]
			out = {day:self._daily_views[lang][day] for day in days}
		else:	
			_out = defaultdict(lambda:0,self._views[lang])
			out = {y+'-'+m:_out[y+'-'+m] for y,m in dates}
			#out = {y+'-'+m:self._views[lang][y+'-'+m] for y,m in dates}
		return out

	def _pageviews_rest(self,rest_start,rest_end,lang='en',daily=False):
		if not lang in self._views.keys():
			self._views[lang] = {}
		if (not lang in self._daily_views.keys())&daily:
			self._daily_views[lang] = {}

		sd = rest_start[0]+rest_start[1]+'01'
		if rest_end[1] == '12':
			fd = str(int(rest_end[0])+1)+'0101'
		else:
			fd = str(rest_end[0])+('00'+str(int(rest_end[1])+1))[-2:]+'01'

		url = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/'+lang+'.wikipedia/all-access/user/'+self.langlinks(lang)+'/daily/'+sd+'/'+fd
		r = rget(url).json()
		if ('title' not in r.keys()):
			monthly = [(val['timestamp'][:4]+'-'+val['timestamp'][4:6],val['views']) for val in r['items'][:-1]]
			monthly = [tuple(val) for val in DataFrame(monthly).groupby(0).sum()[[1]].reset_index().values]
			out = dict(monthly)
		else:
			out = defaultdict(lambda: 0)

		for date in out.keys():
			self._views[lang][date] = out[date]
		if daily:
			out = dict([(val['timestamp'][:4]+'-'+val['timestamp'][4:6]+'-'+val['timestamp'][6:8],val['views']) for val in r['items'][:-1]])
			for day in out:
				self._daily_views[lang][day] = out[day]
	
	def _pageviews_grok(self,y,m,lang='en',daily=False):
		if not lang in self._views.keys():
			self._views[lang] = {}
		if (not lang in self._daily_views.keys())&daily:
			self._daily_views[lang] = {}
		title = self.langlinks(lang)
		url = ('http://stats.grok.se/json/'+lang+'/'+y+m+'/'+title).replace(' ','_')
		r = rget(url).json()
		self._views[lang][y+'-'+m] = sum(r['daily_views'].values())
		if daily:
			for day in r['daily_views']:
				self._daily_views[lang][day] = r['daily_views'][day]

	def find_article(self):
		'''
		Find the article by trying different combinations of the title's capitalization.
		'''
		self.redirect()
		if self.curid() == 'NA':
			old_title = self.title()
			titles = correct_titles(self.title())
			for title in titles:
				self.__init__(title,Itype='title')
				self.redirect()
				if self.curid() != 'NA':
					break
			if self.curid() == 'NA':
				self.__init__(old_title,Itype='title')

	def _is_a(self,full=False):
		'''
		Gets the phrase after the verb 'to be' in the first paragraph of the extract.

		Parameters
		----------
		full : boolean (False)
			If True it will return a tuple with (phrase,sentence,verb), otherwise it returns only phrase.

		Returns
		-------
		phrase : str
			Phrase after the verb 'to be'.
		sentence : str
			Full sentence that contains the verb 'to be'.
		verb : str
			Verb to be as it appears in the sentence.
		'''
		if self._isa_values is None:
			sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')
			lmt = WordNetLemmatizer()
			ex = self.extract()
			sentences = sent_detector.tokenize(ex)
			found_tobe = False
			phrase = ''
			for sentence in sentences:
				words = nltk.pos_tag(nltk.word_tokenize(sentence))
				for word,tag in words:
					if tag[:2] == 'VB':
						if lmt.lemmatize(word, pos='v') == 'be':
							found_tobe = True
							phrase = sentence[sentence.find(word)+len(word):].strip()
							break
				if found_tobe:
					break
			if found_tobe:
				self._isa_values = (phrase,sentence,word.lower())
			else:
				self._isa_values = ('NA','NA','NA')
		if full:
			return self._isa_values
		else:
			return self._isa_values[0]

class place(article):
	def __init__(self,I,Itype=None):
		super(place, self).__init__(I,Itype=None)
		self._coords = None
		self._is_city = None
		self._wpcities = None
		self._country = None


	def coords(self,wiki='wp'):
		'''
		Get the coordinates either from Wikipedia or Wikidata.
		
		Parameters
		----------
		wiki : string
			Wiki to use, either 'wd' or 'wp'.
			Default is 'wp'
		'''
		if self._coords is None:
			if wiki=='wd':
				try:
					coords = self.wd_prop('P625')[0]
					self._coords = (coords['latitude'],coords['longitude'])
				except:
					self._coords = ('NA','NA')
			else:
				wiki ='en' if wiki == 'wp' else wiki
				try:
					if wiki !='en':
						r = wp_q({'prop':'coordinates',"titles":self.langlinks(wiki)},lang=wiki)
					else:
						r = wp_q({'prop':'coordinates',"pageids":self.curid()})
					coords = r['query']['pages'].values()[0]['coordinates'][0]
					self._coords = (coords['lat'],coords['lon'])
				except:
					wikicode = mwparserfromhell.parse(self.content())
					templates = wikicode.filter_templates()
					lat,lon = ('NA','NA')
					names = set([template.name.strip().lower() for template in templates])
					if 'coord' in names:
						for template in templates:
							name = template.name.strip().lower()
							if name == 'coord':
								lat = []
								lon = []
								values = template.params
								for i,val in enumerate(values):
									lat.append(val)
									if (val.lower() == 'n')|(val.lower() == 's'):
										break
								for val in values[i+1:]:
									lon.append(val)
									if (val.lower() == 'e')|(val.lower() == 'w'):
										break
								lat,lon= (dms2dd(lat),dms2dd(lon))
								break
					else:
						parameters = {'latd':'NA','latns':'','longd':'NA','longew':''}
						for template in templates:
							name = template.name.strip().lower()
							if 'infobox settlement' in name:
								for param in template.params:
									name = param.name.strip().lower()
									for ppp in parameters:
										if name == ppp:
											parameters[ppp] = param.value.strip().lower()
								parameters['latd'] = float(parameters['latd'])
								parameters['longd'] = float(parameters['longd'])
								if parameters['latns'] == 's':
									parameters['latd'] *= -1
								if parameters['latew'] == 'w':
									parameters['longd'] *= -1
								break
						lat,lon = (parameters['latd'],parameters['longd'])
					self._coords = (lat,lon)
		return self._coords

	def country(self,GAPI_KEY=None,name=False):
		'''
		Uses google places API to get the country of the given place.

		Parameters
		----------
		GAPI_KEY : str
			Name of the environment variable that has the API key.
		name : boolean (False)
			If True it returns the name of the country.

		Returns
		-------
		ccode : str
			Country code.
		'''
		if self._country is None:
			ctr,ccode = country(self.coords(),save=False,GAPI_KEY=GAPI_KEY)
			self._country = (ctr,ccode)
		if name:
			return self._country[0]
		else:
			return self._country[1]


class song(article):
	def __init__(self,I,Itype=None):
		super(song, self).__init__(I,Itype=None)
		self._is_song = None
		self._wpsong  = None
		self._genre   = None

	def disambiguate(self,artist=None):
		'''
		If the provided page is a disambiguation page, it returns the song that it was able to find within the links.

		Parameters
		----------
		artist : str (optional)
			If provided it will get the song associated with the given artist.
		'''
		song_titles = []
		if "(disambiguation)" not in self.title().lower():
			self.__init__(self.title()+' (disambiguation)',Itype='title')
		self.redirect()
		if ("(disambiguation)" in self.title().lower())&(self.curid()!='NA'):
			for link in get_links(self.content()):
				if song(link).is_song():
					song_titles.append(link)
			if len(song_titles) == 0:
				return []
			if artist is None:
				return song_titles
			else:
				a = article(artist)
				a.find_article()
				if a.curid() == "NA":
					for sep in ['and','&']:
						if sep in artist.lower():
							a = article(artist[:artist.lower().find(sep)].strip())
							a.find_article()
							break
				if a.curid() != 'NA':
					for t in song_titles:
						perf = song(t).performer()
						if perf == a.title():
							return [t]
				return []
		else:
			return []

	def find_article(self):
		'''
		Find the article by trying different combinations of the title.
		'''
		self.redirect()
		if self.curid() == 'NA':
			old_title = self.title()
			titles = correct_titles(self.title())
			titles += [title+' (song)' for title in titles]
			disamb = []
			for title in titles:
				self.__init__(title,Itype='title')
				self.redirect()
				disamb.append(('(disambiguation)' in self.title()))
				if (self.curid() != 'NA')&(self.is_song()):
					break
			if self.curid() == 'NA':
				self.__init__(old_title,Itype='title')
				for i,d in enumerate(disamb):
					if d:
						self.__init__(titles[i],Itype='title')
						self.redirect()

	def is_song(self):
		if self._is_song is None:
			if self.curid()=='NA':
				self._is_song=False
			elif self._wpsong_template() is None:
				if ('song' in self._is_a().lower())|('single' in self._is_a().lower()):
					self._is_song = True
				else:
					self._is_song = False
			else:
				self._is_song = True
		return self._is_song

	def _wpsong_template(self):
		'''
		Returns the template associated to the WP Songs when available.
		'''
		if self._wpsong is None:
			self._is_song = False
			r = wp_q({'prop':"revisions",'rvprop':'content','rvsection':0,'titles':'Talk:'+self.title()})
			r = r['query']['pages'].values()[0]
			if 'revisions' in r.keys():
				wikicode = mwparserfromhell.parse(r['revisions'][0]['*'])
				templates = wikicode.filter_templates()
				for t in templates:
					if ('songs' in t.name.lower().replace(' ','')):
						self._wpsong = t
						self._is_song = True
						break
		return self._wpsong

	def performer(self):
		return article(self.wd_prop('P175')[0]['id']).title()




class biography(article):
	def __init__(self,I,Itype=None):
		super(biography, self).__init__(I,Itype=None)
		self._is_bio = None
		self._wpbio = None
		self._death_date = None
		self._birth_date = None
		self._name = None
		#if not self.is_bio():
		#	print 'Warning: Not a biography ('+str(self.curid())+')'

	def name(self):
		if self._name is None:
			if self.title() is not None:
				self._name = re.sub(r'\([^\(\)]*\)','',self.title()).strip()
			else:
				data = self.data_wd()
				if 'aliases' in data.keys():
					if 'en' in data['aliases'].keys():
						self._name = data['aliases']['en'][0]['value']
					else:
						self._name = data['aliases'].values()[0][0]['value']
				else:
					self._name = 'NULL'
		return self._name

	def desc(self):
		phrase,sentence,verb = self._is_a(full=True)
		return sentence

	def is_bio(self):
		if (self._is_bio is None)&(not self.no_wp):
			if self._wpbio_template() is None:
				self._is_bio = False
			else:
				if self._is_group():
					self._is_bio = False
				else:
					self._is_bio = True
		return self._is_bio

	def _is_group(self):
		phrase,sentence,verb = self._is_a(full=True)
		if (verb=='are')|(verb=='were'):
			return True
		words = set(nltk.word_tokenize(phrase.lower()))
		buzz_words = ['band','duo','group','team']
		for w in buzz_words:
			if (w in words):
				return True
		return False

	def _wpbio_template(self):
		'''
		Returns the template associated to the WP Biography when available.
		'''
		if self._wpbio is None:
			self._is_bio = False
			r = wp_q({'prop':"revisions",'rvprop':'content','rvsection':0,'titles':'Talk:'+self.title()})
			try:
				wikicode = mwparserfromhell.parse(r['query']['pages'].values()[0]['revisions'][0]['*'])
				templates = wikicode.filter_templates()
				for t in templates:
					if ('biography' in t.name.lower().replace(' ',''))|('bio' in t.name.lower().replace(' ','')):
						self._wpbio = t
						self._is_bio = True
						break
			except:
				pass
		return self._wpbio

	def living(self):
		print 'Warning: This function will be dropped.'
		return self.alive()

	def alive(self,boolean=False):
		'''
		Retrieves the information whether the biography is about a living or dead person.
		It uses the WikiProject Biography template from the Talk page to get this information.

		Returns
		-------
		alive : str
			Returns either 'yes' or 'no'.
		'''
		t = self._wpbio_template()
		alive = 'NULL'
		if (t !='NA')&(t is not None):
			for p in t.params:
				if p.name.strip().replace(' ','').lower() == 'living':
					living = drop_comments(p.value.lower().strip())
					if (living[0] == 'n'):
						alive = 'no'
						break
					elif (living[0] == 'y'):
						alive = 'yes'
						break
					else:
						alive = p.value
						break
			if alive == 'NULL':
				phrase,sentence,verb = self._is_a(full=True)
				if (verb == 'is')|(verb == 'are'):
					alive = 'yes'
				elif (verb == 'was')|(verb == 'were'):
					alive = 'no'
		if boolean:
			mp = defaultdict(lambda: 'NULL',{'yes':True,'no':False})
			return mp[alive]
		else:
			return alive


	def death_date(self,raw=False):
		'''
		Gets the death date from the infobox. 
		If it is not available in the infobox (or it cannot parse it) it uses Wikidata.

		Parameters
		----------
		raw : boolean (False)
			If True it also returns the raw text from the infobox.

		Returns
		-------
		d : tuple
			(yyyy,mm,dd)
		t : string (if raw)
			Raw text from the infobox.

		MISSING TAG: d-da (490286)
		'''
		if self._death_date is None:
			if self.alive() =='yes':
				return 'alive'
			d = ['NA']
			t = 'NA'
			if len(self.infobox()) !=0:
				for box in self.infobox().values():
					if 'death_date' in box.keys():
						t = box['death_date']
						break
				if t != 'NA':
					d = parse_date(t)
			else:
				t = 'wd'
				d = self.wd_prop('P570')[0]['time']
				if d != 'NA':
					d = d.split('T')[0][1:].split('-')
			if d[0] == 'NA':
				t = 'wd' if t =='NA' else t
				d = self.wd_prop('P570')[0]['time']
				if d != 'NA':
					d = d.split('T')[0][1:].split('-')
			self._death_date = (d,t)
		d,t = self._death_date
		if raw:
			return (d,t)
		else:
			return d

	def occupation(self,return_all=False):
		'''
		Uses the occupation classifier Occ to predict the occupation.
		Warning: This function runs very slow because it loads a new classifier each time.
		Instead use:
		>>> C = wt.Occ()
		>>> C.classify(article)
		'''
		print 'Warning: This function runs very slow because it loads a new classifier each time.'
		if self._occ is None:
			C = Occ()
			article = copy.deepcopy(self)
			self._feats = C.feats(article)
			self._occ = C.classify(article,return_all=True)
		if return_all:
			return self._occ
		else:
			if self._occ[1] == 'trained':
				return self._occ
			else:
				prob_ratio = self._occ[0][1]/self._occ[1][1]
				return self._occ[0][0],prob_ratio


class CTY(object):
	def __init__(self,city_data='geonames'):
		'''
		Assigns a city to a set of coordinates.
		'''
		self.city_data=city_data
		path = os.path.split(__file__)[0]+'/data/'
		print 'Loading data from:\n'+path
		if city_data == 'geonames':
			header = ['geonameid','name','asciiname','alternatenames','latitude','longitude','feature class','feature code',
				'country code','cc2','admin1 code','admin2 code','admin3 code','admin4 code','population','elevation',
				'dem','timezone','modification date']
			f = codecs.open(path+'cities5000.txt',encoding='utf-8')
			self.cities = DataFrame([c.split('\t') for c in f.read().split('\n') if c !=''],columns=header)
			self.c_coords = [tuple(c) for c in self.cities[['geonameid','latitude','longitude']].drop_duplicates().values]
		elif city_data == 'chandler':
			self.cities = read_csv(path+'chandler_cities.csv',encoding='utf-8')
			self.c_coords = [tuple(c) for c in self.cities[['ch_id','Latitude','Longitude']].drop_duplicates().values]

		self._out = {}

	def city(self,coords):
		if (len(coords) == 2)&(not hasattr(coords[0], '__iter__')):
			if coords not in self._out.keys():
				self._out[coords] = _city(self,coords)
			return self._out[coords]
		else:
			coords_ = [coord for coord in coords if coord not in self._out.keys()]
			n_jobs = multiprocessing.cpu_count()
			distances = Parallel(n_jobs=n_jobs)(delayed(_city)(self,coord) for coord in coords_)
			dmap = dict(zip(coords_,distances))
			out = []
			for coord in coords:
				if coord not in self._out.keys():
					self._out[coord] = dmap[coord]
				out.append(self._out[coord])
			return out

def _city(C,coords):
	out = []
	for city in C.c_coords:
		try:
			out.append((city[0],vincenty(coords,(city[1],city[2])).kilometers))
		except:
			pass
	if len(out)==0:
		out.append(('NA','NA','NA'))
	if C.city_data == 'geonames':
		out = DataFrame(out,columns=['geonameid','dis']).sort_values(by='dis').iloc[0]
		return int(out['geonameid']),out['dis']
	elif C.city_data == 'chandler':
		out = DataFrame(out,columns=['geonameid','dis']).sort_values(by='dis').iloc[0]
		return int(out['geonameid']),out['dis']
		


class Occ(object):
	def __init__(self):
		'''
		Occupation classifier based onf Wikipedia and Wikidata information.
		'''
		path = os.path.split(__file__)[0]+'/data/'
		print 'Loading data from:\n'+path
		f = open(path+'trained_classifier.pkl', 'rb')
		self._classifier = pickle.load(f)
		f.close()

		self.lmt = WordNetLemmatizer()
		self.sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

		self.occ_vocab = set(open(path+'occ_vocab.txt').read().split('\n'))
		self.tpc_vocab = set(open(path+'tpc_vocab.txt').read().split('\n'))

		self.wdmap = defaultdict(lambda:'NA',dict([tuple(line[:-1].split('\t')) for line in open(path+"wd_occs_controlled.tsv")]))
		self.bmap  = defaultdict(lambda:'NA',dict([tuple(line[:-1].split('\t')) for line in open(path+"box_controlled.tsv")]))

		self.train = dict([tuple(line[:-1].split('\t')) for line in open(path+"train.tsv")])
		self.train_keys = set(self.train.keys())

	def classify(self,article,return_all=False):
		'''
		Classifier function

		Parameters
		----------
		article : wiki_tools.article object
			Article to classify.
		return_all : boolean (False)
			If True it will return the probabilities for all occupations.
		'''	
		if str(article.curid()) in self.train_keys:
			return self.train[str(article.curid())],'trained'
		else:
			probs = self._classifier.prob_classify(self.feats(article))
			probs = sorted([(c,probs.prob(c)) for c in probs.samples()],key=operator.itemgetter(1),reverse=True)
			prob_ratio = probs[0][1]/probs[1][1]
			article._occ = (probs[0][0],prob_ratio)
			if return_all:
				return probs
			else:
				return probs[0][0],prob_ratio

	def _normalize(self,text):
		text_ = text.lower()
		for word in self.occ_vocab:
			if word.replace('-',' ') in text_:
				text_ = text_.replace(word.replace('-',' '),word)
		for word in self.tpc_vocab:
			if word.replace('-',' ') in text_:
				text_ = text_.replace(word.replace('-',' '),word)
		return text_

	def _wd_occs(self,article):
		'''
		Returns the occupations as reported in Wikidata using the vocabulary provided in occ_vocab.txt
		'''
		wd_occs = set([self.wdmap[o['id']] for o in article.wd_prop('P106')])
		if 'NA' in wd_occs:
			wd_occs.remove('NA')
		return wd_occs

	def _isa(self,article):
		'''
		Get the first and second occupation reported in the first sentence in Wikipedia, using the controlled vocabulary provided in box_controlled.tsv
		'''
		ex = article.extract()
		sentences = self.sent_detector.tokenize(ex)
		first_occ = ''
		second_occ = ''
		for sentence in sentences:
			words = nltk.pos_tag(nltk.word_tokenize(sentence))
			for i,(word,tag) in enumerate(words):
				if tag[:2] == 'VB':
					if self.lmt.lemmatize(word, pos='v') == 'be':
						first_occ = 'NA'
						second_occ = 'NA'
						for ww in nltk.word_tokenize(self._normalize(' '.join([w for w,t in words[i+1:]]))):
							if (ww in self.occ_vocab):
								if (second_occ == 'NA')&(first_occ != 'NA'):
									second_occ = ww
								if (first_occ == 'NA'):
									first_occ = ww
						break
			if first_occ != '':
				return first_occ,second_occ
		return 'NA','NA'

	def _box_type(self,article):
		'''
		Gets the type of the first infobox of the provided Wikipedia page using the controlled vocabulary provided in box_controlled.tsv
		'''
		types = [self.bmap[val.replace('_',' ').strip().replace(' ','_')] for val in article.infobox().keys()]
		try:
			types.remove('NA')
		except:
			pass
		if len(types) >=1:
			return types[0]
		else:
			return 'NA'

	def _topics(self,article):
		'''
		Gets the topic words from the Wikipedia extract of the provided article, using the vocabulary provided in tpc_controlled.txt
		'''
		words = set([])
		ex = article.extract()
		ex = self._normalize(ex)
		ex_words = set(nltk.word_tokenize(ex))
		for word in self.tpc_vocab:
			if word in ex_words:
				words.add(word)
		return words

	def feats(self,article):
		if article._feats is None:
			feats = defaultdict(lambda:False)
			feats['btype'] = self._box_type(article)
			isa = self._isa(article)
			feats['isa1'] = isa[0]
			feats['isa2'] = isa[1]
			tpcs = self._topics(article)
			for word in tpcs:
				feats[word] = True
			wd_occs = self._wd_occs(article)
			for word in wd_occs:
				feats[word] = True
			article._feats = feats
		return article._feats

	
def id_type(I):
	if _isnum(I):
		return 'curid'
	elif I.isdigit():
		return 'curid'
	elif (I[0].lower() == 'q')&(I[1:].isdigit()):
		return 'wdid'
	else:
		return 'title'


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