import mwparserfromhell
import json,os,operator,copy
import nltk.data
import nltk
from query import wd_q,wp_q,_string,_isnum
from functions import drop_comments
from collections import defaultdict
from nltk.stem import WordNetLemmatizer
try:
    import cPickle as pickle
except:
    import pickle

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

		self._coords = None

		self._content = None

		self._creation_date = {}
		self._feats = None
		self._occ   = None

		self.no_wp = False
		self.no_wd = False

	def _missing_wd(self):
		'''
		This function is used to signal that the article does not correspond to a Wikidata page.
		'''
		self.no_wd = True
		self.I['wdid'] = ''
		self._data['wd'] = defaultdict(lambda:{})

		self._wd_claims      = defaultdict(lambda:'')
		self._wd_claims_data = defaultdict(lambda:'')

	def _missing_wp(self):
		'''
		This function is used to signal that the article does not correspond to a Wikipedia page.
		'''
		self.no_wp = True
		self.I['title'] = 'NA' if self.I['title'] is None else self.I['title']
		self.I['curid'] = 'NA' if self.I['curid'] is None else self.I['curid']
		self._data['wp'] = defaultdict(lambda:{})
		self._ex = ''
		self._langlinks_dat = '' 
		self._langlinks = ''

		self._infobox = ''
		self.raw_box  = ''
		self._image_url = ''
		self._content = ''
		self._creation_date = defaultdict(lambda:'')
		self._feats = ''
		self._occ   = ''

	def data_wp(self):
		'''
		Returns the metadata about the Wikipedia page.
		'''
		if (self._data['wp'] is None):
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
			if ('missing' in self._data['wp'].keys())|('invalid' in self._data['wp'].keys()):
				self._missing_wp()
		return self._data['wp']

	def data_wd(self):
		'''
		Returns the metadata about the Wikidata page.
		'''
		if (self._data['wd'] is None):
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
		if (self.I['wdid'] is None):
			d = self.data_wp()['pageprops']
			if 'wikibase_item' in d.keys():
				self.I['wdid']  = d['wikibase_item']
			else:
				self._missing_wd()
		return self.I['wdid']

	def curid(self):
		'''
		Returns the english curid of the article.
		Will get it if it is not provided.
		'''
		if (self.I['curid'] is None):
			d = self.data_wp()['pageid']
			if self.I['curid'] is None:
				self.I['curid'] = d
		return self.I['curid']

	def title(self):
		'''
		Returns the title of the article.
		Will get it if it is not provided.

		To speed up for a list of articles, run:
		>>> wp_data(articles)
		>>> [a.wdid() for a in articles]
		'''
		if (self.I['title'] is None):
			if 'missing' in self.data_wp().keys():
				self._missing_wp()
			if self.I['title'] is None:
				self.I['title'] = self.data_wp()['title']
		return self.I['title']

	def url(self,wiki='wp'):
		if wiki == 'wp':
			if self.no_wp:
				print "No Wikipedia page corresponding to this article"
			print 'https://en.wikipedia.org/wiki/'+self.title().replace(' ','_')
		elif wiki =='wd':
			if self.no_wd:
				print "No Wikidata page corresponding to this article"
			print 'https://www.wikidata.org/wiki/'+self.wdid()
		else:
			raise NameError('Wrong wiki')

	def infobox(self):
		"""
		Returns the infobox of the article.
		By getting the infobox, the class handles the redirect.
		If the raw_box is given, it will only parse the box.
		If raw_box is not given, it will get it.
		"""
		if self._infobox is None:
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
			for template in templates:
				name = template.name.strip().lower()
				if 'infobox' in name:
					box_ = {}
					box_type = drop_comments(_string(name).replace('infobox','')).strip()
					for param in template.params:
						key = drop_comments(_string(param.name.strip_code())).strip().lower()
						#value = _string(param.value.strip_code()).strip()
						value = _string(param.value).strip()

						box_[key] = value
					box[box_type] = box_
			if box is None:
				self._infobox = {}
			else:
				self._infobox = box
		return self._infobox

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
				r = wp_q({'prop':'langlinks','lllimit':500,'pageids':self.curid()})
				if 'langlinks' in r['query']['pages'].values()[0].keys():
					self._langlinks_dat = r['query']['pages'].values()[0]['langlinks']  
				else:
					self._langlinks_dat = []
			self._langlinks = {val['lang']:val['*'] for val in self._langlinks_dat}
			if 'en' not in self._langlinks.keys():
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
			if lang not in self._creation_date.keys():
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

	def image_url(self):
		if self._image_url is None:
			images = []
			ibox = self.infobox()
			for btype in ibox:
				box = ibox[btype]
				for tag in ['image','image_name','img','smallimage']:
					if tag in box.keys():
						images.append(box[tag])
			images = ['Image:'+image for image in images]
			r = wp_q({'titles':images,'prop':'imageinfo','iiprop':'url','iilimit':1},continue_override=True)
			norm = {}
			if 'normalized' in r['query'].keys(): #This is to keep the order
				norm = {val['from']:val['to'] for val in r['query']['normalized']}
			pages = {val['title']:val['imageinfo'][0]['url'] for val in r['query']['pages'].values()}
			results = []
			for image in images:
				if image in norm.keys():
					image = norm[image]
				results.append(pages[image])
			self._image_url = results
		return self._image_url
	
	def coords(self,wiki):
		'''
		Special method for articles corresponding to places to get the coordinates either from Wikipedia or Wikidata.
		
		Parameters
		----------
		wiki : string
			Wiki to use, either 'wd' or 'wp'.
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
					self._coords = ('NA','NA')
		return self._coords

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
		if self._content is None:
			r = wp_q({'titles':self.title(),'prop':'revisions','rvprop':'content'})
			if ('missing' in r['query']['pages'].values()[0].keys())|('invalidreason' in r['query']['pages'].values()[0].keys()):
				self._missing_wp()
			else:
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
				self.__init__(red,Itype='title')

	def occupation(self,return_all=False):
		'''
		Uses the occupation classifier Occ to predict the occupation.
		Warning: This function runs very slow because it loads a new classifier each time.
		Instead use:
		>>> C = wt.Occ()
		>>> C.classify(article)
		'''
		if self._occ is None:
			C = Occ()
			article = copy.deepcopy(self)
			self._feats = C.feats(article)
			self._occ = C.classify(article,return_all=return_all)
		return self._occ


class Occ(object):
	def __init__(self):
		'''
		Occupation classifier based onf Wikipedia and Wikidata information.
		'''
		path = os.path.split(__file__)[0]+'/data/'
		print 'Loading data from:\n'+path
		f = open(path+'trained_classifier.pkl', 'rb')
		self.classifier = pickle.load(f)
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
			probs = self.classifier.prob_classify(self.feats(article))
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
