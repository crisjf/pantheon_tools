Tutorial
********

.. toctree::
   :maxdepth: 2

This brief tutorial is meant to teach you how to use johnny5

Getting started
===============

To use, clone the repository and run

>>> python setup.py develop

While using johnny5 you might have to download the latest Wikidata and Wikipedia dumps. Since both of these files can use up a lot of space (over 80G in total), you might wish to specify the folder where you want to save them. If you wish to do so, first create the folder, and then open python and run:

>>> import johnny5 as j5
>>> j5.dumps_path(new_path='full_path_to_dumps_folder')

The article class
=================

The :class:`johnny5.article` class is johnny5's main class. It can be initiallized using either the english Wikipedia curid of the page, the english title of the page, or the Wikidata ID. The following three examples lead to the same object corresponding to the article about Albert Einstein:

>>> a = j5.article(736)
>>> a = j5.article('Albert Einstein')
>>> a = j5.article('Q937')

Any object of this class is immediately linked to its correpsonding pages in all the available language editions of Wikipedia, and its corresponding Wikidata instance.

>>> print a
curid : 736
title : Albert Einstein
wdid  : Q937
L     : 193

Note that everytime you instanciate an :class:`johnny5.article` object, johnny5 will handle all the necessary redirects and title normalizations. This leads to two objects initialized with different titles, correspond to the same page:

>>> a1 = j5.article('Cesar Hidalgo')
>>> a2 = j5.article('Cesar A. Hidalgo')
>>> print a1
>>> print a2
curid : 49041372
title : Cesar A. Hidalgo
wdid  : Q22004920
L     : 2
curid : 49041372
title : Cesar A. Hidalgo
wdid  : Q22004920
L     : 2

The class :class:`johnny5.article` has three main methods to access the identifiers for the article:

.. autosummary::
    johnny5.article.title
    johnny5.article.curid
    johnny5.article.wdid

Addionally, the :class:`johnny5.article` can be instanciated with the `slow_connection` parameter set to `True` to keep the number of URL requests and API calls to the bare minimum. This is used when downloading bulks of articles.

For more information about the article class, read the full documentation in: :class:`johnny5.article`.

All the other classes in this module are subclasses of article. The currently supported subclasses are listed below.

.. autosummary::
    johnny5.biography
    johnny5.place
    johnny5.band
    johnny5.song

Using the dumps
===============

Here we will talk about how to use the Wikidata and the Wikipedia dumps to download bulks of data.

Examples
========

Language editions
-----------------

To get a list of the titles of a given page in all the available language editions we must first create an instance of the article class:

>>> a = j5.article('Albert Einstein')

We can directly use the function :func:`~johnny5.article.langlinks`, which returns a dictionary with the code of each language edition as keys and the titles as values:

>>> a.langlinks()
{u'af': u'Albert Einstein',
 u'als': u'Albert Einstein',
 u'am': u'\u12a0\u120d\u1260\u122d\u1275 \u12a0\u12ed\u1295\u1235\u1273\u12ed\u1295',
 u'an': u'Albert Einstein',
 u'ang': u'Albert Einstein',
 u'ar': u'\u0623\u0644\u0628\u0631\u062a \u0623\u064a\u0646\u0634\u062a\u0627\u064a\u0646',
 u'arz': u'\u0627\u0644\u0628\u0631\u062a \u0627\u064a\u0646\u0634\u062a\u0627\u064a\u0646',
 u'as': u'\u098f\u09b2\u09ac\u09be\u09f0\u09cd\u099f \u0986\u0987\u09a8\u09b7\u09cd\u099f\u09be\u0987\u09a8',
 u'ast': u'Albert Einstein',
 u'ay': u'Albert Einstein'
 ...}

 However, we can directly pass the language as a parameter to :func:`~johnny5.article.langlinks`

 >>> a.langlinks(lang='es')
 u'Albert Einstein'

We must note that the class will only get the langlinks the first time we run the :func:`~johnny5.article.langlinks` function. Every time we access a langlink after that, we are accessing information that is already stored in the object.

Finally, if we are only interested in the number of language editions we can use the :func:`~johnny5.article.L` function:

>>> a.L()
193


Pageviews
---------

Occupation classifier
---------------------

johnny5 comes with a pre-trained classifier that retrieves a set of features from the english Wikipedia and from Wikidata to classify each biography according to their major field of contribution. The classifier is an object from the :class:`~johnny5.Occ` class.

First, we must initialize the classifier. This operation takes a couple of seconds since it needs to load the necessary files:

>>> C = j5.Occ()

Next, we must instantiate an object from the :class:`~johnny5.biography` class:

>>> a = j5.biography('Cesar A. Hidalgo')
>>> print a
curid : 49041372
title : Cesar A. Hidalgo
wdid  : Q22004920
L     : 2

Finally, we run the classifier on our biography using the function :func:`~johnny5.Occ.classify`:

>>> C.classify(a)
('PHYSICIST', 3.4677045561392363)

The first variable returned by :func:`~johnny5.Occ.classify` corresponds to the occupation, and the second is a measure of how confident the classifier is on the result (the ratio between the most likely occupation, and the second most likely occupation). When the biography corresponds to the training set, the second returned variable is set to zero. 

If we are interested in seeing the probabilities for all the occupations, :func:`~johnny5.Occ.classify` has the parameter `return_all`:

>>> C.classify(a,return_all=True)
[('PHYSICIST', 0.24419204799823266),
 ('COMPUTER SCIENTIST', 0.070418931037799684),
 ('PHYSICIAN', 0.048714601309136539),
 ('ASTRONOMER', 0.030870808149097931),
 ('INVENTOR', 0.029528979978649089),
 ('EXTREMIST', 0.028802557416552358),
 ('BIOLOGIST', 0.027603077637510905),
 ('CHEMIST', 0.027392376292808687),
 ('POLITICIAN', 0.024383442762041466),
 ('MILITARY PERSONNEL', 0.022729935267372121),
 ...]

Finally, you can also run the classifier without instanciating any :class:`~johnny5.Occ` object, using the :func:`~johnny5.biography.occupation` function from the :class:`~johnny5.biography` class. This is not recommended, however, since it loads a classifier everytime, making this very slow when you need to classify many biographies.

>>> a.occupation()
('PHYSICIST', 3.4677045561392363)

A way to avoid each biography loading a classifier each time is to pass it the classifier as an argument.

>>> a.occupation(C=C)
('PHYSICIST', 3.4677045561392363)

Below is a sample code that iterates over a two biographies and prints the occupations for each of them:

>>> import johnny5 as j5
>>> C = j5.Occ()
>>> names = ['Cesar A. Hidalgo','Nicholas Negroponte']
>>> for name in names:
>>>    b = j5.biography(name)
>>>    print b.occupation(C=C)
('PHYSICIST', 3.4677045561392363)
('ARCHITECT', 4.9926055695222313)

List all the universities
-------------------------

johnny5 allows users to get a full list of articles that belong to a certain Wikidata category.



