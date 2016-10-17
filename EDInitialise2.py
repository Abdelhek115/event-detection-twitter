
# coding: utf-8

# 
# ## Tweets
# 
# A tweet consists of many data fields. [Here is an example](https://gist.github.com/arapat/03d02c9b327e6ff3f6c3c5c602eeaf8b). You can learn all about them in the Twitter API doc. We are going to briefly introduce only the data fields that will be used in this homework.
# 
# * `created_at`: Posted time of this tweet (time zone is included)
# * `id_str`: Tweet ID - we recommend using `id_str` over using `id` as Tweet IDs, becauase `id` is an integer and may bring some overflow problems.
# * `text`: Tweet content
# * `user`: A JSON object for information about the author of the tweet
#     * `id_str`: User ID
#     * `name`: User name (may contain spaces)
#     * `screen_name`: User screen name (no spaces)
# * `retweeted_status`: A JSON object for information about the retweeted tweet (i.e. this tweet is not original but retweeteed some other tweet)
#     * All data fields of a tweet except `retweeted_status`
# * `entities`: A JSON object for all entities in this tweet
#     * `hashtags`: An array for all the hashtags that are mentioned in this tweet
#     * `urls`: An array for all the URLs that are mentioned in this tweet
# 
# 
# ## Data source
# 
# All tweets are collected using the [Twitter Streaming API](https://dev.twitter.com/streaming/overview).
# 
# 
# ## Users partition
# 
# Besides the original tweets, we will provide you with a Pickle file, which contains a partition over 452,743 Twitter users. It contains a Python dictionary `{user_id: partition_id}`. The users are partitioned into 7 groups.

# # Part 0: Load data to a RDD

# The tweets data is stored on AWS S3. We have in total a little over 1 TB of tweets. We provide 10 MB of tweets for your local development. For the testing and grading on the homework server, we will use different data.
# 
# ## Testing on the homework server
# In the Playground, we provide three different input sizes to test your program: 1 GB, 10 GB, and 100 GB. To test them, read files list from `../Data/hw2-files-1gb.txt`, `../Data/hw2-files-5gb.txt`, `../Data/hw2-files-20gb.txt`, respectively.
# 
# For final submission, make sure to read files list from `../Data/hw2-files-final.txt`. Otherwise your program will receive no points.
# 
# ## Local test
# 
# For local testing, read files list from `../Data/hw2-files.txt`.
# Now let's see how many lines there are in the input files.
# 
# 1. Make RDD from the list of files in `hw2-files.txt`.
# 2. Mark the RDD to be cached (so in next operation data will be loaded in memory) 
# 3. call the `print_count` method to print number of lines in all these files
# 
# It should print
# ```
# Number of elements: 2193
# ```

# In[44]:

get_ipython().magic(u'pylab inline')


# In[9]:

# Your code here
with open('./Data/sliced.txt') as f:
    files = [l.strip() for l in f.readlines()]
rdd1 = sc.textFile(','.join(files)).cache()
rdd1.count()


# # Part 1: Parse JSON strings to JSON objects

# Python has built-in support for JSON.
# 
# **UPDATE:** Python built-in json library is too slow. In our experiment, 70% of the total running time is spent on parsing tweets. Therefore we recommend using [ujson](https://pypi.python.org/pypi/ujson) instead of json. It is at least 15x faster than the built-in json library according to our tests.

# ## Broken tweets and irrelevant messages
# 
# The data of this assignment may contain broken tweets (invalid JSON strings). So make sure that your code is robust for such cases.
# 
# In addition, some lines in the input file might not be tweets, but messages that the Twitter server sent to the developer (such as [limit notices](https://dev.twitter.com/streaming/overview/messages-types#limit_notices)). Your program should also ignore these messages.
# 
# *Hint:* [Catch the ValueError](http://stackoverflow.com/questions/11294535/verify-if-a-string-is-json-in-python)
# 
# 
# (1) Parse raw JSON tweets to obtain valid JSON objects. From all valid tweets, construct a pair RDD of `(user_id, text)`, where `user_id` is the `id_str` data field of the `user` dictionary (read [Tweets](#Tweets) section above), `text` is the `text` data field.

# In[10]:

import ujson

def safe_parse(x):
    try:
        json_object = ujson.loads(x)
    except ValueError, e:
        pass # invalid json
    else:
        return json_object

# your code here


# In[28]:

import re
regex = re.compile('[%s]' % re.escape('!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~\n'))


# In[30]:

# rdd2 = rdd1.filter(lambda x: 'user' in x and 'id_str' in x and 'text' in x).map(safe_parse)\
#         .map(lambda x:  x['text'].encode('utf-8')).cache()
rdd2 = rdd1.filter(lambda x: 'user' in x and 'id_str' in x and 'text' in x).map(safe_parse)        .map(lambda x: regex.sub('', x["text"].lower()).encode('utf-8')).cache()
N = rdd2.count()
N


# # Remove Punctuation Stopwords And Strip

# In[12]:

# ## removing punctuation stopwords and https
# import string
# import nltk
# from nltk.corpus import stopwords
# from nltk.stem import PorterStemmer
# stop_words =set(stopwords.words("english"))
# stop_words|=set(["edu", "com", "also", "still", "anyone", "cc" , "ca", "us", "much", "even", "would", "see", "rt", 'is', 'of'])
# st = PorterStemmer()

# #IMP: Put yout nltk_data folder in /usr/local/share for it to work :)

# def getwords(tweet):
#     return [x for x in tweet if not ( x.startswith('https') or x in stop_words or x is ' ')]
# punc = string.punctuation+"\n"
# table_t = string.maketrans(punc, " "*len(punc))  ##TODO empty word
# tweetNeat = rdd2.map(lambda s: s.translate(table_t).lower())\
#             .map(lambda s: getwords(s.decode('utf-8').split(" "))).filter(lambda x: len(x)>0).map(lambda x: filter(None,x))
# tweetNeat.take(2)


# In[31]:

## removing punctuation stopwords and https
import string
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
stop_words =set(stopwords.words("english"))
stop_words|=set(["edu", "com", "also", "still", "anyone", "cc" , "ca", "us", "much", "even", "would", "see", "rt", 'is', 'of'])
st = PorterStemmer()

#IMP: Put yout nltk_data folder in /usr/local/share for it to work :)

def getwords(tweet):
    return [st.stem(x) for x in tweet if not ( x.startswith('htt') or x.startswith('@') or x in stop_words or x is ' ')]

punc = string.punctuation+"\n"
tweetNeat = rdd2.map(lambda s: getwords(s.decode('utf-8').split(" "))).filter(lambda x: len(x)>0).map(lambda x: filter(None,x))
tweetNeat.first()


# # Make Dictionary of Words

# In[32]:

## make dictionary 
vocab = tweetNeat.flatMap(lambda x: x).distinct().collect()
V = len(vocab)
vocab


# # Histogram of Words

# In[34]:

## Annotate words
import numpy as np

tweetHist = tweetNeat.map(lambda s: np.array([vocab.index(k) for k in s if k in vocab]))          .map(lambda y: np.bincount(y.astype(int), minlength=len(vocab)))
#tweetHist.take(2)
tweetHist.first()


# # Part 2 : Gibbs Sampler for Naive Bayes
# Naive Bayes:\\ 
# //Prior
# • For each latent class c ∈ {1,...,K} 
#     • θ(c) ∼ Dirichlet(α)
# • π ∼ Dirichlet(β) //Likelihood
# • For each document d ∈ {1,...,N}
#     • Document latent class z(d) ∼ discrete(π) 
#     • For each word i in document d
#         • w(d) ∼ discrete(θ(z(d))) //Sample a word i

# In[35]:

def sampleFromDiscrete(probs):
    temp = random.random()
    total =0
    for i in range(len(probs)):
        total+=probs[i]
        if(temp<total):
            return i
    return 0


# Z initialised based on similarity measure

# In[48]:

K=15 # NO of classes.
alpha = np.ones(V)*0.1 #hyperparameter
A = [alpha]*K
beta = np.ones(K)
pi = np.random.dirichlet(beta)
pi1=pi
theta = np.random.dirichlet(alpha, K)
no_tweets = N
doc_vec = tweetHist.collect()
docs = set(range(0, N))
z = [0]*N
for i in range(K):
    d = docs.pop()
    z[d] = i
    dist = np.linalg.norm(doc_vec - doc_vec[d], axis=1)
    sorted_dist = np.argsort(dist)[0:N/K]
    for k in sorted_dist:
        docs.discard(k)
        doc_vec[k] = [-np.inf]*len(doc_vec[0])
        z[k] = i


# # Gibbs Sampler (Naive Bayes)

# In[52]:

np.bincount(z) + beta


# In[50]:

print np.shape(beta)


# In[54]:

doc_vectors = tweetHist.collect()
for i in range(80):
    print bincount(z)
    pi = np.random.dirichlet(beta + np.bincount(z, minlength=K))
    sum_z = np.zeros((K, V))
    for i in range(no_tweets):
        sum_z[z[i]]+=doc_vectors[i]
    for i in range(K):
        theta[i] = np.random.dirichlet(A[i]+sum_z[i])
    for d in range(no_tweets):
        for i in range(K):
            pi1[i]= np.dot(np.log(theta[i]),doc_vectors[d])
        pi1 = np.exp(pi1-np.max(pi1))
        for i in range(K):
            pi1[i] *= pi[i]
        pi1 = pi1/np.sum(pi1)
        #print pi1
        z[d]=sampleFromDiscrete(pi1)


# In[55]:

print z


# In[56]:

print np.bincount(z)
freq_class = argsort(bincount(z))[::-1][: 8]
print freq_class


# In[57]:

print len(theta)
indices = argsort(theta, axis = 1)
best = indices[:,-40:]
for k in freq_class:
    wordlist = [vocab[best[k][j]] for j in range(len(best[0]))]          
    print wordlist


# # Part 2: Number of posts from each user partition

# Load the Pickle file `../Data/users-partition.pickle`, you will get a dictionary which represents a partition over 452,743 Twitter users, `{user_id: partition_id}`. The users are partitioned into 7 groups. For example, if the dictionary is loaded into a variable named `partition`, the partition ID of the user `59458445` is `partition["59458445"]`. These users are partitioned into 7 groups. The partition ID is an integer between 0-6.
# 
# Note that the user partition we provide doesn't cover all users appear in the input data.

# (1) Load the pickle file.

# # your code here
# import pickle
# file_Name = '../Data/users-partition.pickle'
# fileObject = open(file_Name,'r')  
# b = pickle.load(fileObject) 

# (2) Count the number of posts from each user partition
# 
# Count the number of posts from group 0, 1, ..., 6, plus the number of posts from users who are not in any partition. Assign users who are not in any partition to the group 7.
# 
# Put the results of this step into a pair RDD `(group_id, count)` that is sorted by key.

# # your code here
# default = 7
# counts_id = rdd2.map(lambda x: (b.get( x[0], default), 1))\
#                 .reduceByKey(lambda x,y:x+y).sortByKey()

# (3) Print the post count using the `print_post_count` function we provided.
# 
# It should print
# 
# ```
# Group 0 posted 81 tweets
# Group 1 posted 199 tweets
# Group 2 posted 45 tweets
# Group 3 posted 313 tweets
# Group 4 posted 86 tweets
# Group 5 posted 221 tweets
# Group 6 posted 400 tweets
# Group 7 posted 798 tweets
# ```

# In[22]:

def print_post_count(counts):
    for group_id, count in counts:
        print 'Group %d posted %d tweets' % (group_id, count)


# # Part 3:  Tokens that are relatively popular in each user partition

# In this step, we are going to find tokens that are relatively popular in each user partition.
# 
# We define the number of mentions of a token $t$ in a specific user partition $k$ as the number of users from the user partition $k$ that ever mentioned the token $t$ in their tweets. Note that even if some users might mention a token $t$ multiple times or in multiple tweets, a user will contribute at most 1 to the counter of the token $t$.
# 
# Please make sure that the number of mentions of a token is equal to the number of users who mentioned this token but NOT the number of tweets that mentioned this token.
# 
# Let $N_t^k$ be the number of mentions of the token $t$ in the user partition $k$. Let $N_t^{all} = \sum_{i=0}^7 N_t^{i}$ be the number of total mentions of the token $t$.
# 
# We define the relative popularity of a token $t$ in a user partition $k$ as the log ratio between $N_t^k$ and $N_t^{all}$, i.e. 
# 
# \begin{equation}
# p_t^k = \log \frac{C_t^k}{C_t^{all}}.
# \end{equation}
# 
# 
# You can compute the relative popularity by calling the function `get_rel_popularity`.

# (0) Load the tweet tokenizer.

# In[23]:

# %load happyfuntokenizing.py
#!/usr/bin/env python

"""
This code implements a basic, Twitter-aware tokenizer.

A tokenizer is a function that splits a string of text into words. In
Python terms, we map string and unicode objects into lists of unicode
objects.

There is not a single right way to do tokenizing. The best method
depends on the application.  This tokenizer is designed to be flexible
and this easy to adapt to new domains and tasks.  The basic logic is
this:

1. The tuple regex_strings defines a list of regular expression
   strings.

2. The regex_strings strings are put, in order, into a compiled
   regular expression object called word_re.

3. The tokenization is done by word_re.findall(s), where s is the
   user-supplied string, inside the tokenize() method of the class
   Tokenizer.

4. When instantiating Tokenizer objects, there is a single option:
   preserve_case.  By default, it is set to True. If it is set to
   False, then the tokenizer will downcase everything except for
   emoticons.

The __main__ method illustrates by tokenizing a few examples.

I've also included a Tokenizer method tokenize_random_tweet(). If the
twitter library is installed (http://code.google.com/p/python-twitter/)
and Twitter is cooperating, then it should tokenize a random
English-language tweet.


Julaiti Alafate:
  I modified the regex strings to extract URLs in tweets.
"""

__author__ = "Christopher Potts"
__copyright__ = "Copyright 2011, Christopher Potts"
__credits__ = []
__license__ = "Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Unported License: http://creativecommons.org/licenses/by-nc-sa/3.0/"
__version__ = "1.0"
__maintainer__ = "Christopher Potts"
__email__ = "See the author's website"

######################################################################

import re
import htmlentitydefs

######################################################################
# The following strings are components in the regular expression
# that is used for tokenizing. It's important that phone_number
# appears first in the final regex (since it can contain whitespace).
# It also could matter that tags comes after emoticons, due to the
# possibility of having text like
#
#     <:| and some text >:)
#
# Most imporatantly, the final element should always be last, since it
# does a last ditch whitespace-based tokenization of whatever is left.

# This particular element is used in a couple ways, so we define it
# with a name:
emoticon_string = r"""
    (?:
      [<>]?
      [:;=8]                     # eyes
      [\-o\*\']?                 # optional nose
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth      
      |
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
      [\-o\*\']?                 # optional nose
      [:;=8]                     # eyes
      [<>]?
    )"""

# The components of the tokenizer:
regex_strings = (
    # Phone numbers:
    r"""
    (?:
      (?:            # (international)
        \+?[01]
        [\-\s.]*
      )?            
      (?:            # (area code)
        [\(]?
        \d{3}
        [\-\s.\)]*
      )?    
      \d{3}          # exchange
      [\-\s.]*   
      \d{4}          # base
    )"""
    ,
    # URLs:
    r"""http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"""
    ,
    # Emoticons:
    emoticon_string
    ,    
    # HTML tags:
     r"""<[^>]+>"""
    ,
    # Twitter username:
    r"""(?:@[\w_]+)"""
    ,
    # Twitter hashtags:
    r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""
    ,
    # Remaining word types:
    r"""
    (?:[a-z][a-z'\-_]+[a-z])       # Words with apostrophes or dashes.
    |
    (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
    |
    (?:[\w_]+)                     # Words without apostrophes or dashes.
    |
    (?:\.(?:\s*\.){1,})            # Ellipsis dots. 
    |
    (?:\S)                         # Everything else that isn't whitespace.
    """
    )

######################################################################
# This is the core tokenizing regex:
    
word_re = re.compile(r"""(%s)""" % "|".join(regex_strings), re.VERBOSE | re.I | re.UNICODE)

# The emoticon string gets its own regex so that we can preserve case for them as needed:
emoticon_re = re.compile(regex_strings[1], re.VERBOSE | re.I | re.UNICODE)

# These are for regularizing HTML entities to Unicode:
html_entity_digit_re = re.compile(r"&#\d+;")
html_entity_alpha_re = re.compile(r"&\w+;")
amp = "&amp;"

######################################################################

class Tokenizer:
    def __init__(self, preserve_case=False):
        self.preserve_case = preserve_case

    def tokenize(self, s):
        """
        Argument: s -- any string or unicode object
        Value: a tokenize list of strings; conatenating this list returns the original string if preserve_case=False
        """        
        # Try to ensure unicode:
        try:
            s = unicode(s)
        except UnicodeDecodeError:
            s = str(s).encode('string_escape')
            s = unicode(s)
        # Fix HTML character entitites:
        s = self.__html2unicode(s)
        # Tokenize:
        words = word_re.findall(s)
        # Possible alter the case, but avoid changing emoticons like :D into :d:
        if not self.preserve_case:            
            words = map((lambda x : x if emoticon_re.search(x) else x.lower()), words)
        return words

    def tokenize_random_tweet(self):
        """
        If the twitter library is installed and a twitter connection
        can be established, then tokenize a random tweet.
        """
        try:
            import twitter
        except ImportError:
            print "Apologies. The random tweet functionality requires the Python twitter library: http://code.google.com/p/python-twitter/"
        from random import shuffle
        api = twitter.Api()
        tweets = api.GetPublicTimeline()
        if tweets:
            for tweet in tweets:
                if tweet.user.lang == 'en':            
                    return self.tokenize(tweet.text)
        else:
            raise Exception("Apologies. I couldn't get Twitter to give me a public English-language tweet. Perhaps try again")

    def __html2unicode(self, s):
        """
        Internal metod that seeks to replace all the HTML entities in
        s with their corresponding unicode characters.
        """
        # First the digits:
        ents = set(html_entity_digit_re.findall(s))
        if len(ents) > 0:
            for ent in ents:
                entnum = ent[2:-1]
                try:
                    entnum = int(entnum)
                    s = s.replace(ent, unichr(entnum))	
                except:
                    pass
        # Now the alpha versions:
        ents = set(html_entity_alpha_re.findall(s))
        ents = filter((lambda x : x != amp), ents)
        for ent in ents:
            entname = ent[1:-1]
            try:            
                s = s.replace(ent, unichr(htmlentitydefs.name2codepoint[entname]))
            except:
                pass                    
            s = s.replace(amp, " and ")
        return s


# from math import log
# 
# tok = Tokenizer(preserve_case=False)
# 
# def get_rel_popularity(c_k, c_all):
#     return log(1.0 * c_k / c_all) / log(2)
# 
# 
# def print_tokens(tokens, gid = None):
#     group_name = "overall"
#     if gid is not None:
#         group_name = "group %d" % gid
#     print '=' * 5 + ' ' + group_name + ' ' + '=' * 5
#     for n, t in tokens:
#         print "%s\t%.4f" % (t.encode('utf-8'), -n)
#     print

# (1) Tokenize the tweets using the tokenizer we provided above named `tok`. Count the number of mentions for each tokens regardless of specific user group.
# 
# Call `print_count` function to show how many different tokens we have.
# 
# It should print
# ```
# Number of elements: 8949
# ```

# # your code here
# rdd_token2 = rdd2.mapValues(lambda x: tok.tokenize(x.encode('utf-8')))\
#                 .reduceByKey(lambda a, b: a+b).mapValues(lambda x: list(set(x)))\
#                 .map(lambda (c,v): (b.get( c, default),v))
# rdd_freq= rdd_token2.flatMap(lambda (c, v) : v).map(lambda x: (x,1)).reduceByKey(lambda a,b:a+b)
# print_count(rdd_freq)
# #rdd_token2.take(2)
# rdd_token2.take(2)

# (2) Tokens that are mentioned by too few users are usually not very interesting. So we want to only keep tokens that are mentioned by at least 100 users. Please filter out tokens that don't meet this requirement.
# 
# Call `print_count` function to show how many different tokens we have after the filtering.
# 
# Call `print_tokens` function to show top 20 most frequent tokens.
# 
# It should print
# ```
# Number of elements: 44
# ===== overall =====
# :	1388.0000
# rt	1237.0000
# .	826.0000
# …	673.0000
# the	623.0000
# trump	582.0000
# to	499.0000
# ,	489.0000
# a	404.0000
# is	376.0000
# in	297.0000
# of	292.0000
# and	288.0000
# for	281.0000
# !	269.0000
# ?	210.0000
# on	195.0000
# i	192.0000
# you	191.0000
# this	190.0000
# ```

# # your code here
# rdd_filt = rdd_freq.filter(lambda (c,v): v >=100)
# freq_sort = rdd_filt.sortBy(lambda x: (-x[1], x[0])).map(lambda (c,v): (-v, c))
# print_count(rdd_filt)
# print_tokens(freq_sort.take(20))

# (3) For all tokens that are mentioned by at least 100 users, compute their relative popularity in each user group. Then print the top 10 tokens with highest relative popularity in each user group. In case two tokens have same relative popularity, break the tie by printing the alphabetically smaller one.
# 
# **Hint:** Let the relative popularity of a token $t$ be $p$. The order of the items will be satisfied by sorting them using (-p, t) as the key.
# 
# It should print
# ```
# ===== group 0 =====
# ...	-3.5648
# at	-3.5983
# hillary	-4.0875
# i	-4.1255
# bernie	-4.1699
# not	-4.2479
# https	-4.2695
# he	-4.2801
# in	-4.3074
# are	-4.3646
# 
# ===== group 1 =====
# #demdebate	-2.4391
# -	-2.6202
# &	-2.7472
# amp	-2.7472
# clinton	-2.7570
# ;	-2.7980
# sanders	-2.8838
# ?	-2.9069
# in	-2.9664
# if	-3.0138
# 
# ===== group 2 =====
# are	-4.6865
# and	-4.7105
# bernie	-4.7549
# at	-4.7682
# sanders	-4.9542
# that	-5.0224
# in	-5.0444
# donald	-5.0618
# a	-5.0732
# #demdebate	-5.1396
# 
# ===== group 3 =====
# #demdebate	-1.3847
# bernie	-1.8480
# sanders	-2.1887
# of	-2.2356
# that	-2.3785
# the	-2.4376
# …	-2.4403
# clinton	-2.4467
# hillary	-2.4594
# be	-2.5465
# 
# ===== group 4 =====
# hillary	-3.7395
# sanders	-3.9542
# of	-4.0199
# clinton	-4.0790
# at	-4.1832
# in	-4.2143
# a	-4.2659
# on	-4.2854
# .	-4.3681
# the	-4.4251
# 
# ===== group 5 =====
# cruz	-2.3861
# he	-2.6280
# are	-2.7796
# will	-2.7829
# the	-2.8568
# is	-2.8822
# for	-2.9250
# that	-2.9349
# of	-2.9804
# this	-2.9849
# 
# ===== group 6 =====
# @realdonaldtrump	-1.1520
# cruz	-1.4532
# https	-1.5222
# !	-1.5479
# not	-1.8904
# …	-1.9269
# will	-2.0124
# it	-2.0345
# this	-2.1104
# to	-2.1685
# 
# ===== group 7 =====
# donald	-0.6422
# ...	-0.7922
# sanders	-1.0282
# trump	-1.1296
# bernie	-1.2106
# -	-1.2253
# you	-1.2376
# clinton	-1.2511
# if	-1.2880
# i	-1.2996
# ```

# # your code here
# rdd_group= rdd_token2.flatMapValues(lambda v : v).map(lambda (c,v): (v,c))\
#             .map(lambda x: (x,1)).reduceByKey(lambda a,b:a+b)
# #rdd_group.take(5)
# def getKey(item):
#     return item[1]
# #, key=getKey, reverse=True
# 
# rdd_joined = rdd_filt.join(rdd_group.map(lambda ((t, w), u): (t, (w,u))))\
#               .map(lambda (t, (v, (w, u))): ((t, w), (u, v)))
# rdd_grouped = rdd_joined.mapValues(lambda x: log(1.0 * x[0] / x[1]) / log(2))\
#                    .map(lambda ((t, w), u): (w, (-u,t))).groupByKey()\
#                     .mapValues(lambda x: sorted(x))
#         #.mapValues(lambda (c,v): (v, -c))
# #rdd_grouped.take(8)        
# for n in range(8):
#     print_tokens(rdd_grouped.collectAsMap()[n][0:10],n)
#     
#     

# (4) (optional, not for grading) The users partition is generated by a machine learning algorithm that tries to group the users by their political preferences. Three of the user groups are showing supports to Bernie Sanders, Ted Cruz, and Donald Trump. 
# 
# If your program looks okay on the local test data, you can try it on the larger input by submitting your program to the homework server. Observe the output of your program to larger input files, can you guess the partition IDs of the three groups mentioned above based on your output?

# # Change the values of the following three items to your guesses
# 
# users_support = [
#     (-1, "Bernie Sanders"),
#     (-1, "Ted Cruz"),
#     (-1, "Donald Trump")
# ]
# 
# for gid, candidate in users_support:
#     print "Users from group %d are most likely to support %s." % (gid, candidate)

# In[ ]:



