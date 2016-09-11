# Import the os module, for the os.walk function
from __future__ import division
from itertools import groupby
import math
import time
import os
import re
 
STOP_WORD = open("stop.txt","r")
stop = STOP_WORD.read();
STOP_WORD.close();
stop=stop.split("\n")

# Set the directory you want to start from
rootDir = 'cloud'
for dirName, subdirList, fileList in os.walk(rootDir):
    print('Found directory: %s' % dirName)
    for fname in fileList:
        print('\t%s' % fname)
	string = open(dirName +'/'+fname).read()
	new_str = re.sub('[^a-zA-Z0-9\n\.]', ' ', string)
	new_str = re.sub(r'\.\.+', ' ', new_str).replace('.', ' ')
	open(dirName + '/'+fname, 'w').write(new_str)
	keywords_file = open(dirName + '/'+fname, "r")
	keywords = keywords_file.read();
	'''
	keywords = keywords.replace('.',' ').replace(';',' ').replace(':',' ').replace('(',' ').replace(')',' ').replace('/',' ').replace('\'',' 	').replace(',',' ').replace('[',' ').replace(']',' ').replace('~',' ').replace('\"',' ').replace('-',' ')
	'''
	keyword_list = keywords.split()
	keyword_list = [x.lower() for x in keyword_list]
	contentreal=[]
	for word in keyword_list:
		if (word not in stop) and (len(word) < 20):
			contentreal.append(word)
	str1 = ' '.join(contentreal)
	open(dirName + '/'+fname, "w").write(str1);
	os.system("sudo truncate -s  2K %s".format(dirName +'/'+fname))

'''
contentreal.sort()
frequency = [(key,len(list(group))) for key,group in groupby (contentreal)]
frequency = sorted(frequency,key = lambda x:x[1],reverse = True)
length = int(math.ceil(len(frequency)*.20))
frequency = frequency[:length]
frequency = [x[0] for x in frequency]
for item in frequency:
	open('cloud/1/data/food/back1.txt', 'w').write(item)
keywords_file.close();
'''
