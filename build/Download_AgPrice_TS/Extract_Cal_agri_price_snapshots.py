# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 14:48:10 2020

@author: clohani
"""

from bs4 import BeautifulSoup
from urllib.request import urlopen
import requests

sitelinks= ["https://usda.library.cornell.edu/concern/publications/c821gj76b?release_start_date=2007-01-01&release_end_date=2020-04-09"]

for i in range(2,18):
    link="https://usda.library.cornell.edu/concern/publications/c821gj76b?locale=en&page="+ str(i) +"&release_end_date=2020-04-09&release_start_date=2007-01-01#release-items"
    sitelinks.append(link)
#print(sitelinks)

for site in sitelinks:

    html_page = urlopen(site).read()
    soup = BeautifulSoup(html_page)
    links = []
    links = [a['href'] for a in soup.find_all('a', href=True)]
    links_text = list(filter(lambda x : x.endswith('.txt'), links))
    links_pdf = list(filter(lambda x : x.endswith('.pdf'), links))
    links_zip = list(filter(lambda x : x.endswith('.zip'), links))
    
    #print(links_text)
    #print(links_pdf)
    #print(links_zip)
    
    for url in links_text :
        save_adr= "C:/Users/clohani/Dropbox/California_Crop_Snapshots/Txt/" + url.split('/')[-1]
        print(save_adr)
        r = requests.get(url, allow_redirects=True)
        open(save_adr, 'wb').write(r.content)
    
    for url in links_pdf :
        save_adr= "C:/Users/clohani/Dropbox/California_Crop_Snapshots/Pdf/" + url.split('/')[-1]
        print(save_adr)
        r = requests.get(url, allow_redirects=True)
        open(save_adr, 'wb').write(r.content)
    
    for url in links_zip :
        save_adr= "C:/Users/clohani/Dropbox/California_Crop_Snapshots/Zip/" + url.split('/')[-1]
        print(save_adr)
        r = requests.get(url, allow_redirects=True)
        open(save_adr, 'wb').write(r.content)
    
    
import os, zipfile

dir_name = 'C:/Users/clohani/Dropbox/California_Crop_Snapshots/Zip'
extension = ".zip"

os.chdir(dir_name) # change directory from working dir to dir with files

for item in os.listdir(dir_name):
    if not item.endswith(extension):
        os.remove(item)
        
for item in os.listdir(dir_name): # loop through items in dir
    if item.endswith(extension): # check for ".zip" extension
        file_name = os.path.abspath(item) # get full path of files
        zip_ref = zipfile.ZipFile(file_name) # create zipfile object
        final_name = file_name.split('.zip')[0]
        if not os.path.exists(final_name):
            os.mkdir(final_name)
        zip_ref.extractall(final_name) # extract file to dir
        zip_ref.close() # close file