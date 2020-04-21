# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 11:23:03 2020

@author: clohani
"""

from bs4 import BeautifulSoup
import glob,os.path
import csv
#soup = BeautifulSoup(open("C:/Users/clohani/Dropbox/California_Crop_Snapshots/Zip/agpr0120/agpr_index.htm"), "html.parser")


filesDepth = glob.glob('C:/Users/clohani/Dropbox/California_Crop_Snapshots/Zip/*')
dirsDepth = list(filter(lambda f: os.path.isdir(f), filesDepth))

output_rows = []
output_row=[]

for folder in dirsDepth:
    foldername= folder.split('\\')[1]
    folder= folder.replace('\\','/')
    print(folder + '*ind*.htm')
    url= glob.glob(folder + '/*ind*.htm')[0]
    #url = "C:/Users/clohani/Dropbox/California_Crop_Snapshots/Zip/agpr0119/agpr_index.htm"
    page = open(url,encoding='latin-1')
    soup = BeautifulSoup(page.read())
    
    tables = soup.find_all("table")
    
    for i, table in enumerate(tables, start=1):
        print(table)
        print(i)
        counter=-3
        for table_element in table.findAll('td'):
            counter= counter+1
            if counter==-2 or counter==-1 :
                continue
            if counter%3==0 :
                output_row.append(foldername)
                output_rows.append(output_row)
                output_row = []
            output_row.append(table_element.text)
        
    
with open('C:/Users/clohani/OneDrive/Desktop/AG_TS_output.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(output_rows)