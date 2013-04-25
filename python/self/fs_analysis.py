import time
import os
from datetime import datetime
from datetime import timedelta
 
def line_count(filename):
    '''Count file's lines neglect '\n' '''
    count=0
    #print filename
    for line in open(filename):
        if(line!='\n'):count+=1
    return count
 
def file_count(dirname,filter_types=[]):
    '''Count the files in a directory includes its subfolder's files
       You can set the filter types to count specific types of file'''
    count=0
    filter_is_on=False
    if filter_types!=[]: filter_is_on=True
    for item in os.listdir(dirname):
        abs_item=os.path.join(dirname,item)
        #print item
        if os.path.isdir(abs_item):
            #Iteration for dir
            count+=file_count(abs_item,filter_types)
        elif os.path.isfile(abs_item):
            if filter_is_on:
                #Get file's extension name
                extname=os.path.splitext(abs_item)[1]
                if extname in filter_types:
                    count+=1
            else:
                count+=1
    return count
def file_changed_count(dirname,base_time,filter_types=[]):
    '''Count the files in a directory includes its subfolder's files.
       You can set the filter types to count specific types of file.
       And set basetiem to count the file if it's modified time is over base_time'''
    count=0
    filter_is_on=False
    if filter_types!=[]: filter_is_on=True
    for item in os.listdir(dirname):
        abs_item=os.path.join(dirname,item)
        if os.path.isdir(abs_item):
            #Iteration for dir
            count+=file_count(abs_item,filter_types)
        elif os.path.isfile(abs_item):
            mt=datetime.fromtimestamp(os.stat(abs_item)[8])
            if mt>base_time:
                if filter_is_on:
                    #Get file's extension name
                    extname=os.path.splitext(abs_item)[1]
                    if extname in filter_types:
                        count+=1
                else:
                    count+=1
    return count
 
def count_all_lines(dirname,filter_types=[]):
    '''Count all files' lines of specific types in one directory includes its
       subdirectories.'''
    count=0
    filter_is_on=False
    if filter_types!=[]: filter_is_on=True
 
    for item in os.listdir(dirname):
        abs_item=os.path.join(dirname,item)
        if os.path.isdir(abs_item):
            count+=count_all_lines(abs_item,filter_types)
        elif os.path.isfile(abs_item):
            if filter_is_on:
                #Get file's extension name
                extname=os.path.splitext(abs_item)[1]
                if extname in filter_types:
                    count+=line_count(abs_item)
            else:
                count+=line_count(abs_item)
    return count