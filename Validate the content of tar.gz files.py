# Databricks notebook source
# DBTITLE 1,Display files in the mount point (Optional) 
display(dbutils.fs.ls("/mnt/test_container/"))

# COMMAND ----------

# DBTITLE 1,Setting Flags


# COMMAND ----------

# DBTITLE 1,Set Yesterday Date
from datetime import datetime,timedelta
import pytz

yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

def getMYTtime():
    current_datetime = datetime.now(pytz.timezone('Asia/Kuala_Lumpur'))
    return current_datetime

print(getMYTtime())

print(datetime.now())
print(yesterday)

# COMMAND ----------

# DBTITLE 1,Reading JSON config file
import json

with open('/dbfs/mnt/test_container/MASTER/config.json','r') as json_file:
    data=json.load(json_file)

system=data['System']
file_format=data['Format']
summary=data['Summary']
row_limit=data['Row_Limit']
date_ref=data['Date'] # 'Today' or 'Yesterday'
email_to=data['Email_To']
email_cc=data['Email_CC']
logfile_name=data['Log_File']+"_"+yesterday+".txt"
logfile_path_folder="/dbfs/mnt/test_container/LOG/"+system+"/"+yesterday+"/"
logfile_path="/dbfs/mnt/test_container/LOG/"+system+"/"+yesterday+"/"+logfile_name
Options=list(data['Optional'][0].keys())
print(system)
print(file_format)
print(summary)
print(row_limit)
print(date_ref)
print(email_to)
print(email_cc)
print(logfile_path_folder)
print(logfile_path)
#print(Params)
#print(Options)

# COMMAND ----------

# DBTITLE 1,Create logging file path and name
import os 

base_path=logfile_path_folder 
print("Before : ",os.path.exists(base_path))

if not os.path.exists(base_path): 
    base_path2=base_path.replace("/dbfs", "dbfs:")
    print(base_path2)
    dbutils.fs.mkdirs(base_path2)

try:
    logfile_path_mod=logfile_path.replace("/dbfs", "dbfs:")
    dbutils.fs.put(logfile_path_mod, "")
    print("Logfile created.")
except Exception as err:
        print(err)

print("After : ",os.path.exists(base_path))

# COMMAND ----------

# DBTITLE 1,Creation of a logging list
logging_entries=[]
logging_entries.append(str(getMYTtime())+" Logging Started...")

def logging_entry():
    with open(logfile_path, 'w') as fp:
        for item in logging_entries:
            #print(item)
            fp.write("%s\n" % item)
        print('Action Logged.')

logging_entry()

# COMMAND ----------

# DBTITLE 1,Defining Additional Parameters + UNPROCESSED file path
root_path="/dbfs/mnt/test_container/MASTER/INBOUND/UNPROCESSED/"
archive_path="/dbfs/mnt/test_container/MASTER/INBOUND/ARCHIVED/"

import os

file_paths=[]

# Check if system name is in files and append those to a list
for path,subdirs,files in os.walk(root_path):
    for name in files:
        if (system in name) & (yesterday in name) & (file_format in name):            
            file_paths.append(os.path.join(path,name))
            logging_entries.append(str(getMYTtime())+" File Path is Found: "+os.path.join(path,name))
            
if file_paths:
    print(file_paths)
else:
    logging_entries.append(str(getMYTtime())+" ERROR: File Path is not Found. ")

logging_entry()

# COMMAND ----------

# DBTITLE 1,Reading files inside tar.gz file
'''import tarfile

for filename in file_paths: # Go through every file in the list 
    targz = tarfile.open(filename, "r:gz")
    for filename in targz.getnames():   # Get filenames
        try:
            files = targz.extractfile(filename)
            Data = files.read()
            print (filename)
            #print (filename, ':', Data)
        except :
            print ('ERROR: Unable to find %s' % filename)

targz.close()'''

# COMMAND ----------

# DBTITLE 1,tar.gz file getmembers() Method + Extracting Files
import tarfile

if file_paths: # Check if file_paths[] is not empty
    for filename in file_paths: # Go through every file in the list 
        tar = tarfile.open(filename, "r:gz")
        for member in tar.getmembers():
            #print(member)
            extracted_f = tar.extract(member, root_path)
            print ('Extracted %s' % member)
            logging_entries.append(str(getMYTtime())+' Extracted %s' % member)
    tar.close()
else:
    print("ERROR: No file path is found.")
    logging_entries.append(str(getMYTtime())+" ERROR: No file path is found.")

logging_entry()

# COMMAND ----------

# DBTITLE 1,Move file from UNPROCESSED to ARCHIVED
if file_paths:
    for current_path in file_paths:
        archive_path=current_path.replace("UNPROCESSED", "ARCHIVED")
        archive_path=archive_path.replace("/dbfs", "dbfs:")
        current_path=current_path.replace("/dbfs", "dbfs:")
        try:
            dbutils.fs.mv(current_path, archive_path)
            print ('SUCCESS: Moved %s' % current_path)
            logging_entries.append(str(getMYTtime())+' SUCCESS: Moved %s' % current_path)
        except :
            print ('ERROR: Unable to move %s' % current_path)
            logging_entries.append(str(getMYTtime())+' ERROR: Unable to move %s' % current_path)
        #print(archive_path)
else:
    print("ERROR: No file path is found.")
    logging_entries.append(str(getMYTtime())+" ERROR: No file path is found.")

logging_entry()

# COMMAND ----------

# DBTITLE 1,Validate Summary File
summary_path="/dbfs/mnt/test_container/MASTER/INBOUND/UNPROCESSED/"

import os

summary_file_paths=[]

# Check if system name is in files and append those to a list
for path,subdirs,files in os.walk(root_path):
    for name in files:
        if (system in name) & (yesterday in name) & (summary in name):            
            summary_file_paths.append(os.path.join(path,name))

print(summary_file_paths)
logging_entries.append(str(getMYTtime())+" SUMMARY File: "+str(summary_file_paths))

logging_entry()

# COMMAND ----------

# DBTITLE 1,Check individual filenames and validation
individual_file_path="/dbfs/mnt/test_container/MASTER/INBOUND/UNPROCESSED/"

import os,re

individual_file_paths=[]

# Check if system name is in files and append those to a list
for path,subdirs,files in os.walk(individual_file_path):
    for name in files:
        try:
            checklist = re.search(r'_\d\d.txt$', name)
            #print(checklist.group())
            if (system in name) & (yesterday in name) & (checklist.group() in name):            
                individual_file_paths.append(os.path.join(path,name))
        except:
            pass       

print(individual_file_paths)
logging_entries.append(str(getMYTtime())+" Individual File Paths: "+str(individual_file_paths))

logging_entry()

# COMMAND ----------

# DBTITLE 1,Read the Summary File
import pandas as pd

df_summary = pd.read_csv(summary_file_paths[0], sep="|", header=None)
print(df_summary)
logging_entries.append(str(getMYTtime())+" Summary DataFrame: "+str(df_summary))

logging_entry()

# COMMAND ----------

# DBTITLE 1,Check Summary File and Individual File Row Counts
file_content = [[] for i in range(len(individual_file_paths))]

for i, files in enumerate(individual_file_paths):
    with open(files, 'r') as fp:
        file_only = re.search(r'[A-Za-z0-9_]*.txt$', files)
        file_only = file_only.group().replace(".txt", "")
        row_count = sum(1 for line in fp)
        file_content[i] = [file_only,row_count]
df_files = pd.DataFrame(file_content)
print(df_files)
logging_entries.append(str(getMYTtime())+" Summary DataFrame: "+str(df_summary))

if df_files.equals(df_summary):
    print("SUCCESS: Summary file and Row Counts are validated.")
    logging_entries.append(str(getMYTtime())+" SUCCESS: Summary file and Row Counts are validated.")
else:
    print("ERROR: Summary file and Row Counts are not validated.")
    logging_entries.append(str(getMYTtime())+" ERROR: Summary file and Row Counts are not validated.")

logging_entry()

# COMMAND ----------

# DBTITLE 1,Table Dictionary
df_tbl_dic = pd.read_csv("/dbfs/mnt/test_container/MASTER/MASTER_TABLE_DICTIONARY.txt", sep="|", header=None)
column_count=len(df_tbl_dic.columns)

print(column_count)
logging_entries.append(str(getMYTtime())+" Table Dictionary Column Count: "+str(column_count))

logging_entry()

# COMMAND ----------

# DBTITLE 1,(Optional)
'''import csv

for i, files in enumerate(individual_file_paths):
    with open(files, 'r') as f:
        reader = csv.reader(f, delimiter='|', skipinitialspace=True)
        first_row = next(reader)
        num_cols = len(first_row)
        if num_cols==column_count:
            print ('SUCCESS: Correct Column Count for %s' % files)
        else:
            print ('ERROR: Incorrect Column Count for %s' % files)
        #print(num_cols)
'''

# COMMAND ----------

# DBTITLE 1,Check Table Dictionary and Individual File Column Counts
for i, files in enumerate(individual_file_paths):
    with open(files, 'r') as input_file:
        n=0
        for line in input_file:
            if len(line.split("|"))==column_count:
                pass
            else:
                print ("ERROR: Incorrect Column Count of ",len(line.split("|"))," for ",files," instead ",column_count,": Row No: ",n+1)
                logging_entries.append(str(getMYTtime())+"ERROR: Incorrect Column Count of "+str(len(line.split()))+" for "+str(files)+" instead "+str(column_count)+": Row No: "+str(n+1))
            n=n+1
            if (n>int(row_limit)):
                break
                 
logging_entry()

# COMMAND ----------

# DBTITLE 1,View logging entries
for item in logging_entries:
    print(item)
