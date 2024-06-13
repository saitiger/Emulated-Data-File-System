#!/usr/bin/env python
# coding: utf-8

# In[53]:


'''
hadm_id offset = 5814
icustay_id offset = 9416
'''
import pandas as pd
import numpy as np
import csv
import datetime
import random

df = pd.read_csv('ICUSTAYS.csv')

def generate_date(start, end):
    '''
    Input:
        start - year in int
        end - year in int
    Output:
        date - Date in specified range in str
    '''
    start_date = datetime.date(start, 1, 1)
    end_date = datetime.date(end, 1, 1)

    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    random_date_str = str(random_date)
    random_date_str = random_date_str.split('-')

    random_date_str = random_date_str[1] + "-" + random_date_str[2] + "-" + random_date_str[0]

    return (random_date, random_date_str)

rand_id = []
for i in range(1,20):
    rand_id.append(random.randrange(1,2500))

headers = ['subject_id', 'hadm_id', 'icu_stay_id', 'dbsource', 'first_careunit', 'last_careunit','firstwardid','lastwardid']
with open('icustays_data.csv', 'w', encoding='UTF8') as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    
    wardid_ls = list(df['first_wardid'].unique())
    first_wardid = wardid_ls
    dbsource = list(df['dbsource'].unique())
    firstcareunit = list(df['first_careunit'].unique())
    hadm_offset = 5814 
    icustay_offset = 9416

    for i in range(1,2501):
        data = []

        data.append(i)

        data.append(i+hadm_offset)
        data.append(i+icustay_offset)

        db_source = random.choice(dbsource)
        data.append(db_source)
    
        if i in rand_id :
            First_careunit = random.choice(firstcareunit)
            data.append(First_careunit)
            Second_careunit = random.choice(firstcareunit)
            data.append(Second_careunit)
            fwid = random.choice(first_wardid)
            data.append(fwid)
            swid = random.choice(first_wardid)
            data.append(swid)
    
        else:
            First_careunit = random.choice(firstcareunit)
            data.append(First_careunit)
            data.append(First_careunit)
            fwid = random.choice(first_wardid)
            data.append(fwid)
            data.append(fwid)
    
        
        writer.writerow(data)

# Getting intime and outime from admits database

df2 = pd.read_csv('icustays_data.csv')
df3 = pd.read_csv('admissions_data.csv')

final_df = df2.merge(df3, left_on='hadm_id', right_on='hadm_id')

final_df2 = final_df.drop(['admission_type','admission_location','discharge_location','insurance','religion','subject_id_y','marital_status','ethnicity','diagnosis','language','Unnamed: 0'],axis=1)
final_df2.rename(columns={"admit_date": "intime", "discharge_date": "outtime","subject_id_x":"subject_id"},inplace=True)
final_df2.to_csv('icustays_clean.csv',index=False)