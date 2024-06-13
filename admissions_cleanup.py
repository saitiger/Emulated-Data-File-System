from cmath import isnan
from math import nan
from operator import sub
import pandas as pd
from collections import defaultdict
import random
from patient_cleanup import generate_date
import sys

df = pd.read_csv('ADMISSIONS.csv')

h = defaultdict(lambda: defaultdict(lambda: 0))

# print(len(df), df['admission_type'].unique())
# print(df['admission_location'].unique())
for i,r in df.iterrows():
    h['admission_type'][r['admission_type']] += 1
    h['admission_location'][r['admission_location']] += 1
    h['discharge_location'][r['discharge_location']] += 1
    h['insurance'][r['insurance']] += 1
    if type(r['language'])!=type('str'):
        pass
    else:
        h['language'][r['language']] += 1
    if type(r['religion'])!=type('str'):
        pass
    else:
        h['religion'][r['religion']] += 1
    if type(r['marital_status'])!=type('str'):
        pass
    else:
        h['marital_status'][r['marital_status']] += 1
    h['ethnicity'][r['ethnicity']] += 1
    h['diagnosis'][r['diagnosis']] += 1

for i in h:
    for x in h[i]:
        if type(x)!=type('str'):
            print(i)
# print(h['language'])
# sys.exit(1)
values = []
cols = []
for i in h:
    tmp = []
    for val in h[i]:
        # print(val, i)
        tmp.extend([val]*h[i][val])
    values.append(tmp)
    cols.append(i)
    # break

# print(values, len(values), cols)

patients = pd.read_csv('patients_data.csv')
print(patients.head())
dob = {}
dod = {}
for i,r in patients.iterrows():
    dob[int(r['subject_id'])]=int(r['dob'].split('-')[-1])
    try:
        dod[int(r['subject_id'])]=int(r['dod'].split('-')[-1])
    except: 
        dod[int(r['subject_id'])] = dob[int(r['subject_id'])] + 60

hadm_offset = 5814 
subject_id = 1
# data = [[] for i in range(2500)]
data = []

for _ in range(2500):
    tmp = []
    tmp.append(subject_id)
    tmp.append(subject_id+hadm_offset)
    admit_date = generate_date(dob[subject_id], dod[subject_id])[-1]
    tmp.append(admit_date)
    try:
        discharge_date = generate_date(int(admit_date.split('-')[-1])+1, min(int(admit_date.split('-')[-1])+4, dod[subject_id]))[-1]
        while int(discharge_date.split('-')[-1])>=dod[subject_id]:
            discharge_date = generate_date(int(admit_date.split('-')[-1])+1, min(int(admit_date.split('-')[-1])+4, dod[subject_id]))[-1]
        tmp.append(discharge_date)
    except: 
        tmp.append(admit_date)
    # tmp.append(dob[subject_id])
    # tmp.append(dod[subject_id])
    for ind in range(len(values)):
    # print(ind, values[ind], data[ind])
    # break
        
        tmp.append(random.choice(values[ind]))
    data.append(tmp)
    subject_id += 1

# for i in data:
#     print(len(i), i)
#     break
# print(len(data))
# print(generate_date(2000, 2013))

df = pd.DataFrame(data, columns=['subject_id', 'hadm_id', 'admit_date', 'discharge_date']+cols)
df.to_csv('admissions_data.csv')



