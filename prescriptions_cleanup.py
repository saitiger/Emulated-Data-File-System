from dis import dis
from operator import sub
import pandas as pd
from collections import defaultdict
import random
from patient_cleanup import generate_date
import sys
# import tqdm 
import json
import datetime

df = pd.read_csv('PRESCRIPTIONS.csv')
print(len(df))
h = defaultdict(lambda: defaultdict(lambda: 0))

for i,r in df.iterrows():
    h['subject_id'][r['subject_id']] += 1
    h['drug_type'][str(r['drug_type']) + "---" + 
        str(r['drug']) + "---" + 
        str(r['formulary_drug_cd']) + "---" + 
        str(r['prod_strength']) + "---" + 
        str(r['dose_val_rx']) + "---" +
        str(r['dose_unit_rx']) + "---" +
        str(r['form_unit_disp'])
        ] += 1

# print(json.dumps(h, indent=4))    
# print(len(h['drug_type']))
# print(h['drug_type'].values())

# noOfPrescriptions- 5-140

l = []
for i in h['drug_type']:
    l.extend([i]*h['drug_type'][i])
print(len(l))

# for i in range(1,)
adm = pd.read_csv('admissions_data.csv')
final = []
for i,r in adm.iterrows():
    # if i%500 == 0:
    print(i)
    subject_id = r['subject_id']
    hadm_id = r['hadm_id']
    icustay_id = r['subject_id'] + 9416

    # date processing
    x = r['admit_date'].split('-')
    x = [int(i) for i in x]
    admit_date = datetime.date(x[2], x[0], x[1])
    # print(admit_date)

    x = r['discharge_date'].split('-')
    x = [int(i) for i in x]
    discharge_date = datetime.date(x[2], x[0], x[1])
    
    
    # sys.exit(1)
    t = random.randrange(3,8)
    for _ in range(t):
        tmp = random.choice(l)
        prescription_date = admit_date
        if admit_date != discharge_date:
            prescription_date += datetime.timedelta(days=random.randrange(0,150))
            # print(prescription_date)
        while prescription_date>discharge_date:
            # print('here', admit_date, prescription_date, discharge_date)
            prescription_date = admit_date + datetime.timedelta(days=random.randrange(1,150))
        final.append([subject_id, hadm_id, icustay_id, prescription_date] + tmp.split('---'))
print(len(final), final[:3])

cols = ['drug_type', 'drug', 'formulary_drug_cd', 'prod_strength', 'dose_val_rx', 'dose_unit_rx', 'form_unit_disp']

df = pd.DataFrame(final, columns=['subject_id', 'hadm_id', 'icustay_id', 'prescription_date']+cols)
df.to_csv('prescription_data.csv')
