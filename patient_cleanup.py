'''
DSCI-551 PROJECT

ASSUMPTIONS:
1. The patient can be alive according to this script, our demo data has all patients dead.
'''

import csv  
import random
import datetime

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


header = ['subject_id', 'gender', 'dob', 'dod', 'dod_hosp', 'dod_ssn', 'expire_flag']

with open('patients1.csv', 'w', encoding='UTF8') as f:
    writer = csv.writer(f)
    writer.writerow(header)

    gender_selection = ["M", "F"]
    alive_selection = [True , False]
    dod_hosp_selection = [True , False]
    dod_ssn_selection = [True , False]
    
    for i in range(1,2501):
        data = []

        # row id
        data.append(i)

        # gender
        gender = random.choice(gender_selection)
        data.append(gender)

        # dob
        dob = generate_date(1950, 2001)
        data.append(dob[1])

        # dod
        alive = random.choice(alive_selection)
        if not alive:
            dod = generate_date(2010, 2021)
            data.append(dod[1])
            expire = 1

            # dod_hosp
            dod_hosp = random.choice(dod_hosp_selection)
            if dod_hosp:
                data.append(dod[1])
            else:
                data.append("None")
            
            # dod_ssn
            dod_ssn = random.choice(dod_ssn_selection)
            if dod_ssn:
                data.append(dod[1])
            else:
                data.append("None")

        else:
            data.append("None")
            data.append("None")
            data.append("None")
            expire = 0
    
        data.append(expire)

        writer.writerow(data)
