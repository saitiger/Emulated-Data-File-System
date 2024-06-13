import sys
from textwrap import indent
import requests
import json
import pandas as pd
import datetime
import mysql.connector
from pymongo import MongoClient
from dateutil.relativedelta import relativedelta

BASE_URL = "https://dsci-project-79-default-rtdb.firebaseio.com/"
NAME_NODE = BASE_URL + "Namenode"
DATA_NODE = BASE_URL + "Datanode"

def filter_options(data, options):
        ans = []
        date_keys = ['dod', 'intime', 'outtime', 'transfertime', 'prescription_date']
        for row in data:
            # print("row-----", row)
            flag = True
            for x, y in options.items():
                # print(x,y)
                if x in date_keys:
                    if x == 'prescription_date':
                        if row[x].split('-')[0] != y:
                            flag = False
                    elif x == 'transfertime':
                        temp = row[x].split(' ') 
                        if temp[0].split('-')[-1] != y:
                            flag = False
                    else:
                        if row[x].split('-')[-1] != y:
                            flag = False
                else:
                    if row[x] != y:
                        flag = False
                        break
            if flag:
                ans.append(row)
        return ans

# Query 1
def queryOne(data, options={}):
        # data = get_data(1)
        result = []
        for x in data:
            if 'gender' in x and x['gender']=="M":
                if 'dob' in x:
                    temp = x['dob'].split('-')[-1]
                    if int (temp) < 2000:
                        result.append(x)

        return filter_options(result,options)

# Query 2
def queryTwo(data, options={}):
        filters = {'admission_location': 'EMERGENCY ROOM ADMIT', 'religion': 'CATHOLIC', 'discharge_location': 'HOME'}
        c = {**filters, **options}
        result = filter_options(data, c)
        return result

# Query 3
def queryThree(data, options={}):
        ans = []
        for row in data:
            intime = datetime.datetime.strptime(row['intime'], '%m-%d-%Y').date()
            outtime = datetime.datetime.strptime(row['outtime'], '%m-%d-%Y').date()
            year_difference = relativedelta(outtime, intime).years

            if year_difference > 2:
                ans.append(row)

        return filter_options(ans,options)

# Query 4
def queryFour(data, options={}):
        filters = {'curr_serv': 'SURG'}
        c = {**filters, **options}
        result = filter_options(data, c)
        return result

# Query 5
def queryFive(data, options={}):
    filters = {'drug': 'Bisacodyl'}
    c = {**filters, **options}
    result = filter_options(data, c)
    return result

# Analytics 1
def analyticsOne(data, data1, options={}):
    ans = 0
    filters = {'gender': 'M'}
    result = filter_options(data, filters)
    # print(result)
    ids = []
    for x in result:
        ids.append(x['subject_id'])
    result = []
    hardcoded_filter = { 'marital_status' : 'SINGLE' }
    c = {**hardcoded_filter, **options}
    data1 = filter_options(data1, c)  
    for x in data1:
        if x['insurance'] == 'Medicare':
            if x['subject_id'] in ids:
                ans+=1       
    return ans

# Analytics 2
def analyticsTwo(data, options={}):
    result = 0
    result1 = 0
    data = filter_options(data, options)
    for x in data:
        temp = x['intime'].split('-')[-1]
        if int(temp) < 2000:
            result+=1
        else:
            result1+=1

    return [result, result1]

# Analytics 3
def analyticsThree(all_data, data, options={}):
    final_ans = {}

    # For Male
    c = {'gender': 'M'}
    result = filter_options(all_data, c)
    idsM = []
    for x in result:
        idsM.append(x['subject_id'])

    # For Female
    c = {'gender': 'F'}
    result = filter_options(all_data, c)
    idsF = []
    for x in result:
        idsF.append(x['subject_id'])


    ans = {}
    data = filter_options(data, options)
    for x in data:
        if x['subject_id'] in idsM:
            temp = x['drug']
            ans[temp] = ans.get(temp, 0) + 1
    if len(ans) == 0:
        return ['No results found!', ""]
    count_M = max(ans.values())
    max_keys_M = [key for key, value in ans.items() if value == max(ans.values())]

    final_ans["M"] = [max_keys_M[0], count_M]

    ans = {}
    data = filter_options(data, options)
    for x in data:
        if x['subject_id'] in idsF:
            temp = x['drug']
            ans[temp] = ans.get(temp, 0) + 1
    count_F = max(ans.values())
    max_keys_F = [key for key, value in ans.items() if value == max(ans.values())]

    final_ans["F"] = [max_keys_F[0], count_F]

    return final_ans
   

query_mapping = {1: 'patients', 2: 'admissions', 3: 'icustays', 4: 'services', 5:'prescriptions'}

def Firebase(query, options={}):

    def get_data(query_number):
        url = NAME_NODE + "/" + query_mapping[query_number] + "_data.json"
        response = requests.get(url)
        locations = json.loads(response.text)
        data = []
        if locations:
            for x,y in locations.items():
                temp = requests.get(y + ".json")
                temp = json.loads(temp.text)
                data.append(temp)
        return data

    
    if query == "1":
        # Query 1
        data = get_data(1)
        ans = {}
        count = 1
        all_data = []
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryOne(x, options)
            })
            count +=1
        ans["all_data"] = queryOne(all_data, options)
        return ans


    elif query == "2":
        # Query 2
        data = get_data(2)
        ans = {}
        all_data = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryTwo(x, options)
            })
            # ans[str(count)] = queryTwo(x,options)
            count +=1
        ans["all_data"] = queryTwo(all_data, options)
        return ans


    elif query == "3":
        # Query 3
        data = get_data(3)
        ans = {}
        count = 1
        all_data = []
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryThree(x, options)
            })
            # ans[str(count)] = queryThree(x,options)
            count +=1
        ans["all_data"] = queryThree(all_data, options)
        return ans

    elif query == "4":
        # Query 4
        data = get_data(4)
        ans = {}
        count = 1
        all_data = []
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryFour(x, options)
            })
            # ans[str(count)] = queryFour(x,options)
            count +=1
        ans["all_data"] = queryFour(all_data, options)
        return ans

    elif query == "5":
        # Query 5
        data = get_data(5)
        ans = {}
        all_data = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryFive(x, options)
            })
            # ans[str(count)] = queryFive(x,options)
            count +=1
        ans["all_data"] = queryFive(all_data, options)
        return ans
    
    elif query == "6":
        # Analytics 1
        data = get_data(1)
        all_data = []
        for x in data:
            all_data.extend(x)
        data1 = get_data(2)
        all_data1 = []
        ans = {}
        ans["partitions"] = []
        for i in range(min(len(data), len(data1))):
            all_data1.extend(data1[i])
            ans["partitions"].append({
                str(i+1): analyticsOne(all_data, data1[i], options)
            })
        ans["all_data"] = analyticsOne(all_data, all_data1, options)
        return ans

    elif query == "7":
        # Analytics 2
        data = get_data(3)
        ans = {}
        all_data = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): analyticsTwo(x,options)
            })
            # ans[str(count)] = analyticsTwo(x,options)
            count +=1
        ans["all_data"] = analyticsTwo(all_data, options)
        return ans

    elif query == "8":
        # Analytics 3
        data = get_data(1)
        all_data = []
        for x in data:
            all_data.extend(x)

        data = get_data(5)
        ans = {}
        all_data1 = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data1.extend(x)
            ans["partitions"].append({
                str(count): analyticsThree(all_data, x, options)
            })
            # ans[str(count)] = analyticsThree(all_data, x, options)
            count +=1
        ans["all_data"] = analyticsThree(all_data, all_data1, options)
        return ans

    
def MySQL(query, options={}):
    
    cnx = mysql.connector.connect(host='dsci-project.ceznqfofymma.us-west-1.rds.amazonaws.com', port='3306', user='root', password='root1234', database='DSCI')

    
    # cnx = mysql.connector.connect(user='root', password='Dsci-551',
    #                           host='127.0.0.1',
    #                           database='DSCI_project')
    
    def get_data(query_number):
        cursor = cnx.cursor()
        query = ("SELECT data FROM `Datanode` WHERE filename=%s")
        val = [query_mapping[query_number]+'_data']
        cursor.execute(query, val)
        res = []
        rows = cursor.fetchall()
        for x in rows:
            res.append(json.loads(x[0]))
        return res

    if query == "1":
        # Query 1
        data = get_data(1)
        ans = {}
        count = 1
        all_data = []
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryOne(x, options)
            })
            count +=1
        ans["all_data"] = queryOne(all_data, options)
        return ans


    elif query == "2":
        # Query 2
        data = get_data(2)
        ans = {}
        all_data = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryTwo(x, options)
            })
            # ans[str(count)] = queryTwo(x,options)
            count +=1
        ans["all_data"] = queryTwo(all_data, options)
        return ans


    elif query == "3":
        # Query 3
        data = get_data(3)
        ans = {}
        count = 1
        all_data = []
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryThree(x, options)
            })
            # ans[str(count)] = queryThree(x,options)
            count +=1
        ans["all_data"] = queryThree(all_data, options)
        return ans

    elif query == "4":
        # Query 4
        data = get_data(4)
        ans = {}
        count = 1
        all_data = []
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryFour(x, options)
            })
            # ans[str(count)] = queryFour(x,options)
            count +=1
        ans["all_data"] = queryFour(all_data, options)
        return ans

    elif query == "5":
        # Query 5
        data = get_data(5)
        ans = {}
        all_data = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryFive(x, options)
            })
            # ans[str(count)] = queryFive(x,options)
            count +=1
        ans["all_data"] = queryFive(all_data, options)
        return ans
    
    elif query == "6":
        # Analytics 1
        data = get_data(1)
        all_data = []
        for x in data:
            all_data.extend(x)
        data1 = get_data(2)
        all_data1 = []
        ans = {}
        ans["partitions"] = []
        for i in range(min(len(data), len(data1))):
            all_data1.extend(data1[i])
            ans["partitions"].append({
                str(i+1): analyticsOne(all_data, data1[i], options)
            })
        ans["all_data"] = analyticsOne(all_data, all_data1, options)
        return ans

    elif query == "7":
        # Analytics 2
        data = get_data(3)
        ans = {}
        all_data = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): analyticsTwo(x,options)
            })
            # ans[str(count)] = analyticsTwo(x,options)
            count +=1
        ans["all_data"] = analyticsTwo(all_data, options)
        return ans

    elif query == "8":
        # Analytics 3
        data = get_data(1)
        all_data = []
        for x in data:
            all_data.extend(x)

        data = get_data(5)
        ans = {}
        all_data1 = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data1.extend(x)
            ans["partitions"].append({
                str(count): analyticsThree(all_data, x, options)
            })
            # ans[str(count)] = analyticsThree(all_data, x, options)
            count +=1
        ans["all_data"] = analyticsThree(all_data, all_data1, options)
        return ans


def MongoDB(query, options={}):

    client = MongoClient("mongodb+srv://root:root1234@dsci-project.d65u3ld.mongodb.net/?retryWrites=true&w=majority")
    db = client["dsci"]
    NameNode = db["NameNode"]
    DataNode = db["DataNode"]

    def get_data(query_number):
        v = query_mapping[query_number]+'_data'
        loc = NameNode[v]
        loc1 = DataNode
        ans = []
        for i in loc.find():
            for x in i:
                if x!= '_id':
                    r = loc1[x]
                    t = list(r.find({}))
                    for item in t:
                        if v in item:
                            for x,y in item.items():
                                if x!= '_id':
                                    ans.append(y)

        return ans
                       
    if query == "1":
        # Query 1
        data = get_data(1)
        ans = {}
        count = 1
        all_data = []
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryOne(x, options)
            })
            count +=1
        ans["all_data"] = queryOne(all_data, options)
        return ans


    elif query == "2":
        # Query 2
        data = get_data(2)
        ans = {}
        all_data = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryTwo(x, options)
            })
            # ans[str(count)] = queryTwo(x,options)
            count +=1
        ans["all_data"] = queryTwo(all_data, options)
        return ans


    elif query == "3":
        # Query 3
        data = get_data(3)
        ans = {}
        count = 1
        all_data = []
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryThree(x, options)
            })
            # ans[str(count)] = queryThree(x,options)
            count +=1
        ans["all_data"] = queryThree(all_data, options)
        return ans

    elif query == "4":
        # Query 4
        data = get_data(4)
        ans = {}
        count = 1
        all_data = []
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryFour(x, options)
            })
            # ans[str(count)] = queryFour(x,options)
            count +=1
        ans["all_data"] = queryFour(all_data, options)
        return ans

    elif query == "5":
        # Query 5
        data = get_data(5)
        ans = {}
        all_data = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): queryFive(x, options)
            })
            # ans[str(count)] = queryFive(x,options)
            count +=1
        ans["all_data"] = queryFive(all_data, options)
        return ans
    
    elif query == "6":
        # Analytics 1
        data = get_data(1)
        all_data = []
        for x in data:
            all_data.extend(x)
        data1 = get_data(2)
        all_data1 = []
        ans = {}
        ans["partitions"] = []
        for i in range(min(len(data), len(data1))):
            all_data1.extend(data1[i])
            ans["partitions"].append({
                str(i+1): analyticsOne(all_data, data1[i], options)
            })
        ans["all_data"] = analyticsOne(all_data, all_data1, options)
        return ans

    elif query == "7":
        # Analytics 2
        data = get_data(3)
        ans = {}
        all_data = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data.extend(x)
            ans["partitions"].append({
                str(count): analyticsTwo(x,options)
            })
            # ans[str(count)] = analyticsTwo(x,options)
            count +=1
        ans["all_data"] = analyticsTwo(all_data, options)
        return ans

    elif query == "8":
        # Analytics 3
        data = get_data(1)
        all_data = []
        for x in data:
            all_data.extend(x)

        data = get_data(5)
        ans = {}
        all_data1 = []
        count = 1
        ans["partitions"] = []
        for x in data:
            all_data1.extend(x)
            ans["partitions"].append({
                str(count): analyticsThree(all_data, x, options)
            })
            # ans[str(count)] = analyticsThree(all_data, x, options)
            count +=1
        ans["all_data"] = analyticsThree(all_data, all_data1, options)
        return ans



if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Enter correct arguments")
        exit(1)
    else:
        server = sys.argv[1]
        query = sys.argv[2]
        # options = sys.argv[3]
        options = {}

    if server == "1":
        print(Firebase(str(query), options) )
    elif server == "2":
        print(MySQL(str(query), options))
    elif server == "3":
        print(MongoDB(str(query), options))
