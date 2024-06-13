import pandas as pd

df = pd.read_csv('admissions_data.csv')

# print(df['language'].unique())
l = []
s = set()
for i in df['discharge_location']:
    # print(i.split('-')[0])
    # l.append(i.split(' ')[0].split('-')[-1])
    s.add(i)
    # l.append(i.split('-')[-1])

# s = set()
# for i,r in df.iterrows():
#     if r['drug']=='Bisacodyl':
#         s.add(r['formulary_drug_cd'])

# print(min(l), max(l))
print([print(i) for i in s])
# print(/)

# [[1,2,3],
# [4,4,6]]
# len(grid), len(grid[0])