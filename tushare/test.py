from base import *


d1 = datetime.datetime.strptime('19901219', '%Y%m%d')
d2 = datetime.datetime.strptime('20210619', '%Y%m%d')
delta = d2 - d1
print(delta.days)