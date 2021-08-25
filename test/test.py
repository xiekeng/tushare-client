import re

str = '豆一\t\t1101\t\t0\t\t0\t\t0\t\t3,985\t\t3,985\t\t3,985\t\t0\t\t0\t\t0\t\t27\t\t0\t\t0\t\t'
print(str)

p1 = re.compile("\t+")

str1 = p1.sub("\t", str)
print(str1)