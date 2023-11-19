import openpyxl
from itertools import islice

wb = openpyxl.load_workbook(filename='data/kba_probe_2023.xlsx')
table = wb['FE1.2']
val = table['C11']
print(val)


data = table['C41':'M53']


