import openpyxl

wb = openpyxl.load_workbook(filename='data/kba_probe_2023.xlsx')
table = wb['FE1.2']
val = table['C11']
print(val)