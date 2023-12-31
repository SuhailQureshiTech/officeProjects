# -*- coding: utf-8 -*-
"""ALL_LOC_SALE.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1ZbqW933t0NW03mCoFlhSLBZ9In03Ox7S

Importing Libraries
"""

import numpy as np
import pandas as pd
import re

"""Reading File and Stripping it"""

with open("ALL_LOC_SALE.txt") as file_in:
  #Seperate Line by Line
  stripped = [line.strip() for line in file_in]
  #Split each character of a single line
  lines = [line.split("$SPL$") for line in stripped if line]
  print(lines[1])
  #Initializing new list
  F = []
  #Ilterating Through Data and saving it into the list F 
  for i in range(1,len(lines) - 1 ):
    d = [attribute for attribute in lines[i]]
    F.append([d[0],d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8],d[9],d[10],d[11],d[12],d[13],d[14],d[15],d[16]])

"""Saving Into CSV Format"""

df = pd.DataFrame(data = F)
df.reset_index(inplace=False)
df.columns = [lines[0][0],lines[0][1],lines[0][2],lines[0][3],lines[0][4],lines[0][5],lines[0][6],lines[0][7],lines[0][8],lines[0][9],lines[0][10],lines[0][11],lines[0][12],lines[0][13],lines[0][14],lines[0][15],lines[0][16]]
df.to_csv("ALL_LOC_SALE.csv", index=False)

"""New DataFrame"""

df.tail(15)