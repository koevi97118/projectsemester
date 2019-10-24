import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import matplotlib.pyplot as plt
%matplotlib inline

pd.set_option('display.max_columns', 150)
pd.set_option('display.max_rows', 20)
pd.set_option("display.max_colwidth", 8000)

df1= pd.read_csv('transfused.csv')
df2= pd.read_csv('nontransfused.csv')

df1 = df1.rename(columns={'unabridgedUnitLOS':'unabridgedunitlos','unabridgedHospLOS':'unabridgedhosplos'
                          ,'apacheScore':'apachescore','unitType':'unittype'})
df1['transfusedmarker']=1
df1

dframe = pd.concat([df1,df2],sort=False)
dframe.to_csv('alldata.csv')
