
import quandl
import pandas as pd
import unicodedata
from string import ascii_letters, punctuation
import re
import numpy as np

from sklearn.svm import SVC
from sklearn.metrics import scorer
from sklearn.metrics import accuracy_score


quandl.ApiConfig.api_key='naYkqVsmrfQQCZes6EfT'
df=quandl.get('NSE/TCS')

df['PrevClose']=df.Close.shift()
#df['Open'].str.len
df['Up_Down_Indicator']=np.where((df['PrevClose'] >= df['Close']),'Down','Up')
print(df)


#t_col=df.dtypes.index
t_col=list(df.head(0)) 
t_col_nme = [re.sub("[^0-9a-zA-Z]+","_",word.encode("utf-8")) for word in t_col]

y = np.where(df['Close'].shift(-1) > df['Close'],-1,1)
df['Open-Close'] = df.Open - df.Close
df['High-Low'] = df.High - df.Low

x=df[['Open-Close','High-Low']]
split_percentage = 0.8
split = int(split_percentage*len(df))

x_train = x[:split]
y_train = y[:split]

# Test data set
x_test = x[split:]
y_test = y[split:]
cls = SVC().fit(x_train, y_train)
accuracy_train = accuracy_score(y_train, cls.predict(x_train))
accuracy_test = accuracy_score(y_test, cls.predict(x_test))

print('\nTrain Accuracy:{: .2f}%'.format(accuracy_train*100))
print('Test Accuracy:{: .2f}%'.format(accuracy_test*100))

df['Predicted_Signal'] = cls.predict(x)

df['Return'] = np.log(df.Close.shift(-1) / df.Close)*100
df['Strategy_Return'] = df.Return * df.Predicted_Signal


df.Strategy_Return.iloc[split:].cumsum().plot(figsize=(10,5))
plt.ylabel("Strategy Returns (%)")
plt.show()

print(df)




excelwrite=pd.ExcelWriter('/Users/piyushbijwal/Documents/test.xlsx','xlsxwriter')

df.to_excel(excelwrite, sheet_name='Sheet1')
excelwrite.save()


