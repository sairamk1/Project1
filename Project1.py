#!/usr/bin/env python
# coding: utf-8

# In[195]:


#1. Get the Metadata from the above files.
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
df = pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')
df.head(2)
print (df.info())


# In[196]:


#1. Get the Metadata from the above files.
df1 = pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv')
df1.head(2)
print (df1.info())


# In[197]:


#2. Get the row names from the above files.
df.index.values


# In[198]:


#2. Get the row names from the above files.

df1.index.values


# In[199]:


#3. Change the column name from any of the above file.
#4. Change the column name from any of the above file and store the changes made permanently.
df.rename(columns={'Indicator': 'Indicator_Id'}, inplace=True)
print(df)


# In[200]:


#5. Change the names of multiple columns.
df.rename(columns={'Indicator': 'Indicator_Id','PUBLISH STATES': 'Publication Status','WHO region': 'WHO Region'}, inplace=True)
print(df)


# In[202]:


#6. Arrange values of a particular column in ascending order.
df.sort_values('Year', inplace=True, ascending=True)
df


# In[201]:


#7. Arrange multiple column values in ascending order.
df1 = df.loc[0:2, ['Indicator_Id','Country','Year','WHO Region','Publication Status']]
df1


# In[194]:


#8. Make country as the first column of the dataframe.
df.reindex(columns=['Country']+df.columns[:-1].tolist())


# In[203]:


#9. Get the column array using a variable
ar1 = df.columns('WHO Region')
np.char.multiply(ar1, 2)


# In[149]:


#10. Get the subset rows 11, 24, 37
df.iloc[[11,24,37]]


# In[153]:


#11. Get the subset rows excluding 5, 12, 23, and 56
df.drop(df.index[4,11,22,56])


# In[165]:


#Load datasets from CSV
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
users = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
sessions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
products = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
transactions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')
users.head()
sessions.head()
transactions.head()


# In[158]:


#12. Join users to transactions, keeping all rows from transactions and only matching rows from users (left join)
transactions.merge(users, how='left', on='UserID')


# In[159]:


#13. Which transactions have a UserID not in users?
transactions[~transactions['UserID'].isin(users['UserID'])]


# In[160]:


#14. Join users to transactions, keeping only rows from transactions and users that match via UserID (inner join)
transactions.merge(users, how='inner', on='UserID')


# In[162]:


#15. Join users to transactions, displaying all matching rows AND all non-matching rows (full outer join)
transactions.merge(users, how='outer', on='UserID')


# In[163]:


#16. Determine which sessions occurred on the same day each user registered
pd.merge(left=users, right=sessions, how='inner', left_on=['UserID', 'Registered'], right_on=['UserID', 'SessionDate'])


# In[166]:


#17. Build a dataset with every possible (UserID, ProductID) pair (cross join)
df3 = pd.DataFrame({'key': np.repeat(1, users.shape[0]), 'UserID': users.UserID})
df4 = pd.DataFrame({'key': np.repeat(1, products.shape[0]), 'ProductID': products.ProductID})
pd.merge(df3, df4,on='key')[['UserID', 'ProductID']]


# In[167]:


#18. Determine how much quantity of each product was purchased by each user
df5 = pd.DataFrame({'key': np.repeat(1, users.shape[0]), 'UserID': users.UserID})
df6 = pd.DataFrame({'key': np.repeat(1, products.shape[0]), 'ProductID': products.ProductID})
user_products = pd.merge(df5, df6,on='key')[['UserID', 'ProductID']]
pd.merge(user_products, transactions, how='left', on=['UserID', 'ProductID']).groupby(['UserID', 'ProductID']).apply(lambda x: pd.Series(dict(
    Quantity=x.Quantity.sum()
))).reset_index().fillna(0)


# In[168]:


#19. For each user, get each possible pair of pair transactions (TransactionID1,TransacationID2)
pd.merge(transactions, transactions, on='UserID')


# In[169]:


#20. Join each user to his/her first occuring transaction in the transactions table
pd.merge(users, transactions.groupby('UserID').first().reset_index(), how='left', on='UserID')


# In[ ]:




