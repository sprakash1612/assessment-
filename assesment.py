import pandas as pd
import sqlite3
from sqlalchemy import create_engine
connector = sqlite3.connect(r'c:\Users\lenovo\Desktop\NGOWebsite\Data Engineer_ETL Assignment.db')
#engine = create_engine(r'c:\Users\lenovo\Desktop\NGOWebsite\Data Engineer_ETL Assignment.db')


sqlConnection = connector.cursor()
tables = sqlConnection.execute('Select name from sqlite_master where type = "table"')
print(tables)

data = sqlConnection.execute('Select Sales.customer_id,Customers.age,Items.item_name,sum(Orders.quantity) as quantity from Sales inner join Orders on Orders.sales_id = Sales.sales_id inner join Customers on Customers.customer_id = Sales.customer_id inner join Items on Items.item_id = Orders.item_id where Customers.age between 18 and 35 and quantity > 0 Group by Sales.customer_id,Items.item_id')

mainDF = pd.DataFrame(data,columns=['Customer','Age','Item','Quantity'])

#mainDF = mainDF[mainDF['quantity']!=0]
mainDF.to_csv('sqlData.csv',index=False,sep=';')

'''
        name   seq
0      items     3
1  customers   100
2      sales   500
3     orders  1500
'''
items = pd.read_sql_query(f'Select * from items',con=connector)
customers = pd.read_sql_query(f'Select * from customers',con=connector)
sales = pd.read_sql_query(f'Select * from sales',con=connector)
orders = pd.read_sql_query(f'Select * from orders',con=connector)

pandasDF = ((sales.merge(orders,how='inner',on='sales_id')).merge(customers,how='inner',on='customer_id')).merge(items,how='inner',on='item_id')
print(pandasDF)
pandasDFMain = pandasDF

print(pandasDF.columns)

pandasDF = pandasDF[(pandasDF['age'] >= 18) & (pandasDF['age'] <= 35)]

pandasDF = pandasDF.groupby(['customer_id','item_name']).agg({'quantity':'sum'})
pandasDF = pandasDF[(pandasDF['quantity']!=0)]

pandasDF = pandasDFMain.merge(pandasDF,how='right',on='customer_id')
print(pandasDF.columns)
pandasDF = pandasDF[['customer_id','age','item_name','quantity_y']]
pandasDF.rename(columns={
    'customer_id':'Customer',
    'age':'Age',
    'item_name':'Item',
    'quantity_y':'Quantity'
    
},inplace=True)
pandasDF.to_csv('pandasData.csv',sep=";",index=False)
connector.close()