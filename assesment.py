import logging.config
import os
import pandas as pd
import sqlite3
import logging
logging.basicConfig(filename='process.log',filemode='a',level=logging.INFO)
try:
    connector = sqlite3.connect(r'c:\Users\lenovo\Desktop\NGOWebsite\Data Engineer_ETL Assignment.db')
    #engine = create_engine(r'c:\Users\lenovo\Desktop\NGOWebsite\Data Engineer_ETL Assignment.db')
    logging.info(f'Connection Established :: {connector}')


    sqlConnection = connector.cursor()
    tables = sqlConnection.execute('Select name from sqlite_master where type = "table"')
    print(tables)
    logging.info(f'Tablee details :: {[table[0] for table in tables]}')
    
    '''
            name   seq
    0      items     3
    1  customers   100
    2      sales   500
    3     orders  1500
    '''

    try:
            
        data = sqlConnection.execute('Select Sales.customer_id,Customers.age,Items.item_name,sum(Orders.quantity) as quantity from Sales inner join Orders on Orders.sales_id = Sales.sales_id inner join Customers on Customers.customer_id = Sales.customer_id inner join Items on Items.item_id = Orders.item_id where Customers.age between 18 and 35 and quantity > 0 Group by Sales.customer_id,Items.item_id')
        logging.info(f'SQL Query Executed')
        mainDF = pd.DataFrame(data,columns=['Customer','Age','Item','Quantity'])

        #mainDF = mainDF[mainDF['quantity']!=0]
        mainDF.to_csv('sqlData.csv',index=False,sep=';')
        logging.info(f'file saved as sqlData.csv at path {os.getcwd()}')
        
    except Exception as err:
        print('Unable to fatch data using SQl Query : ',err)
        logging.error(f'Unable to fatch data using SQl Query : {err}')
    try:
        items = pd.read_sql_query(f'Select * from items',con=connector)
        logging.info(f'Data fatched items :: {items.shape}')
        customers = pd.read_sql_query(f'Select * from customers',con=connector)
        logging.info(f'Data fatched customers :: {customers.shape}')
        sales = pd.read_sql_query(f'Select * from sales',con=connector)
        logging.info(f'Data fatched sales :: {sales.shape}')
        orders = pd.read_sql_query(f'Select * from orders',con=connector)
        logging.info(f'Data fatched orders :: {orders.shape}')

        pandasDF = ((sales.merge(orders,how='inner',on='sales_id')).merge(customers,how='inner',on='customer_id')).merge(items,how='inner',on='item_id')
        print(pandasDF)
        logging.info(f'Merge Completed :: {pandasDF.shape}')
        pandasDFMain = pandasDF

        print(pandasDF.columns)

        pandasDF = pandasDF[(pandasDF['age'] >= 18) & (pandasDF['age'] <= 35)]
        logging.info(f'Age Filter Applied :: {pandasDF.shape}')

        pandasDF = pandasDF.groupby(['customer_id','item_name']).agg({'quantity':'sum'})
        logging.info(f'Group Created :: {pandasDF.shape}')
        pandasDF = pandasDF[(pandasDF['quantity']!=0)]
        logging.info(f'Zero values Removed :: {pandasDF.shape}')

        pandasDF = pandasDFMain.merge(pandasDF,how='right',on='customer_id')
        logging.info(f'Merge Completed with Main DF :: {pandasDF.shape}')
        print(pandasDF.columns)
        pandasDF = pandasDF[['customer_id','age','item_name','quantity_y']]
        logging.info(f'Columns Filtered :: {pandasDF.shape}')
        pandasDF.rename(columns={
            'customer_id':'Customer',
            'age':'Age',
            'item_name':'Item',
            'quantity_y':'Quantity'
            
        },inplace=True)
        logging.info(f'Colun Renemed :: {pandasDF.columns}')
        pandasDF.to_csv('pandasData.csv',sep=";",index=False)
        logging.info(f'CSV File Creaded as pandasData.csv in path {os.getcwd()}')
        connector.close()
        logging.info('Connection Closed')
    except Exception as err:
        print('Unable to perform Panda Operations : ',err)
        logging.error(f'Unable to perform Panda Operations : {err}')
        

except Exception as err:
    print('Unable to connect to SQl DB : ',err)
    logging.error(f'Unable to connect to SQl DB : {err}')
    
