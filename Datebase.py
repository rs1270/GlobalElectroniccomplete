import mysql.connector
import pandas as pd
from datetime import datetime
def connection():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="6198"
    )
    mycursor = mydb.cursor()
    mycursor.execute("USE GlobalElectronics")
    return mydb, mycursor  
def close(mydb,mycursor):
    mydb.commit()
    mycursor.close()
def convert_date(date_str):
     return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')

def CustomersInsert():
    csv_file = 'Customers_Clean.csv'
    df = pd.read_csv(csv_file)
    
    df['Birthday'] = df['Birthday'].apply(convert_date)
    mydb, mycursor = connection()
    insert_query = """
        INSERT INTO Customers
        (CustomerKey, Gender, `Name`, City, StateCode, State, ZipCode, Country, Continent, Birthday) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for i, row in df.iterrows():
        mycursor.execute(insert_query, tuple(row))
    close(mydb,mycursor)

def ExchangeInsert():
    csv_file = 'Exchange_Rates_Clean.csv'
    df = pd.read_csv(csv_file)
    
    df['Date'] = df['Date'].apply(convert_date)
    mydb, mycursor = connection()
    insert_query = """
        INSERT INTO `Exchange` (
      
    `Date`,                
    CurrencyCode,       
    ExchangeValue

      
    )
    VALUES (%s, %s, %s)
    """

    for i, row in df.iterrows():
            mycursor.execute(insert_query, tuple(row))
    
    close(mydb,mycursor)

def ProductInsert():
    csv_file = 'Products_Clean.csv'
    df = pd.read_csv(csv_file)
    df['Unit Cost USD'] = df['Unit Cost USD'].replace({r'\$': '', r',': '', ' ': ''}, regex=True).astype(float)
    df['Unit Price USD'] = df['Unit Price USD'].replace({r'\$': '', r',': '', ' ': ''}, regex=True).astype(float)
    mydb, mycursor = connection()
    insert_query = """
            INSERT INTO Products
        (
      
        ProductKey,
        ProductName ,
        Brand,
        Color,
        UnitCostUSD,
        UnitPriceUSD,
        SubcategoryKey,
        Subcategory,
        CategoryKey,
        Category 
        )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

    for i, row in df.iterrows():
        mycursor.execute(insert_query, tuple(row))
   
    close(mydb,mycursor)

def SalesInsert():
    df=pd.read_csv('Sales_Clean.csv')
    df['Order Date'] = df['Order Date'].apply(convert_date)
    mydb, mycursor = connection()
    df['Delivery Date'] = pd.to_datetime(df['Delivery Date'].str[:26])


    insert_query = """
        INSERT INTO Sales
        (
            OrderNumber,
            LineItem,
            OrderDate,
            DeliveryDate,
            CustomerKey,
            StoreKey,
            ProductKey,
            Quantity,
            CurrencyCode
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for i, row in df.iterrows():
        mycursor.execute(insert_query, tuple(row))
    close(mydb,mycursor)
    

def  StoreInsert():
    df=pd.read_csv('Stores_Clean.csv')
    df['Open Date'] = df['Open Date'].apply(convert_date)
    mydb, mycursor = connection()
    insert_query = """
        INSERT INTO Stores
        (
        
            
    StoreKey,
    Country,
    State,
    SquareMeters, 
    OpenDate               
    )
        VALUES (%s, %s, %s, %s, %s)
"""

    for i, row in df.iterrows():
        mycursor.execute(insert_query, tuple(row))
    close(mydb,mycursor)

def main():
    CustomersInsert()
    ExchangeInsert()
    StoreInsert()
    ProductInsert()
    SalesInsert()
    
if __name__=="__main__":
    main()