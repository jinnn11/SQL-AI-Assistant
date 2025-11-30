import psycopg2
import psycopg2.extras 
import os
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
print(DATABASE_URL)
DATA_FILENAME = 'data.csv' 

def create_connection():
    """ Creates a connection to the Cloud Postgres DB """
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"Error connecting to database: {e}")
    return conn

def create_table(conn, create_table_sql, drop_table_name=None):
    """ Creates a table, optionally dropping it first with CASCADE """
    try:
        cur = conn.cursor()
        if drop_table_name:
            cur.execute(f"DROP TABLE IF EXISTS {drop_table_name} CASCADE;")
        
        cur.execute(create_table_sql)
        conn.commit() 
        cur.close()
        print(f"Table '{drop_table_name}' created successfully.")
    except Exception as e:
        print(f"Error creating table {drop_table_name}: {e}")


def step1_create_region_table(data_filename, conn):
    print("--- Step 1: Region ---")
    regions = set()
    with open(data_filename, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            line = line.strip()
            if not line: continue
            cols = line.split("\t")
            if len(cols) > 4:
                region = cols[4].strip()
                if region:
                    regions.add(region)
    
    sorted_regions = sorted(regions)
    region_rows = [(idx+1, region) for idx, region in enumerate(sorted_regions)]

    create_region_sql = '''
    CREATE TABLE IF NOT EXISTS Region (
      RegionID SERIAL PRIMARY KEY, 
      Region TEXT NOT NULL
    );
    '''

    create_table(conn, create_region_sql, drop_table_name="Region")

    insert_sql = "INSERT INTO Region (RegionID, Region) VALUES (%s, %s)"

    cur = conn.cursor()
    cur.executemany(insert_sql, region_rows)
    conn.commit()
    cur.close()

def step2_get_region_dict(conn):
    cur = conn.cursor()
    cur.execute("SELECT RegionID, Region FROM Region")
    rows = cur.fetchall()
    cur.close()
    return {region: region_id for region_id, region in rows}

def step3_create_country_table(data_filename, conn):
    print("--- Step 3: Country ---")
    region_to_regionid = step2_get_region_dict(conn)
    countries = {}

    with open(data_filename, 'r', encoding='utf-8') as f:
        header = f.readline()
        for line in f:
            line = line.strip()
            if not line: continue 
            cols = line.split("\t")
            if len(cols) < 5: continue 
            country = cols[3].strip()
            region = cols[4].strip()

            if not country or not region: continue
            
            region_id = region_to_regionid.get(region)
            if region_id is None: continue 
            
            countries[country] = region_id 

    sorted_countries = sorted(countries.items(), key=lambda x: x[0])
    country_rows = [(idx+1, country, region_id) for idx, (country, region_id) in enumerate(sorted_countries)]

    create_country_sql = """
    CREATE TABLE IF NOT EXISTS Country(
      CountryID SERIAL PRIMARY KEY,
      Country TEXT NOT NULL,
      RegionID INTEGER NOT NULL,
      FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
    );
    """

    create_table(conn, create_country_sql, drop_table_name="Country")

    insert_sql = "INSERT INTO Country (CountryID, Country, RegionID) VALUES (%s, %s, %s)"

    cur = conn.cursor()
    cur.executemany(insert_sql, country_rows)
    conn.commit()
    cur.close()

def step4_get_country_dict(conn):
    cur = conn.cursor()
    cur.execute("SELECT CountryID, Country FROM Country")
    rows = cur.fetchall()
    cur.close()
    return {country: country_id for country_id, country in rows}

def step5_create_customer_table(data_filename, conn):
    print("--- Step 5: Customer ---")
    country_to_countryid = step4_get_country_dict(conn)
    customer_rows_no_id = []

    with open(data_filename, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            line = line.strip()
            if not line: continue 
            cols = line.split("\t")
            if len(cols) < 4: continue 
            
            name = cols[0].strip()
            address = cols[1].strip()
            city = cols[2].strip()
            country = cols[3].strip()

            if " " in name:
                first_name, last_name = name.split(" ", 1)
            else:
                first_name = name 
                last_name = ""
            
            country_id = country_to_countryid.get(country)
            if country_id is None: continue 
            
            customer_rows_no_id.append((first_name, last_name, address, city, country_id))
    
    customer_rows_no_id.sort(key=lambda x: (x[0], x[1]))
    customers = []
    for idx, (first_name, last_name, address, city, country_id) in enumerate(customer_rows_no_id, start=1):
        customers.append((idx, first_name, last_name, address, city, country_id))

    create_customer_sql = """
    CREATE TABLE IF NOT EXISTS Customer(
      CustomerID SERIAL PRIMARY KEY,
      FirstName TEXT NOT NULL,
      LastName TEXT NOT NULL,
      Address TEXT NOT NULL,
      City TEXT NOT NULL,
      CountryID INTEGER NOT NULL,
      FOREIGN KEY (CountryID) REFERENCES Country(CountryID)
    )
    """

    create_table(conn, create_customer_sql, drop_table_name="Customer")

    insert_sql = """
    INSERT INTO Customer (CustomerID, FirstName, LastName, Address, City, CountryID)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    cur = conn.cursor()
    cur.executemany(insert_sql, customers)
    conn.commit()
    cur.close()

def step6_get_customer_dict(conn):
    cur = conn.cursor()
    cur.execute("SELECT CustomerID, FirstName, LastName FROM Customer")
    rows = cur.fetchall()
    cur.close()
    return {f"{first_name} {last_name}": customer_id for customer_id, first_name, last_name in rows}

def step7_create_productcategory_table(data_filename, conn):
    print("--- Step 7: ProductCategory ---")
    category_to_desription = {}

    with open(data_filename, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            line = line.strip()
            if not line: continue
            cols = line.split("\t")
            if len(cols) <= 7: continue 
            
            product_category_field = cols[6].strip()
            product_category_desc_field = cols[7].strip()

            if not product_category_field or not product_category_desc_field: continue 

            categories = [c.strip() for c in product_category_field.split(";")]
            descriptions = [d.strip() for d in product_category_desc_field.split(";")]

            for cat, desc in zip(categories, descriptions):
                if not cat: continue
                if cat not in category_to_desription:
                    category_to_desription[cat] = desc 
    
    sorted_categories = sorted(category_to_desription.items(), key=lambda x: x[0])
    productcategory_rows = [(idx+1, cat, category_to_desription[cat]) for idx, (cat, _) in enumerate(sorted_categories)]

    create_productcategory_sql = """
    CREATE TABLE IF NOT EXISTS ProductCategory (
      ProductCategoryID SERIAL PRIMARY KEY,
      ProductCategory TEXT NOT NULL,
      ProductCategoryDescription TEXT NOT NULL 
    )
    """

    create_table(conn, create_productcategory_sql, drop_table_name="ProductCategory")

    insert_sql = "INSERT INTO ProductCategory (ProductCategoryID, ProductCategory, ProductCategoryDescription) VALUES (%s, %s, %s)"

    cur = conn.cursor()
    cur.executemany(insert_sql, productcategory_rows)
    conn.commit()
    cur.close()

def step8_get_category_dict(conn):
    cur = conn.cursor()
    cur.execute("SELECT ProductCategoryID, ProductCategory FROM ProductCategory")
    rows = cur.fetchall()
    cur.close()
    return {product_category: product_category_id for product_category_id, product_category in rows}

def step9_create_product_table(data_filename, conn):
    print("--- Step 9: Product ---")
    productcategory_to_productcategoryid = step8_get_category_dict(conn)
    products = {}

    with open(data_filename, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            line = line.strip()
            if not line: continue 
            cols = line.split("\t")
            if len(cols) <=8: continue 
            
            product_name_field = cols[5].strip()
            product_category_field = cols[6].strip()
            product_unitprice_field = cols[8].strip()

            if not product_name_field or not product_category_field or not product_unitprice_field: continue
            
            names = [n.strip() for n in product_name_field.split(";")]
            categories =[c.strip() for c in product_category_field.split(";")]
            prices = [p.strip() for p in product_unitprice_field.split(";")]

            for name, category, price_str in zip(names, categories, prices):
                if not name: continue
                try: unit_price = float(price_str)
                except ValueError: continue
                
                category_id = productcategory_to_productcategoryid.get(category)
                if category_id is None: continue 
                
                if name not in products:
                    products[name] = (unit_price, category_id)
    
    sorted_products = sorted(products.items(), key=lambda x: x[0])
    product_rows = []
    for idx, (name, (unit_price, category_id)) in enumerate(sorted_products, start=1):
        product_rows.append((idx, name, unit_price, category_id))
    
    create_product_sql = """
    CREATE TABLE IF NOT EXISTS Product (
      ProductID SERIAL PRIMARY KEY,
      ProductName TEXT NOT NULL,
      ProductUnitPrice REAL NOT NULL,
      ProductCategoryID INTEGER NOT NULL,
      FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
    );
    """

    create_table(conn, create_product_sql, drop_table_name="Product")
    
    insert_sql = "INSERT INTO Product (ProductID, ProductName, ProductUnitPrice, ProductCategoryID) VALUES (%s, %s, %s, %s)"

    cur = conn.cursor()
    cur.executemany(insert_sql, product_rows)
    conn.commit()
    cur.close()

def step10_get_product_dict(conn):
    cur = conn.cursor()
    cur.execute("SELECT ProductID, ProductName FROM Product")
    rows = cur.fetchall()
    cur.close()
    return {product_name: product_id for product_id, product_name in rows}


def step11_create_orderdetail_table(data_filename, conn):
    print("--- Step 11: OrderDetail (Optimized for Cloud) ---")
    import datetime 
    customer_to_customerid = step6_get_customer_dict(conn)
    product_to_productid = step10_get_product_dict(conn)

    order_rows = []
    order_id = 1 

    print("Parsing CSV file...")
    with open(data_filename, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            line = line.strip()
            if not line: continue 
            cols = line.split("\t")
            if len(cols) <= 10: continue 
            
            name = cols[0].strip()
            product_name_field = cols[5].strip()
            quantity_field = cols[9].strip()
            orderdate_field = cols[10].strip()

            if not name or not product_name_field or not quantity_field or not orderdate_field: continue

            customer_id = customer_to_customerid.get(name)
            if customer_id is None: continue 
            
            product_names = [p.strip() for p in product_name_field.split(";")]
            quantities = [q.strip() for q in quantity_field.split(";")]
            orderdates = [d.strip() for d in orderdate_field.split(";")]

            for prod_name, qty_str, date_str in zip(product_names, quantities, orderdates):
                if not prod_name: continue
                
                product_id = product_to_productid.get(prod_name)
                if product_id is None: continue 
                try: quantity = int(qty_str)
                except ValueError: continue
                try:
                    order_date = datetime.datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
                except ValueError: continue
                
                order_rows.append((order_id, customer_id, product_id, order_date, quantity))
                order_id += 1 
    
    total_rows = len(order_rows)
    print(f"Data parsed. Total rows to insert: {total_rows}")

    create_orderdetail_sql = """
    CREATE TABLE IF NOT EXISTS OrderDetail (
      OrderID SERIAL PRIMARY KEY,
      CustomerID INTEGER NOT NULL,
      ProductID INTEGER NOT NULL,
      OrderDate DATE NOT NULL,
      QuantityOrdered INTEGER NOT NULL,
      FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
      FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
    );
    """
    create_table(conn, create_orderdetail_sql, drop_table_name="OrderDetail")

    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO OrderDetail (OrderID, CustomerID, ProductID, OrderDate, QuantityOrdered)
    VALUES %s
    """
    
    print("Starting Fast Insertion...")
    
    try:
        psycopg2.extras.execute_values(
            cur, 
            insert_query, 
            order_rows, 
            template=None, 
            page_size=1000
        )
        conn.commit()
        print(f"Successfully inserted {total_rows} rows.")
    except Exception as e:
        print(f"Error during insertion: {e}")
        
    cur.close()


if __name__ == "__main__":
    # if DATABASE_URL.startswith("postgres://user:password"):
    #     print("ERROR: You must update the DATABASE_URL variable at the top of the file with your Render URL.")
    # else:
    conn = create_connection()
    
    if conn:
        print("Connected to Render Postgres successfully.")
        
        step1_create_region_table(DATA_FILENAME, conn)
        step3_create_country_table(DATA_FILENAME, conn)
        step5_create_customer_table(DATA_FILENAME, conn)
        step7_create_productcategory_table(DATA_FILENAME, conn)
        step9_create_product_table(DATA_FILENAME, conn)
        step11_create_orderdetail_table(DATA_FILENAME, conn)
        
        conn.close()
        print("All tables populated successfully!")
    else:
        print("Failed to connect.")