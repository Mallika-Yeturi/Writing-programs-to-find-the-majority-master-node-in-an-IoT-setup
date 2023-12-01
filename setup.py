from database import *
import csv
from datetime import datetime
import main
timestamp_ms = int(datetime.now().timestamp() * 1000)
if __name__ == "__main__":
    try:
        #Creating session object 
        session = create_session()
        
        #Keyspace to be used for performing various operations
        KEYSPACE = 'e_commerce'

        #Setting Keyspace in the cassandra session
        set_session_keyspace(session, KEYSPACE)

        #Code for setting session with cassandra database

        #Creating table 'Customer'
        create_customer_table_query =f"""
        CREATE TABLE IF NOT EXISTS Customer (
            customer_id int,
            first_name text,
            last_name text,
            cr_at timestamp,
            PRIMARY KEY (customer_id)); """

        if execute_query(session, create_customer_table_query) is not None:
            print("Customer table is successfully created.")


        #Creating table 'Product'
        create_product_table_query = f"""
        CREATE TABLE IF NOT EXISTS Product (
            product_id int,
            title text,
            PRIMARY KEY (product_id))"""

        if execute_query(session, create_product_table_query) is not None:
            print("Product table is successfully created.")

        #Creating table 'Product_Liked_By_Customer'
        create_product_liked_by_customer_table_query =f"""
        CREATE TABLE IF NOT EXISTS Product_Liked_By_Customer (
            customer_id int,
            first_name text,
            last_name text,
            liked_product_id int,
            liked_on timestamp,
            title text,
            PRIMARY KEY (customer_id, liked_product_id, liked_on ))"""

        if execute_query(session, create_product_liked_by_customer_table_query) is not None:
            print("Product_Liked_By_Customer table is successfully created.")


        # Inserting Customer data in the table
        try:
            customer_data_insert_query = "INSERT INTO Customer (customer_id, first_name, last_name, cr_at) VALUES (%s, '%s', '%s', %s)"
            with open("config/customers.csv", "r") as file:
                csvreader = csv.reader(file)
                header = next(csvreader)
                for row in csvreader:
                    execute_query(session, customer_data_insert_query % (row[0], row[1], row[2], int(datetime.now().timestamp() * 1000)
                        #row[0],
                        #row[1],
                        #row[2],
                        #int(datetime.now().timestamp() * 1000) #timestamp_ms
                        #int(float(datetime.now().strftime("%s.%f"))) *1000
                    ))
                        #timestamp_ms

            print("Customer data inserted in to the table")
        except Exception as e:
            print("Error in the execution customer_data_insert_query: ", str(e))


        #Inserting Product data in the table
        try:
            product_data_insert_query = "INSERT INTO Product (product_id , title ) VALUES (%s, '%s')"
            with open("config/products.csv", "r") as file:
                csvreader = csv.reader(file)
                header = next(csvreader)
                for row in csvreader:
                    execute_query(session, product_data_insert_query % (
                        row[0],
                        row[1]))
            print("Product data inserted in to the table")
        except Exception as e:
            print("Error in the execution product_data_insert_query: ", str(e))

        #Inserting Product_Liked_By_Customer data in the table
        try:
            product_liked_by_customer_data_insert_query = f"""INSERT INTO Product_Liked_By_Customer(
                customer_id ,
                first_name ,
                last_name ,
                liked_product_id ,
                liked_on ,
                title ) VALUES (%s, '%s', '%s', %s, %s, '%s')"""
            with open("config/product_liked_by_customer.csv", "r") as file:
                csvreader = csv.reader(file)
                header = next(csvreader)
                for row in csvreader:
                    execute_query(session, product_liked_by_customer_data_insert_query % (
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        int(datetime.now().timestamp() * 1000), #timestamp_ms, #int(float(datetime.now().strftime("%s.%f"))) * 1000,
                        row[4]))
            print("Product_Liked_By_Customer data inserted in to the table")
        except Exception as e:
            print("Error in the execution product_liked_by_customer_data_insert_query : ", str(e))
                
    except Exception as e:
        print("Error in the execution : ", str(e))
        