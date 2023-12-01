from database import *
from setup import *


if __name__ == "__main__":
    try:

        # Creating session object
        session = create_session()

        # Keyspace to be used for performing various operations
        KEYSPACE = 'e_commerce'

        # Setting Keyspace in the cassandra session
        set_session_keyspace(session, KEYSPACE)
        # Code for setting session with cassandra database

        # Getting Count of Products in the database
        row = execute_single_row_query(session, "SELECT COUNT(*) As Count FROM Product")
        print("No of Products in database are : ", row.count)

        # Getting Count of Customers in the database
        row = execute_single_row_query(session, "SELECT COUNT(*) As Count FROM Customer")
        print("No of Customer in database are : ", row.count)

        # Getting count of product wise likes by customer
        #rows = execute_query(session, "SELECT liked_product_id, title, COUNT(*) As Likes FROM Product_Liked_By_Customer GROUP BY liked_product_id")
        rows = execute_query(session, "SELECT customer_id, liked_product_id, COUNT(*) AS likes FROM Product_Liked_By_Customer GROUP BY customer_id, liked_product_id")

        for row in rows:
            print(row)

    except Exception as e:
        print("Error in the execution : ", str(e))