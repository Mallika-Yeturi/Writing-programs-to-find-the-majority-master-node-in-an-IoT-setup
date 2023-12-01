#import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
#from cassandra.cluster import NoHostAvailable

#AstraDB Client and Secret required for the connection
ASTRA_CLIENT_ID = 'WBMJwjYIFpxeZfjtaTtXvZzj'
ASTRA_SECRET = 'dR1c_+ajXw+1sRK1zA84Sm1C4Wy_aQuplfJ,jkUz-OBIFv02sO-oFin3xRf.n.nSuAccGkPayzdKz+M15.dFLD-8LqjEAkekvOzht28fgS6ycHDzglx3HgztdqZqmNz0'

#Cloud Configuration
cloud_config= {
        'secure_connect_bundle': 'connect_bundle/secure-connect-e-commerce-db.zip'
}

#Authentication Provider
auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_SECRET)

#session object whuch is used fot interacting with Cassandra database
session = None

#Method for creating session object based on cloud configuration and Auth Provider
def create_session():
    global session
    try:
        if session is None:
            cluster = Cluster (cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            print("session is successfully initialised.")
    except Exception as e:
        print("Error in inititalising session: ", str(e))
        return None
    else:
        return session
    #pass

    
#Method for setting keyspace in the session
#Input Parameters:
#    Session object
#    Keyspace_name
def set_session_keyspace(session, keyspace_name):
    try:
        session.execute(f'USE {keyspace_name}')
    except Exception as e:
        print('Error in setting Keyspace in the session', str(e))
    else:
        print(f"{keyspace_name} keyspace is set in the session")

    #pass

#Method for executing query using the given session object
#Input Parameters:
#    Session object
#    query for execution        
def execute_query(session, query):
    try:
        return session.execute(query)
    except Exception as e:
        print("Error in the executing the query:", str(e))
    #pass

#Method for executing single row query using the given session object
#Input Parameters:
#    Session object
#    query for execution         
def execute_single_row_query(session, query):
    try:
        return session.execute(query).one()
        #for row in result:
            #return row[0]
    except Exception as e:
        print("Error in executing single row query:", str(e))

    #pass

#Method for fetching table data using the given session object
#Input Parameters:
#    Session object
#    table name for which the data is to be fetched    
def show_table_data(session, table_name):
    rows = None
    try:
        rows = session.execute(f"SELECT * FROM {table_name}")

    except Exception as e:
        print("Error in showing table data:", str(e))
    else:
        for row in rows:
            print(row)


        
