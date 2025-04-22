import mysql.connector
import pandas as pd
import os
import json
import requests #used for making HTTP reuqests to the API



class Extractor:
    """
    Class that unifies the handling of the data extraction from multiple sources..:
    - Local CSV files 
    - MySQL database
    - API endpoints
    
    - returns a DataFrame
    """
    
    
    def __init__(self): 
        """ 
        Initialization of the Extractor object
        
    
        """        

        self.connection = None

            
    ######## CSV ###############       
            
    def extract_from_csv(self, file_path):
        """
        Extracts data from CSV files
        
        Arguments:
            file_path = path to the specific CSV file
        
        Return a DataFrame containing the CSV data
        
        """

        try:
            
            print(f"\nExtracting data from {file_path}..")

            # checking if the file exists
            if not os.path.exists(file_path):
                print(f"Error: File {file_path} not found")
                return pd.DataFrame()
            

            #next we read the CSV file into a pandas df
            # pandas should automatically detect headers and data types from the CSV
            df = pd.read_csv(file_path)
            print(f"Extracted {len(df)} rows of data from {file_path}")
            return df
                
        except Exception as e:
                print(f"Sorry, error when attempting to extract data from {file_path}: {e}")
                return pd.DataFrame()
            
        
      
    
    ######### source database ###################
    
    
    def connect_to_productDB(self):
        """
        method which attempts to make a connection to the mySQL server
        mysql.connector.connect() establishes the connection with the credidentials passed in
        returns a connection object
        """

        with open("cred_info.json") as f:
            content = f.read()
            json_content = json.loads(content)
            #connect to the MySQL server 
            self.connection = mysql.connector.connect(
                host = json_content["host"],
                user = json_content["user"],
                password = json_content["password"],
                database= "ProductDB"
                )
        
        return self.connection
    
    def extract_from_db(self, table_name):
        """
        Function which extracts data from a (to be)specified table in the source database (here: ProductDB)
        
        Arguments:
            table_name: Name of the table from which to extract data (e.g brands, staffs, stocks)

        Returns a DataFrame containing the extracte data
        """

        print("\nExtracting data from ProductDB")

        #connect to the source database, ProductDB
        try:
            if self.connection is None or not self.connection.is_connected():
                self.connect_to_productDB()
                
            #creates cursor, here dictionary=true return the results as a dict which is easier to work with 
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {table_name}") # grabs all with *
            results = cursor.fetchall() # fetchall method retrieves all the rows in the result set of a query  
            
            if not results:
                print(f"No data found in {table_name} table...")   
                cursor.close()
                return pd.DataFrame()
        
            df = pd.DataFrame(results) # table data goes into a df
            print(f"Extracted {len(df)} rows of records from {table_name} table")
            
            # closing cursor but keeping connection open for now
            cursor.close()
            return df
            
        # error handling in case connection or extraction fails
        except mysql.connector.Error as e:
            print(f"Oh no, error when attempting to extarct data from {table_name}: {e}")        
            return pd.DataFrame()
        
               
    ######### API ###########
    def extract_from_api(self, endpoint, base_url="http://localhost:8000"):
        """
        Extracts data from endpoints(=data sources available from the API) on a fastAPI server
        
        NB: it requires the API to be running already 
        --> the API server can be started by running "fastapi run run_api.py" in terminal
        
        Arguments:
                endpoint: API endpoint (e.g customers, orders, order_items)
                base_url: base URL address for the API
                
        Returns:
                pandas Dataframe containing the response data from the API
        """
        
        print("\nBeginning process of extracting data from API")

        
        try:
            
            

                
            full_url = f"{base_url}/{endpoint}" #making a varible that contains the full url address for each endpoint
            print(f"Requesting data from {full_url}...")
                            
            
            #requests.get() sends an HTTP GET request to the newly created url
            # the API server receives the request and sends back data
            # the response variable below contains everything the server sends back (data, status codes, headers)
            response = requests.get(full_url)
            response_text = response.text
            
            # checks if the request was successful (=HTTP status code 200)
            if response.status_code == 200:
                
                #can then parse the response (which is in the JSON file format) into a python data structure:
                response_text = json.loads(response_text)
                data = json.loads(response_text)
            
                #then converts it to a pandas df 
                df = pd.DataFrame(data)
                
                return df
            
            else:
                #error handling
                print(f"Error when accessing {endpoint}: Status code {response.status_code}")
                print(f"Response text: {response.text}")
                return pd.DataFrame()

 
        except Exception as e:
                print(f"Error when processing {endpoint}: {e}")
                return pd.DataFrame()

    def close_connections(self):
        """
        closes any open database connections if existing
        """
        if self.connection is not None and self.connection.is_connected():
            self.connection.close()
            print("Connection to Database closed")



    