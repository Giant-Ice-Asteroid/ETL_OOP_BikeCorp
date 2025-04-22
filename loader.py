import mysql.connector
import pandas as pd
import json

class Loader:
    
    """
    Class for loading the transformed data into the intended target database (here= BikeCorpDB)
    
    """
    
    def __init__(self, target_db="BikeCorpDB"):
        
        """
        Initialises the Loader with the target DB and conneciton
        
        Arguments: 
            target_db: Name of the target database
            
        """
        
        self.target_db = target_db
        self.connection = None 
        
    def connect_to_db(self):
    # method for the actual connection to target db
        
        with open("cred_info.json") as f:
            content = f.read()
            json_content = json.loads(content)
            self.connection = mysql.connector.connect(
                host = json_content["host"],
                user = json_content["user"],
                password = json_content["password"],
                database = self.target_db
            )
        return self.connection
    
    def load(self, df, table_name):
        """
        Method that handles loading of a dataframe into a database table
        
        Arguments:
                df: pandas DataFrame to be loaded
                table_name: Name of table for the df to be loaded into
                
        Returns:
                Bool - True if loading was successful, False otherwise
        
        """
        
        if df.empty:
            print(f"Attention: Empty dataframe inserted for {table_name} -> Nothing to load!!")
            return False
        
        try:
            #connect to the db if not alreayd conencted
            if self.connection is None or not self.connection.is_connected():
                self.connect_to_db()
                

            cursor = self.connection.cursor()
            
            # create list of column names from the current df
            columns = list(df.columns)
            
            #create placeholders for the SQL insert statements
            placeholders =", ".join(["%s" for _ in columns])
            
            #creates a string of column names for SQL insert statemment
            column_names = ", ".join(columns)
            
            # the SQL INSERT statement:
            insert_query =f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            
            #as previous week, have to disable foreign key check temporarily to load without regard to order
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            
            #next, converting the DataFrame into a list of tuples for SQL insertion
            # NULL values handled by converting NaN to None
            df_values = df.astype(object).where(pd.notnull(df), None)
            values = [tuple(x) for x in df_values.to_numpy()]
            
            # the INSERT query is then executed for multiple rows
            cursor.executemany(insert_query, values)
            
            #commits
            self.connection.commit()
            
            #Turning foregin key chekc back on
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            
            print(f"Successfully loaded {len(df)} rows of records into {table_name} table!\n")
            return True
            
        except mysql.connector.Error as e:
            print(f"Error when attempting to load data into {table_name} table: {e}")
            if self.connection:
                self.connection.rollback()
            return False
        
    def close_connection(self):
        #closes database connection down
        if self.connection is not None and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed")