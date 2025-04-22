from extractor import Extractor
from transformer import Transformer
from loader import Loader

def run_etl_process():
    #Runs the entire process
    print("Starting the ETL process...")
    
    #First initialize the ETL classes
    extractor = Extractor()
    transformer = Transformer()
    loader = Loader()
    
    try:
        
        #processing reference tables first due to dependencies later.. 
        reference_tables = [
            {"type":"db", "name":"brands"}, 
            {"type":"db", "name":"categories"},
            {"type":"csv", "name": "stores", "path": "data/stores.csv"}
            ]
        
        
        for table_info in reference_tables:
            if table_info["type"] == "db":            
                df = extractor.extract_from_db(table_info["name"])
            else: #csv
                df = extractor.extract_from_csv(table_info["path"])
            
            #add reference data before transformation
            transformer.add_reference_data(df, table_info["name"])
            
            # Transform
            transformed_df = transformer.transform(df, table_info["name"])
            
            # Load
            success = loader.load(transformed_df, table_info["name"])
            
            if success:
                #updating ref data with the newly transformed data
                transformer.add_reference_data(transformed_df, table_info["name"])
            else:
                print(f"Warning: Failed to load {table_info["name"]} data.")
        
        # Process tables with dependencies (first level)
        first_level_tables = [
            {"type": "db", "name":"products" },
            {"type": "csv", "name": "staffs", "path": "data/staffs.csv" },
            {"type": "api", "name": "customers"}
        ] 
        
        for table_info in first_level_tables:
            # Extract based on source
            if table_info["type"] == "db":
                df = extractor.extract_from_db(table_info["name"])
            
            elif table_info["type"] == "csv":
                df = extractor.extract_from_csv(table_info["path"])
            
            else:
                df = extractor.extract_from_api(table_info["name"])
            
            
            # Transform
            transformed_df = transformer.transform(df, table_info["name"])
            # reference data added before being used 
            if table_info["name"] in ["staffs", "products", "customers"]:
                transformer.add_reference_data(transformed_df, table_info["name"])
                
            # Load
            success = loader.load(transformed_df, table_info["name"])
            if not success:
                print(f"Warning: Failed to load {table_info["name"]} data.")
        
        # Process tables with dependencies (second level)
        second_level_tables = [
            {"type": "db", "name": "stocks"},
            {"type": "api" , "name": "orders" },
            {"type": "api", "name": "order_items"}
        ] 
        
        for table_info in second_level_tables:
            # Extract based on source
            if table_info["type"] == "db":
                df = extractor.extract_from_db(table_info["name"])
            
            elif table_info["type"] == "csv":
                df = extractor.extract_from_csv(table_info["path"])
            
            else:
                df = extractor.extract_from_api(table_info["name"])
            
            
            # Transform
            transformed_df = transformer.transform(df, table_info["name"])
            
            # Load
            success = loader.load(transformed_df, table_info["name"])
            if not success:
                print(f"Warning: Failed to load {table_info["name"]} data.")

    finally:
        # Clean up connections
        extractor.close_connections()
        loader.close_connection()
    
    print("ETL PROCESS COMPLETED!")

if __name__ == "__main__":
    run_etl_process()
