import pandas as pd

class Transformer:
    """
    Class with the purpose of transforming data from different sources
    
    Handles datacleaning, typeconversion and standardisation
    """
    
    def __init__(self):
         # Initialize the Transformer with empty reference data containers
         
        self.reference_data = {
            "brands": None,
            "categories": None,
            "stores": None,
            "staffs": None,
            "products": None,
            "customers": None,
            "orders": None
        }
    def add_reference_data(self, df, table_type):
        """
        Add a reference DataFrame that other transformations might need.
        
        Saves a copy each type of reference data as a df (self.reference_data)
        
        Arguments:
            df: pandas DataFrame containing reference data
            table_type: Type of reference table (brands, categories, etc.)
            
        """
        
        
        if df is not None and not df.empty:
            self.reference_data[table_type] = df.copy()
            print(f"Added {table_type} reference data with {len(df)} records")
            
            
    def transform(self, df, table_type):
        """
        Transforms a DataFrame based on its table type..
        
        Arguements: 
                df: Pandas DataFrame to be transformed
                table_type: specific table type (ie. brands, stocsk, categories ect)
                
        Returns: 
                Transformed dataframe
                
        """
        
        if df.empty:
            print(f"Oops, received an empty Dataframe as arguemnt for {table_type} transformation")
            return df

        print(f"Initialising transformation of {table_type} data")
        
        if table_type == "brands":
            return self._transform_brands(df)
        elif table_type == "categories":
            return self._transform_categories(df)       
        elif table_type == "stores":
            return self._transform_stores(df)
        elif table_type == "staffs":        
            return self._transform_staffs(df)
        elif table_type == "products":
            return self._transform_products(df)
        elif table_type == "stocks":
            return self._transform_stocks(df)    
        elif table_type == "customers":
            return self._transform_customers(df)
        elif table_type == "orders":
            return self._transform_orders(df)
        elif table_type == "order_items":
            return self._transform_order_items(df)
        else:
            print("Attention: Received unknown table type as argument. No transformation - returning original DataFrame")
            return df
        
    
    #BRANDS
    
    def _transform_brands(self, df):
           
        #  dataframe is copied to avoid modifying the original data
        transformed_df = df.copy()

        # first step of tranformation -> data types
        # brand_id type is set to integer,brand_name data type as string 
        transformed_df["brand_id"] = transformed_df["brand_id"].astype(int)
        transformed_df["brand_name"] = transformed_df["brand_name"].astype(str)

        # since data set is small, no need to check for duplicates here
        print(f"Transformed  {len(transformed_df)} reocrds")
        return transformed_df
    
    #CATEGORIES
    
    def _transform_categories(self, df):
        
        transformed_df = df.copy()
    
        # data types: category_id is set as int; category_name as string
        transformed_df['category_id'] = transformed_df['category_id'].astype(int)
        transformed_df['category_name'] = transformed_df['category_name'].astype(str)    

        print(f"Transformed {len(transformed_df)} category records")
        return transformed_df
        
    #STORES
    
    def _transform_stores(self, df):
        
        # copy the DataFrame 
        transformed_df = df.copy()
        
        # first: adding store_id column if it doesn't exist (to be used as primary key)
        if "store_id" not in transformed_df.columns:
            # we do this by creating incrementing IDs starting from 1
            transformed_df["store_id"] = range(1, len(transformed_df) + 1)
            print("Added a new store_id column (primary key)")
        
        # next, loop through each column in the df and check if datatype = object (string in pandas)
        # for those, missing values (na/NaN) is replaced with an empty string (" ") and each column is set as string type
        # doesn't seem super necessary in this set, but doing it for consistency
        for col in transformed_df.columns:
            if transformed_df[col].dtype == "object":
                transformed_df[col] = transformed_df[col].astype(str)    

        # making sure that zip_code can be treated as intergers
        transformed_df["zip_code"] = transformed_df["zip_code"].astype(int)
        print("Converted zip_code to integer")

        # lastly, saving the newly transformed stores data in its target dir
        
        print(f"Transformed {len(transformed_df)} STORES records")
        return transformed_df
        
    #STAFFS
    
    def _transform_staffs(self, df):
        
            
        # again, copy the df
        transformed_df = df.copy()
        
        # starting by renaming  the "name" column to "first_name" for clarity
        if "name" in transformed_df.columns:
            transformed_df = transformed_df.rename(columns={"name": "first_name"})
            print("Renamed 'name' column to 'first_name'")
        
        # then adding a NEW staff_id column if it doesn't exist already (to be used as primary key)
        if "staff_id" not in transformed_df.columns:
            transformed_df["staff_id"] = range(1, len(transformed_df) + 1)
            print("Added a new column: staff_id, designated primary key")
        
        # convert store_name to store_id and map it using the transformed stores df just created (where store_id was added)
        if "store_name" in transformed_df.columns and self.reference_data["stores"] is not None:

            # zip() pairs the two columns in tuples, and then into a dict (e.g {'Santa Cruz Bikes': 1, 'Baldwin Bikes': 2, 'Rowlett Bikes': 3})
            # this dict can then be used to look up ID for store name
            store_name_to_id = dict(zip(self.reference_data["stores"]["name"], self.reference_data["stores"]["store_id"]))

            # then we can create a new "store_id" column by replacing each store name with its corresponding store_id:
            # for each staff member .map takes each store_name and looks up its corresponding store_id in the dict
            # the IDs are then assigned to a new column on the left called store_id
            # lastly we the store_name column is dropped(deleted)
            transformed_df["store_id"] = transformed_df["store_name"].map(store_name_to_id)
            transformed_df = transformed_df.drop(columns=["store_name"])
            print("Converted store_name to store_id in a new store_id column and dropped store_name column")
        
        # next we need to handle manager_id because first row is empty
        if "manager_id" in transformed_df.columns:

            # Convert manager_id to floats in pandas first
            # this is because, in pandas, int columns cant store null/NaN values, but float columns can
            # errors="coerce" tells pandas to convert any values that can't be interpreted as numbers (like stirngs) to NaN rather than raise error
            transformed_df["manager_id"] = pd.to_numeric(transformed_df["manager_id"], errors="coerce")     

            # got to keep manager_id as NaN for top manager and as the rest as integers..
            # to do this, we create a boolean array (=mask) where rows where manager_id is not Nan == True
            # the .loc method allows us to select only specified rows, choosing only the rows where mask is True in the manager_id coloumn
            # these are then converted to integer type (though will prob come out as float in the csv due to pandas shenanigans)
            mask = transformed_df["manager_id"].notna()
            transformed_df.loc[mask, "manager_id"] = transformed_df.loc[mask, "manager_id"].astype(int)
            
            print("Processed manager_id values: kept NaN for top manager, converted others to integers")

        # also ensure that "active" column is an integer type (0 or 1)
        if "active" in transformed_df.columns:
            transformed_df["active"] = transformed_df["active"].astype(int)
            print("Converted values in 'active' column to integers")
        

        # as before standardise remaining columns
        for col in transformed_df.columns:
            if transformed_df[col].dtype == "object":  # string columns
                transformed_df[col] = transformed_df[col].fillna('').astype(str)
        
        # finally, dropping the street column which is redundant
        transformed_df = transformed_df.drop(columns=["street"])

        # then save the transformed staffs data to its dir

        print(f"Transformed {len(transformed_df)} staff records")
        return transformed_df
        
    #PRODUCTS
    
    def _transform_products(self, df):
          
        #copying the df
        transformed_df = df.copy()
        
        # beginning the tranformation by ensuring correct data types for all columsn in products
        
        transformed_df["product_id"] = transformed_df["product_id"].astype(int) #product_id -> int (primary key)
        print("converted product_id to integers")
        transformed_df["product_name"] = transformed_df["product_name"].astype(str) #product_name -> string
        print("Converted product_name to string type")
        transformed_df["brand_id"] = pd.to_numeric(transformed_df["brand_id"], errors="coerce") #brand_id -> num. using pd.to_num which allows handling of NaN
        print("Converted brand_id to numeric")
        transformed_df["category_id"] = pd.to_numeric(transformed_df["category_id"], errors="coerce") #category_id -> numeric (might encounter NaN)
        print("Converted category_id to numeric")
        transformed_df["model_year"] = transformed_df["model_year"].astype(int) #model_year -> int
        print("converted model_year to integers")
        transformed_df["list_price"] = pd.to_numeric(transformed_df["list_price"], errors="coerce") #list_price -> numeric (to be float)
        print("Converted list_price to numeric (float)")
        
        # validating the brand IDs in products by comparing to brands
        # first extracting the brand_id column from the brands df resulting in a list 
        # the list is converted to a set to eliminate potential duplicates and for optimisation
        if self.reference_data["brands"] is not None:
            
            valid_brand_ids = set(self.reference_data["brands"]["brand_id"])
        
            #next a we create a mask for catching invaLid brand IDs:
            # a list of all brand ID in products is checked against the set of valid IDs created above and returns a boolen
            # ~ operator inverts those booleans value, so that True becomes False and vice versa
            # the invalid_brand_mask series of bools now has the value of True for the rows(if nay) that need fixing
            invalid_brand_mask = ~transformed_df["brand_id"].isin(valid_brand_ids)
        
            # here then any() return True if at least one value in the mask is True
            # potentential invalid ID are then counted with .sum (True is 1 and False is 0)
            # where invalid ID are encountered, they're changed to NULL at the affected rows 
            if invalid_brand_mask.any():
                invalid_count = invalid_brand_mask.sum()
                print(f"Attention: Located {invalid_count} products with invalid brand_id values..!")
                transformed_df.loc[invalid_brand_mask, "brand_id"] = None
                print("Invalid brand_id values changed to NULL")
            else:
                print("All good - No invalid brand_id values identified!")
            
            #repeating the procedure for category_id values against category data set..
        if self.reference_data["categories"] is not None:
                
            valid_category_ids = set(self.reference_data["categories"]["category_id"])
            invalid_category_mask = ~transformed_df["category_id"].isin(valid_category_ids)
            
            if invalid_category_mask.any():
                invalid_count = invalid_category_mask.sum()
                print(f"Attention: Located {invalid_count} products with invalid category_id values..!")
                transformed_df.loc[invalid_category_mask, "category_id"] = None
                print("Invalid categoryd_id values changed to NULL")
            else:
                print("All the category_id values are valid - good data quality!")
            
        print(f"Transformed {len(transformed_df)} product records")
        return transformed_df
    
    #STOCKS
    
    def _transform_stocks(self, df):
        
        #copy time
        transformed_df = df.copy()
        
        # beginning the transforming of stocks data by converting store_name to store_id
        # doing this in order to be able to establish relationships between tables later
        # first a dict is created to map store names to store IDs using the store df
        # then each store "name" is replaced by the corresponding "store_id" with .map
        if "store_name" in transformed_df.columns and self.reference_data["stores"] is not None:
            store_name_to_id = dict(zip(self.reference_data["stores"]["name"], self.reference_data["stores"]["store_id"]))
            transformed_df["store_id"] = transformed_df["store_name"].map(store_name_to_id)
            print("converted store names to store IDs in stocks data set")
            # can then remove the store_name columns which is now redundant 
            transformed_df = transformed_df.drop(columns=["store_name"])
            print("Removed store_name column in stocks data set")
        else:
            print("store_name column not found")
        
        #moving on to data type conversions:
        transformed_df["product_id"] = transformed_df["product_id"].astype(int) # product_id -> int
        print("Converted product_id to integers")
        transformed_df["quantity"] = transformed_df["quantity"].astype(int) # quantity -> int
        print("converted quantity to integer")
        
        #lastly, validation that product_id values in the stocks data exist in the products data 
        if self.reference_data["products"] is not None:
            valid_product_ids = set(self.reference_data["products"]["product_id"])
            invalid_product_mask = ~transformed_df["product_id"].isin(valid_product_ids)
        
            if invalid_product_mask.any():
                invalid_count = invalid_product_mask.sum()
                print(f"Warning: Encountered {invalid_count} rows in stocks data set with invalid product IDs")

                #opting to delete any rows in stocks with invalid product ID since it represents non-existing product
                transformed_df = transformed_df[~invalid_product_mask]
                print(f"Removed {invalid_count} stocks rows with invalid product IDs")
            else:
                print("All inventory in stock has a valid product ID - Yay!")
            
        #save the transformed stocks data

        print(f"Transformed {len(transformed_df)} rows of stocks records")
        return transformed_df
    
    #CUSTOMERS
    
    def _transform_customers(self, df):
        
            
        # copy ok ok
        transformed_df = df.copy()
        
        # data type conversion for customers data set columns
        
        transformed_df["customer_id"] = transformed_df["customer_id"].astype(int) #customer_id -> int (primary key)
        print("converted customer_id to integer")

        for col in ['first_name', 'last_name', 'phone', 'email', 'street', 'city', 'state']: # -> all strings
            if col in transformed_df.columns:
                transformed_df[col] = transformed_df[col].fillna('').astype(str)
        print("Converted 'first_name', 'last_name', 'phone', 'email', 'street', 'city', 'state' to string values and converted NaN to empty strings")
        
        if "zip_code" in transformed_df.columns:
            transformed_df["zip_code"] = pd.to_numeric(transformed_df["zip_code"], errors="coerce") # zip_code -> numeric first
            transformed_df["zip_code"] = transformed_df["zip_code"].fillna(0).astype(int) # NaN are replaced ith 0 and zip_code -> int
            print("Zip codes are converted to numeric, NaN are replaced with 0, and zip codes are finally converted to integers")
            
        print(f"Transformed {len(transformed_df)} rows of customers data")
        return transformed_df
    
    #ORDERS
    
    def _transform_orders(self, df):
        
        # copy copy copy
        transformed_df = df.copy()
                
        # data type conversions
        transformed_df["order_id"] = transformed_df["order_id"].astype(int) # order_id -> int
        transformed_df["customer_id"] = transformed_df["customer_id"].astype(int) # customer_id -> int
        transformed_df["order_status"] = transformed_df["order_status"].astype(int) # order_status -> int
        print("converted order_id, customer_id, and order_status to integers")
        
        # data type conversion cont... Dates <____<
        # converting string dates into DATETIME objects with pandas
        transformed_df["order_date"] = pd.to_datetime(transformed_df["order_date"], format="%d/%m/%Y", errors="coerce") #order_date -> datetime 
        transformed_df["required_date"] = pd.to_datetime(transformed_df["required_date"],format="%d/%m/%Y", errors="coerce") #required_date -> datetime
        transformed_df["shipped_date"] = pd.to_datetime(transformed_df["shipped_date"], format="%d/%m/%Y", errors="coerce") #shipped_date -> datetime
        print("converted order_date, required_date, and shipped_date to datetime data types. Note that shipped_date values may Null values (=not shipped yet)")
        
        # Next, changing store names to store IDs (and thus creation of relationship with stores table)
        if "store" in transformed_df.columns and self.reference_data["stores"] is not None:
            store_name_to_id = dict(zip(self.reference_data["stores"]["name"], self.reference_data["stores"]["store_id"]))
            transformed_df["store_id"] = transformed_df["store"].map(store_name_to_id)
            transformed_df = transformed_df.drop(columns=["store"])
            print("Converted store names to store_id referencing staffs table")
            
        # changing staff_name to staff_id. note that staff_name in orders corresponds to first_name in our staffs data set
        if "staff_name" in transformed_df.columns and self.reference_data["staffs"] is not None:

            staff_name_to_id = dict(zip(self.reference_data["staffs"]["first_name"], self.reference_data["staffs"]["staff_id"]))
            transformed_df["staff_id"] = transformed_df["staff_name"].map(staff_name_to_id)
            transformed_df = transformed_df.drop(columns=["staff_name"])
            print("converted staff names to staff_id referencing staffs table")
            
        # lastly, validating customer_id's, ensuring that all orders are referencing customers that exist
        # OPting to setting potential orders with invalid customer_id to NULL to keep the data
        if self.reference_data["customers"] is not None:
            valid_customer_ids = set(self.reference_data["customers"]["customer_id"])
            invalid_customer_mask = ~transformed_df["customer_id"].isin(valid_customer_ids)
            
            if invalid_customer_mask.any():
                invalid_count = invalid_customer_mask.sum()
                transformed_df.loc[invalid_customer_mask, "customer_id"] = None
                print(f"Attention: encountered {invalid_count} orders where customer_id is invalid! Where applicable, customer_id set as NULL")
            else:
                print("No issues encountered when validating customer_id in orders data set")
                
        print(f"Transformed {len(transformed_df)}  rows of orders data")
        return transformed_df
    
    #ORDER_ITEMS
    
    def _transform_order_items(self, df):
        
        # copy dataframe
        transformed_df = df.copy()
        
        # conversion of datatypes
        transformed_df["order_id"] = transformed_df["order_id"].astype(int) #order_id -> int
        transformed_df["product_id"] = transformed_df["product_id"].astype(int) # product_id -> int
        transformed_df["quantity"] = transformed_df["quantity"].astype(int) # quantity -> int
        print("Converted order_id, item_id, product_id, and quantity to integers")    
        transformed_df["list_price"] = pd.to_numeric(transformed_df["list_price"], errors="coerce") #list_price -> numeric (to allow decimals -> float)
        transformed_df["discount"] = pd.to_numeric(transformed_df["discount"], errors="coerce") #discount -> numeric (ditto)
        print("Converted list_price and discount to numeric (-> float) values")
        
        #next up, validating order_id against the orders data set, ensuring that the ordered items refer to actual orders
        if self.reference_data["orders"] is not None:
            valid_order_ids = set(self.reference_data["orders"]["order_id"])
            invalid_order_mask = ~transformed_df["order_id"].isin(valid_order_ids)
            
            if invalid_order_mask.any():
                invalid_count = invalid_order_mask.sum()
                transformed_df = transformed_df[~invalid_order_mask] # deletes the bad rows
                print(f"Warning!! Found {invalid_count} rows of order_items data with invalid order_ids - These rows have been removed from the transformed order_items data")
            else:
                print("Wow, all order items reference valid order_id - Nice data")
            
        # same thing with product_id's - ensuring that all products in order_items reference actual products in the products table
        if self.reference_data["products"] is not None:
            valid_product_ids = set(self.reference_data["products"]["product_id"])
            invalid_product_mask = ~transformed_df["product_id"].isin(valid_product_ids)
            
            if invalid_product_mask.any():
                invalid_count = invalid_product_mask.sum()
                transformed_df.loc[invalid_order_mask, "product_id"] = None # opting to set these as NULL rather than delete
                print(f"Warning!! Found {invalid_count} rows of order_items data with invalid product_id's - these set as NULL values")
            else:
                print("Yay, all order items reference valid product_id - Nice data")
                
        # ensuring all quantities are positive 
        negative_qty_mask = transformed_df["quantity"] <= 0
        if negative_qty_mask.any():
            negative_count = negative_qty_mask.sum()                   
            transformed_df.loc[negative_qty_mask, 'quantity'] = 1 # Fixing the issue by setting to a minimum value of 1
            print(f"Oops! Found {negative_count} order items with zero or negative quantities, that doens't make sense. Correctly the affected rows by setting val as 1")

        # likewise, ensure that all discounts are between 0 and 1 (=0% to 100%)
        invalid_discount_mask = (transformed_df["discount"] < 0) | (transformed_df["discount"] > 1)
        if invalid_discount_mask.any():
            invalid_count = invalid_discount_mask.sum()
            transformed_df.loc[transformed_df["discount"] < 0, "discount"] = 0 # if negative, set to 0
            transformed_df.loc[transformed_df["discount"] > 1, "discount"] = 1 # if > 1 set to 1
            print(f"Warning: Found {invalid_count} order items with invalid discount values.. Vals > 1 set to 1, vals < 0 set to 0 ")

        print(f"Transformed {len(transformed_df)} rows of order_item records")
        return transformed_df

                
