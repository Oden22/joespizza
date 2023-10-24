import pyodbc
from pymongo import MongoClient
import random
from datetime import datetime

class SQLDataAccess:
    #This class is used to create objects for azure sql operations
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = self.connect()

    def connect(self):
        # Azure MySQL Database Configuration
        connection_attempt = 1
        print("Connecting to " + self.server)
        driver= '{ODBC Driver 18 for SQL Server}'
        connection_string = 'Driver='+driver+';Server=tcp:'+self.server+',1433;Database='+self.database+';Uid='+self.username+';PWD='+self.password+';Encrypt=yes;TrustServerCertificate=no;'
        
        try:
            #Connnect to the database
            connection = pyodbc.connect(connection_string, timeout=30)
            print("Successfully connected")
            return connection
        except Exception as e:
            print("Failed to Connect to " + self.server)
            print(e)
            connection_attempt += 1
        raise ValueError('Cannot Connect With This Object')

    def get_data(self, query):
        #Get data using a query from the sql server
        print("Getting Data from " + self.server + " using query:\n" + query + "\n")
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            # Fetch column names from cursor.description
            column_names = [column[0] for column in cursor.description]

            data = []

            #Format the data to have column names
            for row in rows:
                format_data = dict(zip(column_names, row))
                data.append(format_data)

            return data
        except pyodbc.OperationalError as e:
            print("Error: Connection timed out. Creating new objects and retrying...")
            self.connection.close() 
            raise ValueError('Cannot Connect With This Object')

    def commit_data(self, query, parameters=None):
        #Commit data to the sql server using a sepcified query
        print("Committing Data to " + self.server + " using query:\n" + query + "\n")
        try:
            cursor = self.connection.cursor()
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            self.connection.commit()
        except pyodbc.OperationalError as e:
            print("Error: Connection timed out. Creating new objects and retrying...")
            self.connection.close() 
            raise ValueError('Cannot Connect With This Object')

class CosmosDataAccess:
    #This class is used to create objects to connect to cosmos db 
    def __init__(self, db_name, uri):
        self.db_name = db_name
        self.uri = uri
        self.mongo_client = MongoClient(uri)
        self.my_db = self.mongo_client[self.db_name]

    def add_data_to_cosmos(self, data, cosmos_collection_name):
        #Add data to cosmos collection
        print("Adding data to Cosmos collection " + cosmos_collection_name)
        my_col = self.my_db[cosmos_collection_name]

        #If there is sever rows of data use insert_many, otherwise use insert_one
        if isinstance(data, list):
                result = my_col.insert_many(data)
                return result.inserted_ids
        elif isinstance(data, dict):
            result = my_col.insert_one(data)
            return result.inserted_id
        else:
            raise ValueError("Data should be a dictionary or a list of dictionaries")
        
    def get_data_from_cosmos(self, cosmos_collection_name, query, projection=None):
        #Get data from a cosmos collection
        print("Getting Cosmos Data from collection: " + cosmos_collection_name)
        my_col = self.my_db[cosmos_collection_name]
        
        # Apply the projection if provided
        if projection is not None:
            my_docs = my_col.find(query, projection)
        else:
            my_docs = my_col.find(query)
        
        return my_docs

    def get_daily_summary(self, target_date):
        #Get the daily summary of orders from the cosmos collection Docket
        docket_collection = self.my_db["Docket"]
        total_orders = docket_collection.count_documents({"orderDate": target_date})
        total_sales = sum(order["totalOrderPrice"] for order in docket_collection.find({"orderDate": target_date}))
        total_commision = sum(order["totalDriverCommision"] for order in docket_collection.find({"orderDate": target_date}))

        #Pipeline queryies to get the most popular pizza
        pipeline = [
            {"$match": {"orderDate": target_date}},
            {"$unwind": "$orderItems"},
            {"$group": {"_id": "$orderItems.productName", "count": {"$sum": "$orderItems.quantity"}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        most_popular_pizza = docket_collection.aggregate(pipeline)

        # Extract the result
        most_popular_pizza = list(most_popular_pizza)
        most_popular_pizza = most_popular_pizza[0] if most_popular_pizza else None

        return total_orders, total_sales, total_commision, most_popular_pizza["_id"]

class OrderManager:
    #This class is created to intereact with the server connection object and run the stores operations
    def __init__(self, clientServ, mainServ, cosmosServ):
        self.clientServ = clientServ
        self.mainServ = mainServ
        self.cosmosServ = cosmosServ

    def format_daily_orders(self, day_orders):
        #Format the daily orders in a usable way for the document database
        print("Formatting Orders")
        orders = {}

        for order_data in day_orders:
            order_id = order_data['ORDER_ID']
            
            if order_id not in orders:
                orders[order_id] = {
                    "orderId": order_data["ORDER_ID"],
                    "customer": {
                        "customerId": order_data['CUSTOMER_ID'],
                        "firstName": order_data['FIRST_NAME'],
                        "lastName": order_data['LAST_NAME'],
                        "phone": order_data['PHONE'],
                        "address": order_data['ADDRESS'],
                        "postCode": order_data['POST_CODE']
                    },
                    "orderDate": str(order_data['ORDER_DATE']), 
                    "storeId": "1102929",  
                    "orderItems": []
                }
            order_items = {
                "itemId": order_data["ORDER_ITEM_ID"],
                "productName": order_data['PRODUCT_NAME'],
                "quantity": order_data['QUANTITY'],
                "itemPrice": float(order_data['LIST_PRICE']),
                "totalPrice": float(order_data['LIST_PRICE']) * int(order_data['QUANTITY'])
            }
            orders[order_id]["orderItems"].append(order_items)
            total_order_price = sum(item["totalPrice"] for item in orders[order_id]["orderItems"])
            orders[order_id]["totalOrderPrice"] = total_order_price

        orders_to_return = []
        for k, v in orders.items():
            orders_to_return.append(v)
        return orders_to_return

    def create_docket(self, order):
        #Create a docket
        docket = order
        print("Creating Docket")
        try:
            driver = self.cosmosServ.get_data_from_cosmos("Driver", {
                "$and": [
                    {"suburbStart": {"$lte": int(order['customer']['postCode'])}},
                    {"suburbEnd": {"$gte": int(order['customer']['postCode'])}}
                ]
            })
            driver = driver[0]
        except:
            print("Failed to find driver covering range, finding closest driver")
            customer_postcode = int(order['customer']['postCode'])

            # Search for the driver with the closest range to the postCode
            drivers = self.cosmosServ.get_data_from_cosmos("Driver", {})

            # Initialize variables to keep track of the closest driver and the minimum difference
            driver = None
            min_difference = float('inf')

            for driver in drivers:
                suburb_start = driver.get("suburbStart")
                suburb_end = driver.get("suburbEnd")

                if suburb_start is not None and suburb_end is not None:
                    difference = min(abs(suburb_start - customer_postcode), abs(suburb_end - customer_postcode))

                    if difference < min_difference:
                        min_difference = difference
                        driver = driver

        docket["driverId"] = driver["driverId"]
        docket["driverName"] = driver["name"]
        docket["totalDriverCommision"] = float(driver["comission"]) * float(docket["totalOrderPrice"])
        self.cosmosServ.add_data_to_cosmos(docket, "Docket")
        return docket

    def get_and_format_daily_orders(self, date):
        #Get the daily orders and format them
        daily_orders = self.mainServ.get_data(
            f"""
                SELECT 
                    PO.ORDER_ID, 
                    ORDER_DATE,
                    PA.CUSTOMER_ID,
                    FIRST_NAME,
                    LAST_NAME,
                    PHONE,
                    ADDRESS,
                    POST_CODE,
                    ORDER_ITEM_ID,
                    QUANTITY,
                    PRODUCT_NAME,
                    LIST_PRICE 
                FROM PIZZA.ORDERS PO
                INNER JOIN PIZZA.CUSTOMERS PA ON PA.CUSTOMER_ID = PO.CUSTOMER_ID
                INNER JOIN PIZZA.ORDER_ITEMS OI ON OI.ORDER_ID = PO.ORDER_ID
                WHERE ORDER_DATE = (SELECT MIN(ORDER_DATE) FROM PIZZA.ORDERS WHERE ORDER_DATE = '{date}')
                """
            )
        formatted_daily_orders = self.format_daily_orders(daily_orders)
        return formatted_daily_orders

    def process_orders(self, date):
        #Gets the daily orders, annd creates dockets for each of them
        formatted_daily_orders = self.get_and_format_daily_orders(date)

        days_orders_cursor = self.cosmosServ.get_data_from_cosmos("Orders", {"orderDate": date})
        days_orders = list(days_orders_cursor)  # Convert cursor to a list

        if days_orders:
            dockets_cursor = self.cosmosServ.get_data_from_cosmos("Docket", {"orderDate": date}, {'_id': 0})
            dockets = list(dockets_cursor)  # Convert cursor to a list
            print("Orders have already been created")
            if dockets:
                print("Dockets have already been created")
                return dockets
            else:
                for order in formatted_daily_orders:
                    self.create_docket(order)
        else:
            self.cosmosServ.add_data_to_cosmos(formatted_daily_orders, "Orders")
            for order in formatted_daily_orders:
                self.create_docket(order)


        dockets_cursor = self.cosmosServ.get_data_from_cosmos("Docket", {"orderDate": date}, {'_id': 0})
        dockets = list(dockets_cursor) 

        return dockets

    def run_day_operations(self, date):
        #Run operations for a complete day, creating dockets, and the summary data
        self.process_orders(date)
        self.end_of_day_operations(date)

    def create_new_order(self, order):
        #Create a new order
        total_price = 0
        last_order_id = self.mainServ.get_data("select MAX(order_id) as order_id from pizza.orders")[0]["order_id"]
        order["orderId"] = int(last_order_id) + 1

        for item in order['orderItems']:
            try:
                order_item_info = self.mainServ.get_data(
                    f"select MIN(order_item_id) as order_item_id, MIN(list_price) as list_price from pizza.order_items where product_name = '{item['productName']}'"
                )
                item["itemId"] = order_item_info[0]["order_item_id"]
                item["itemPrice"] = float(order_item_info[0]["list_price"])
                total = int(item["quantity"]) * float(item["itemPrice"])
                item["totalPrice"] = total
                total_price += total
            except:
                last_item_id = self.mainServ.get_data("select MAX(order_item_id) as order_item_id from pizza.order_items")[0]["order_item_id"]
                item["itemId"] = int(last_item_id) + 1
                item["itemPrice"] =  random.uniform(5, 50)
                total = int(item["quantity"]) * float(item["itemPrice"])
                item["totalPrice"] = total
                total_price += total

        try:
                customer_info = self.mainServ.get_data(
                    f"select MIN(customer_id) from pizza.customers where first_name = '{order['customer']['firstName']}' && last_name = '{order['customer']['lastName']}'"
                )
                order["customer"]["customerId"] = customer_info[0]["customer_id"]
        except:
            last_customer_id = self.mainServ.get_data("select MAX(customer_id) as customer_id from pizza.customers")[0]["customer_id"]
            order["customer"]["customerId"] = int(last_customer_id) + 1
        
        order['orderDate'] = datetime.now().strftime('%Y-%m-%d')
        order['totalOrderPrice'] = total_price
        self.cosmosServ.add_data_to_cosmos(order, "Orders")
        docket = self.create_docket(order)
        try:
            del docket["_id"]
        except:
            pass
        return docket

    def end_of_day_operations(self, date):
        #Get the daily summary and add the data into the sql servers.
        try:
            total_orders, total_sales, total_commision, most_popular_pizza = self.cosmosServ.get_daily_summary(date)
            summary_dict = {
                "total_orders": total_orders, 
                "total_sales": total_sales, 
                "total_commision": total_commision, 
                "most_popular_pizza": most_popular_pizza
            }
        except:
            print("No orders for current Day")
            summary_dict = {
                "total_orders": 0, 
                "total_sales": 0, 
                "total_commision": 0, 
                "most_popular_pizza": "No Orders"
            }

        try:
            self.clientServ.commit_data( '''
                INSERT INTO DailySummary (Date, TotalOrders, TotalSales, TotalDriverCommission, MostPopularPizza)
                VALUES (?, ?, ?, ?, ?);
            ''', (date, total_orders, total_sales, total_commision, most_popular_pizza))
        except:
            print("Summary Already Created")


        self.mainServ.commit_data('''
            INSERT INTO pizza.summary (store_id, summary_date, total_sales, total_orders, best_product)
            VALUES (?, ?, ?, ?, ?);
        ''', (1102929, date, total_sales, total_orders, most_popular_pizza))

        return summary_dict
        
