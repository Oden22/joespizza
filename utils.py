import pyodbc
from pymongo import MongoClient
import random
from datetime import datetime
import time
import base64
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.styles import getSampleStyleSheet

class SQLDataAccess:
    '''
        This class is used to create objects for azure sql operations
    '''
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = self.connect()

    def connect(self):
        '''
            This Function is used to connect to the SQL Database and return the connection
        '''
        print("Connecting to " + self.server)
        driver= '{ODBC Driver 18 for SQL Server}'
        connection_string = 'Driver='+driver+';Server=tcp:'+self.server+',1433;Database='+self.database+';Uid='+self.username+';PWD='+self.password+';Encrypt=yes;TrustServerCertificate=no;'
        attempt = 0
        while attempt < 3:
            try:
                #Connnect to the database
                connection = pyodbc.connect(connection_string, timeout=30)
                print("Successfully connected")
                return connection
            except Exception as e:
                print("Failed to Connect to " + self.server)
                print(e)
                attempt += 1
                time.sleep(30)
        
        raise ValueError('Cannot Connect With This Object')

    def get_data(self, query):
        '''
            This function is used to get data from the sql database using a defined query.
        '''
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
        '''
            This function is used to commit data to the sql server using a sepcified query
        '''
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
    '''
        This class is used to create objects to connect to cosmos db 
    '''
    def __init__(self, db_name, uri):
        self.db_name = db_name
        self.uri = uri
        self.mongo_client = MongoClient(uri)
        self.my_db = self.mongo_client[self.db_name]

    def add_data_to_cosmos(self, data, cosmos_collection_name):
        '''
            This function is used to add data to cosmos collection
        '''
        print("Adding data to Cosmos collection " + cosmos_collection_name)
        my_col = self.my_db[cosmos_collection_name]

        #If there is several rows of data use insert_many, otherwise use insert_one
        if isinstance(data, list):
                result = my_col.insert_many(data)
        elif isinstance(data, dict):
            result = my_col.insert_one(data)
        else:
            raise ValueError("Data should be a dictionary or a list of dictionaries")
        
    def get_data_from_cosmos(self, cosmos_collection_name, query, projection=None):
        '''
            This function is used for getting data from a cosmos collection
        '''
        print("Getting Cosmos Data from collection: " + cosmos_collection_name)
        my_col = self.my_db[cosmos_collection_name]
        
        # Apply the projection if provided
        if projection is not None:
            my_docs = my_col.find(query, projection)
        else:
            my_docs = my_col.find(query)
        
        return my_docs

    def get_daily_summary(self, target_date):
        '''
            This function is used to get the daily summary of orders from the cosmos collection Docket
        '''
        order_collection = self.my_db["Orders"]
        total_orders = order_collection.count_documents({"orderDate": target_date})

        # Define the pipeline for aggregating the total_sales and total_commision
        pipeline = [
            {"$match": {"orderDate": target_date}},
            {"$group": {
                "_id": None,
                "total_sales": {"$sum": "$totalOrderPrice"},
                "total_commision": {"$sum": "$totalDriverCommision"}
            }}
        ]

        # Execute the aggregation query
        result = list(order_collection.aggregate(pipeline))

        # Get the total_sales and total_commision
        if result:
            total_sales = result[0]["total_sales"]
            total_commision = result[0]["total_commision"]
        else:
            total_sales = 0
            total_commision = 0

        #Pipeline queryies to get the most popular pizza
        pipeline = [
            {"$match": {"orderDate": target_date}},
            {"$unwind": "$orderItems"},
            {"$group": {"_id": "$orderItems.productName", "count": {"$sum": "$orderItems.quantity"}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        most_popular_pizza = order_collection.aggregate(pipeline)

        # Extract the result
        most_popular_pizza = list(most_popular_pizza)
        most_popular_pizza = most_popular_pizza[0] if most_popular_pizza else None

        return total_orders, total_sales, total_commision, most_popular_pizza["_id"]

    def aggregate_data_in_cosmos(self, collection, pipeline):
        my_col = self.my_db[collection]
        result = list(my_col.aggregate(pipeline))
        return result

class OrderManager:
    '''
        This class is created to intereact with the server access objects and run the stores operations
    '''
    def __init__(self, clientServ, mainServ, cosmosServ):
        self.clientServ = clientServ
        self.mainServ = mainServ
        self.cosmosServ = cosmosServ

    def format_daily_orders(self, day_orders):
        '''
            Format the daily orders in a usable way for the document database
            Returns: formatted daily orders
        '''
        
        print("Formatting Orders")
        orders = {}

        for order_data in day_orders:
            order_id = order_data['ORDER_ID']
            
            if order_id not in orders:
                #If the order hasnt been created already set the base data
                driver = self.get_closest_driver(order_data['POST_CODE'])

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
                    "orderItems": [],
                    "driverId": driver["driverId"],
                    "driverName": driver["name"],
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
            orders[order_id]["totalDriverCommision"] = round(float(driver["comission"]) * total_order_price, 2)

        #Turn the orders into a list
        orders_to_return = []
        for k, v in orders.items():
            orders_to_return.append(v)
        return orders_to_return

    def create_pdf(self, docket):
        '''
            This function creates a pdf from the docket and encodes it into a base64 string
            Returns: base64 encoded pdf
        '''
        doc = SimpleDocTemplate("temp.pdf", pagesize=letter)
        story = []

        # Create a paragraph style with word wrapping
        styles = getSampleStyleSheet()
        style = styles["Normal"]
        style.wordWrap = 'CJK'

        # Iterate through the dictionary and add each item as a paragraph
        for key, value in docket.items():
            item = f"<b>{key}:</b> {value}"
            p = Paragraph(item, style)
            story.append(p)

        doc.build(story)

        # Encode the generated PDF content as base64
        with open("temp.pdf", "rb") as pdf_file:
            pdf_content_base64 = base64.b64encode(pdf_file.read()).decode('utf-8')

        return pdf_content_base64

    def create_docket_for_order(self, order):
        '''
            This function creates the docket for the order
            Returns: docket
        '''
        docket = {}
        docket["customer"] = order["customer"]
        docket["orderDate"] = order["orderDate"]
        docket["storeId"] = order["storeId"]
        docket["orderItems"] = order["orderItems"]
        docket["driverName"] = order["driverName"]
        docket["OrderTotal"] = order["totalOrderPrice"]
        docket["pdf"] = self.create_pdf(docket)
        return docket

    def get_daily_orders(self,date):
        '''
            This function gets the daily orders from the head office sql server
            Returns: Daily orders
        '''
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
        return daily_orders

    def get_closest_driver(self, postcode):
        '''
        This function gets the closest driver given a postcode.
        Returns: Driver dictionary
        '''
        try:
            driver = self.cosmosServ.get_data_from_cosmos("Driver", {
                "$and": [
                    {"suburbStart": {"$lte": int(postcode)}},
                    {"suburbEnd": {"$gte": int(postcode)}}
                ]
            })
            driver = driver[0]
        except:
            print("Failed to find driver covering range, finding closest driver")
            customer_postcode = int(postcode)

            # Use MongoDB aggregation framework to find the closest driver
            pipeline = [
                {
                    "$match": {
                        "suburbStart": {"$exists": True},
                        "suburbEnd": {"$exists": True}
                    }
                },
                {
                    "$project": {
                        "driver": "$$ROOT",
                        "difference": {
                            "$min": [
                                {"$abs": {"$subtract": ["$suburbStart", customer_postcode]}},
                                {"$abs": {"$subtract": ["$suburbEnd", customer_postcode]}}
                            ]
                        }
                    }
                },
                {
                    "$sort": {"difference": 1}
                },
                {
                    "$limit": 1
                }
            ]

            drivers = self.cosmosServ.aggregate_data_in_cosmos("Driver", pipeline)
            driver = drivers[0]['driver'] if drivers else None
            print(drivers)

        return driver


    def process_orders(self, date):
        '''
            This function adds the daily orders into the document database
            Returns: Formmated Daily Orders
        '''
        #Check if the orders already exist
        days_orders_cursor = self.cosmosServ.get_data_from_cosmos("Orders", {"orderDate": date}, {'_id': 0})
        days_orders = list(days_orders_cursor)  # Convert cursor to a list

        if days_orders:
            print("Orders have already been created")
            return days_orders
        else:
            daily_orders = self.get_daily_orders(date)
            formatted_daily_orders = self.format_daily_orders(daily_orders)

            #Create dockets for each order
            for order in formatted_daily_orders:
                docket = self.create_docket_for_order(order)
                order["docket"] = docket

            self.cosmosServ.add_data_to_cosmos(formatted_daily_orders, "Orders")

        #Remove the '_id' tag from the orders
        for order in formatted_daily_orders:
            order.pop("_id", None)
        return formatted_daily_orders

    def run_day_operations(self, date):
        '''
            This function adds the daily orders into the document 
            database and then creates a summary of the daily orders and adds it into the sql databases
            Returns: null
        '''
        self.process_orders(date)
        self.end_of_day_operations(date)

    def create_new_order(self, order):
        '''
            This function creates a new order and adds the order to the document database
            Returns: Complete order
        '''
        

        total_price = 0

        #get the last order id from head office to ensure unique order id
        last_order_id = self.mainServ.get_data("select MAX(order_id) as order_id from pizza.orders")[0]["order_id"]
        order["orderId"] = int(last_order_id) + 1

        #Extract and add the order items
        for item in order['orderItems']:
            try:
                #Get order item info from head office
                order_item_info = self.mainServ.get_data(
                    f"select MIN(order_item_id) as order_item_id, MIN(list_price) as list_price from pizza.order_items where product_name = '{item['productName']}'"
                )
                item["itemId"] = order_item_info[0]["order_item_id"]
                item["itemPrice"] = float(order_item_info[0]["list_price"])
            except:
                #Create new order id from the last id
                last_item_id = self.mainServ.get_data("select MAX(order_item_id) as order_item_id from pizza.order_items")[0]["order_item_id"]
                item["itemId"] = int(last_item_id) + 1
                item["itemPrice"] =  random.uniform(5, 50)
            finally:
                total = int(item["quantity"]) * float(item["itemPrice"])
                item["totalPrice"] = total
                total_price += total

        try:
            #Get customer info from the head office
            customer_info = self.mainServ.get_data(
                f"select MIN(customer_id) from pizza.customers where first_name = '{order['customer']['firstName']}' && last_name = '{order['customer']['lastName']}'"
            )
            order["customer"]["customerId"] = customer_info[0]["customer_id"]
        except:
            #Create new customer id from last customer
            last_customer_id = self.mainServ.get_data("select MAX(customer_id) as customer_id from pizza.customers")[0]["customer_id"]
            order["customer"]["customerId"] = int(last_customer_id) + 1
        
        order['orderDate'] = datetime.now().strftime('%Y-%m-%d')
        order['totalOrderPrice'] = total_price

        driver = self.get_closest_driver(order["customer"]["postCode"])

        order["driverId"] = driver["driverId"]
        order["driverName"] = driver["name"]
        order["totalDriverCommision"] = round(float(driver["comission"]) * total_price, 2)

        docket = self.create_docket_for_order(order)
        order["docket"] = docket

        self.cosmosServ.add_data_to_cosmos(order, "Orders")
        del order["_id"]
        return order

    def end_of_day_operations(self, date):
        '''
            This functions gets the daily summary and adds the data into the sql servers.
            returns: summary dictionary
        '''
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
            return summary_dict


        self.mainServ.commit_data('''
            INSERT INTO pizza.summary (store_id, summary_date, total_sales, total_orders, best_product)
            VALUES (?, ?, ?, ?, ?);
        ''', (1102929, date, total_sales, total_orders, most_popular_pizza))

        return summary_dict
        
