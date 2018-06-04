import pymongo
from pymongo import MongoClient
import configparser
from bson.objectid import ObjectId
import redis

config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['mongo_connection']

customers = None
products = None
orders = None
redisConn = None #This is the redis connection object. Used to access redis.py functions

customer_keys = ('firstName', 'lastName', 'street', 'city', 'state', 'zip')

# The following functions are REQUIRED - you should REPLACE their implementation
# with the appropriate code to interact with your Mongo database.
def initialize():
    # this function will get called once, when the application starts.
    # this would be a good place to initalize your connection!
    global customers
    global products
    global orders
    global redisConn

    client = MongoClient(connection_string)
    customers = client.project2.customers
    products = client.project2.products
    orders = client.project2.orders

    #Connect to redis here
    redisConn = redis.StrictRedis(
                                    host='redis-17466.c14.us-east-1-3.ec2.cloud.redislabs.com',
                                    port = 17466,
                                    password = 'temppasswprd',
                                    db = 0,
                                    charset = "utf-8",
                                    decode_responses = True
                                )

def get_customers():
    existingCustomers = customers.find({})

    for customer in existingCustomers:
        yield customer


def get_customer(id):
    return customers.find_one({'_id' : ObjectId(id)})

def upsert_customer(customer):
    if '_id' in customer: #Means edit
        oneCustomer = get_customer(customer['_id']);
        toUpdate = {
                        'firstName' : oneCustomer['firstName'],
                        'lastName' : oneCustomer['lastName'],
                        'street' : oneCustomer['street'],
                        'city' : oneCustomer['city'],
                        'state' : oneCustomer['state'],
                        'zip' : oneCustomer['zip']
                    }

        updateWith = {
                        'firstName' : customer['firstName'],
                        'lastName' : customer['lastName'],
                        'street' : customer['street'],
                        'city' : customer['city'],
                        'state' : customer['state'],
                        'zip' : customer['zip']
                     }
        customers.update_one(toUpdate, {'$set': updateWith} )

    else: #means insert
        documentToInsert = {
                                'firstName' : customer['firstName'],
                                'lastName' : customer['lastName'],
                                'street' : customer['street'],
                                'city' : customer['city'],
                                'state' : customer['state'],
                                'zip' : customer['zip']
                            }
        customers.insert_one(documentToInsert)

def delete_customer(id):
    customers.delete_one({'_id' : ObjectId(id)})

def get_products():
    existingProducts = products.find({})
    for product in existingProducts:
        yield product

def get_product(id):
    product = products.find_one({'_id' : ObjectId(id)})
    return product

def upsert_product(product):
    if '_id' in product: #Means edit
        oneproduct = get_product(product['_id']);
        toUpdate =  {
                        'name' : oneproduct['name'],
                        'price' : oneproduct['price']
                    }

        updateWith = {
                        'name' : product['name'],
                        'price' : product['price']
                    }

        products.update_one(toUpdate, {'$set': updateWith} )
    else: #means insert
        documentToInsert = {
                                'name' : product['name'],
                                'price' : product['price']
                            }

        products.insert_one(documentToInsert)

def delete_product(id):
    products.delete_one({'_id' : ObjectId(id)})
    orders.delete_many({'productId' : ObjectId(id)})    #Cascading
    redisConn.delete(str(id))

def get_orders():
    allOrders = orders.find({})
    for oneOrder in allOrders:
        customerData = get_customer(oneOrder['customerId'])
        productData = get_product(oneOrder['productId'])
        oneOrder['customer'] = customerData
        oneOrder['product'] = productData
        yield oneOrder

def get_order(id):
    return orders.find_one({'_id' : ObjectId(id)})

def upsert_order(order):
    documentToInsert = {
                            'customerId' : ObjectId(order['customerId']),
                            'productId' : ObjectId(order['productId']),
                            'date' : order['date']
                        }
    orders.insert_one(documentToInsert)

    #Invalidate Redis cache
    keyToDelete = str(documentToInsert['productId'])
    redisConn.delete(keyToDelete)

def delete_order(id):
    #Whenever an order is deleted, we need to Invalidate redis cache associated with the product in that order
    tempOrder = orders.find_one({'_id' : ObjectId(id)})
    keyToDelete = str(tempOrder['productId'])
    redisConn.delete(keyToDelete)

    #Finally, delete the order from the database
    orders.delete_one({'_id' : ObjectId(id)})

# Pay close attention to what is being returned here.  Each product in the products
# list is a dictionary, that has all product attributes + last_order_date, total_sales, and
# gross_revenue.  This is the function that needs to be use Redis as a cache.

# - When a product dictionary is computed, save it as a hash in Redis with the product's
#   ID as the key.  When preparing a product dictionary, before doing the computation,
#   check if its already in redis!
def sales_report():
    productsList = list()

    for oneproduct in products.find({}):
        prodId = str(oneproduct['_id'])
        hashToLookFor = redisConn.hgetall(prodId)

        if not redisConn.exists(prodId): #The hash is not present
            tempOrders = orders.find({'productId' : prodId})
            tempOrders = sorted(tempOrders, key=lambda k: k['date'])
            print(tempOrders)
            oneproduct['total_sales'] = len(tempOrders)
            oneproduct['gross_revenue'] = len(tempOrders) * oneproduct['price']

            if len(tempOrders) > 0:
                oneproduct['last_order_date'] = tempOrders[-1]['date']
            else:
                oneproduct['last_order_date'] = 'N/A'

            redisConn.hmset(prodId, oneproduct)
            productsList.append(oneproduct)
        else:
            productsList.append(hashToLookFor)

    return productsList
