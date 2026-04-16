import random
import string
import time
from datetime import datetime, timedelta

import logging

import psycopg
from psycopg_pool import ConnectionPool   #handles multiple connections as a pool. Enhances performance

#FIX: update your PG database connection.
DATABASE_URL = "postgresql://postgres:pass@127.0.0.1:30432/mydb"
db_pool:ConnectionPool = None

# --- Configure logger once ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

message_counter = 0  #message id counter -- global


#--------------------------------------------------------------------------------
def record_postgres(id, message):
    """Records a message to postgres.
    Expects a table called `events` with column mid (integer) and
    message (text)
    In case of error nothing happens.
    """
    global db_pool
    try:
        sql = """INSERT INTO events(mid, message) VALUES(%s, %s)"""
        
        with db_pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (id, message) ) 

        logger.info(f"Message {id} recorded")
    except Exception as e:
        logger.exception(f"Exception on record msg{id}")

    
    pass


#--------------------------------------------------------------------------------
def get_random_mesg_type():
    return ''.join(random.choices(string.ascii_uppercase, k=4))

#--------------------------------------------------------------------------------
def get_random_username():
    return ''.join(random.choices(string.ascii_lowercase, k=8))
#--------------------------------------------------------------------------------
def generate_user_line(users, save, invalid=False):
    """Generates random user data"""
    
    global message_counter
    
    username = get_random_username()
    
    include_email = random.choice([True, False])
    include_date = random.choice([True, False])
    email = f"{username}@example.com" if include_email else ""
    if include_date:
        days_ago = random.randint(0, 5 * 365)
        join_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    else:
        join_date = ""
    
    users[username]=f"{username},{email},{join_date}"
    message_counter += 1

    key = 'CUST'
    if invalid:
        key = get_random_mesg_type()
        
    save(message_counter, f"{key}:{username},{email},{join_date}")
    

#--------------------------------------------------------------------------------
def get_random_productid():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
#--------------------------------------------------------------------------------
def generate_product_line(products, save, invalid=False):
    """Generates random products"""
    
    global message_counter

    product_id = get_random_productid()
    title = f"Product {product_id}"
    include_stock = random.choice([True, False])
    stock = str(random.randint(0, 1000)) if include_stock else ""
    price = f"{round(random.uniform(5.0, 500.0), 2)}"

    products[product_id]= f"{product_id},{title},{stock},{price}"
    message_counter += 1
    key = 'PROD'
    if invalid:
        key = get_random_mesg_type()
        
    save(message_counter, f"{key}:{product_id:15}{title:50}{stock:10}{price:20}")




#--------------------------------------------------------------------------------
def _rand_alpha(min_len, max_len):
    length = random.randint(min_len, max_len)
    return ''.join(random.choices(string.ascii_letters + ' ', k=length)).strip()

#--------------------------------------------------------------------------------
def generate_review(users: dict, products: dict, save, invalid=False):
    """Generates random reviews"""
    
    global message_counter
    if not users or not products:
        return

    #Select user and product. some random dirty data is generated as well. 
    if random.random() < 0.25:
        username = get_random_username()
    else:
        username = random.choice(list(users.keys()))
        
    if random.random() < 0.25:
        product = get_random_productid()
    else:
        product = random.choice(list(products.keys()))
    
    title = _rand_alpha(1, 30)
    comment = _rand_alpha(40, 120)
    review_time = datetime.now().strftime("%Y%m%d%H%M")
    message_counter += 1
    key = 'VIEW'
    if invalid:
        key = get_random_mesg_type()
        
    save(message_counter, f"{key}:" + str({"username": username,
                                          "product": product,
                                          "datetime":review_time,
                                          "title": title,
                                          "comment": comment })
         )


#--------------------------------------------------------------------------------
def sleep():
    #this simulates the generation randomness
    random_number = random.uniform(0.1, 1.0)
    time.sleep(random_number)


#================================================================================
#================================================================================
if __name__=="__main__":
    USER_PROB = 0.2        # Probability for New random user generation 
    PROD_PROB = 0.4        # Probability for New random product generation
    REVIEW_PROB = 0.8      # Probability for New random review generation
    INVALID_PROB = 0.1     # Probability of invalid messages
    
    run = True
    users = {}
    products = {}

    logger.info("Process Started")
    
    print("*"*60)
    print(f"{'Select Action':^60}")
    print("*"*60)
    print(" 1) Print to Screen")
    print(" 2) Save to Postgres")
    print("-----------------------------")
    option = input("Select Option>")
    if option not in ["1","2"]:
        exit(1)
    elif int(option) == 1:
        save = print             # display data
    else:
        db_pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=3)
        save = record_postgres   # save data to PostgreSQL
        

    for i in range(20):
        generate_user_line(users, save)
    for i in range(10):
        generate_product_line(products, save)

        
    while run:
        try:
            invalid = False
            if random.random() < INVALID_PROB :
                invalid = True
                
            if random.random() < USER_PROB :
                generate_user_line(users, save, invalid)
            
            if random.random() < PROD_PROB :
                generate_product_line(products, save, invalid)
            
            if random.random() < REVIEW_PROB :
                generate_review(users, products, save, invalid)

            sleep()
            pass
        except KeyboardInterrupt:
            run = False
            print("\nSystem Terminated")
            
