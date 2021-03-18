import psycopg2



def connect():
    """deze functie maakt de connectie met de postgres db"""
    connection = psycopg2.connect(host='localhost', database='huwebshope1', user='postgres', password='Lafa22446688##')
    return connection

def disconnect():
    """deze functie breekt de connectie met postgres db"""
    con = connect()
    return con.close()

def sql_execute(sql):
    """deze functie executes de query naar de Postgres db"""
    connection = connect()
    cur = connection.cursor()
    cur.execute(sql)
    connection.commit()

def sql_execute2(sql, value):
    """deze functie executes de query naar de Postgres db"""
    connection = connect()
    cur = connection.cursor()
    cur.execute(sql, value)
    connection.commit()


def sql_select(sql):
    """deze functie selecteert de values van de tabellen op de Postgres db"""
    c = connect()
    cur = c.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    return results





#{ collaborative_filtering }

def collaborative_filter():

    """deze functie selecteert de meeste populair uit het table profiles_previously_viewed en vervolgens maakt een nieuwe table die most_viewed heet en voegt de
       product id's in van profiles_previously_viewed en de namen plus de target audience van de producten van de table products"""


    connect()
    prodid =  sql_select("select prodid from profiles_previously_viewed group by prodid having count(*) > 4000 order by prodid ASC; ")
    sql_execute("DROP TABLE IF EXISTS most_viewed CASCADE")
    sql_execute("""CREATE TABLE most_viewed(most_viewed_prodid VARCHAR(255),naam VARCHAR (255),target_audience VARCHAR (255), FOREIGN KEY (most_viewed_prodid) REFERENCES products (id))""")


    for id  in prodid:
        sql_execute2("Insert into most_viewed(most_viewed_prodid) VALUES (%s)", id)

    naam = sql_select(
        "SELECT name, id FROM products INNER JOIN most_viewed ON products.id = most_viewed.most_viewed_prodid;")
    sub = sql_select("SELECT targetaudience, id FROM products INNER JOIN most_viewed ON products.id = most_viewed.most_viewed_prodid;")

    for i in naam:
        sql_execute2("UPDATE most_viewed SET naam = %s WHERE most_viewed_prodid = %s", (i[0], i[1]))

    for i in sub:
        sql_execute2("UPDATE most_viewed SET target_audience = %s WHERE most_viewed_prodid = %s", (i[0], i[1]))

    print("TABLE {most_viewed} is gemaakt.\ndata is toegevoegd.")
    disconnect()


def Content_filter():
    """deze functie neemt de target audience van de meeste bekeken producten en vervolgens wordt naar de 3 laagste prijzen van elke target audience
    en die wordt toegevoegd aan de nieuw table met de naam en id"""

    connect()

    target_audience= sql_select(
        "SELECT id, name,targetaudience, sellingprice FROM products WHERE targetaudience = 'Unisex' ORDER BY sellingprice ASC LIMIT 3;")

    sql_execute("DROP TABLE IF EXISTS goedkoopste_pro CASCADE")
    sql_execute(
        """CREATE TABLE goedkoopste_pro(goedkoopste_proid VARCHAR(255),naam VARCHAR (255),target_audience VARCHAR (255), selling_price INTEGER
        ,FOREIGN KEY (goedkoopste_proid) REFERENCES products (id))""")

    for target in target_audience:
        sql_execute2("INSERT INTO goedkoopste_pro(goedkoopste_proid, naam,target_audience,selling_price) VALUES (%s, %s,%s,%s)",(target[0], target[1], target[2],target[3]))

    target_audience1= sql_select(
        "SELECT id, name,targetaudience, sellingprice FROM products WHERE targetaudience = 'Mannen' ORDER BY sellingprice ASC LIMIT 3;")

    for target in target_audience1:
        sql_execute2("INSERT INTO goedkoopste_pro(goedkoopste_proid, naam,target_audience,selling_price) VALUES (%s, %s,%s,%s)",(target[0], target[1], target[2],target[3]))

    target_audience2= sql_select(
        "SELECT id, name,targetaudience, sellingprice FROM products WHERE targetaudience = 'Volwassenen' ORDER BY sellingprice ASC LIMIT 3;")

    for target in target_audience2:
        sql_execute2("INSERT INTO goedkoopste_pro(goedkoopste_proid, naam,target_audience,selling_price) VALUES (%s, %s,%s,%s)",(target[0], target[1], target[2],target[3]))

    target_audience3= sql_select(
        "SELECT id, name,targetaudience, sellingprice FROM products WHERE targetaudience = 'Kinderen' ORDER BY sellingprice ASC LIMIT 3;")

    for target in target_audience3:
        sql_execute2("INSERT INTO goedkoopste_pro(goedkoopste_proid, naam,target_audience,selling_price) VALUES (%s, %s,%s,%s)",(target[0], target[1], target[2],target[3]))
    print("\nTABLE {goedkoopste_pro} is gemaakt.\ndata is toegevoegd.")
    disconnect()



collaborative_filter()
Content_filter()