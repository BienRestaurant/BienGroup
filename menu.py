import gspread
#import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import json
import sqlite3
from sqlite3 import Error
from gspread_formatting import *
import requests
import os

class StoreData:
    def __init__(self, name):
        self.name = name
        self.cells = []
        self.formats = []


    def create_sheet(self, worksheet, delivery_date):
        self.delivery_date = delivery_date
        self.file_name = self.name + " " + delivery_date
        self.sheet = get_sheet(worksheet, self.file_name)

    def get_link(self):
        url_format = "https://spreadsheets.google.com/feeds/download/spreadsheets/Export?key=%s&exportFormat=%s&gid=%i"
        return url_format % ("15x2NG_HhyVZCnzcVkjq1WCx20Zo4TvRDSLS3ASsaxbA", "pdf", self.sheet.id)

    def append_cell(self, row, col, value):
        self.cells.append(gspread.Cell(row, col, value))

    def append_format(self, range, fmt):
        self.formats.append((range, fmt))

    def submit(self):
        self.sheet.update_cells(self.cells)
        format_cell_ranges(self.sheet, self.formats)
        url = self.get_link()
        r = requests.get(url)
        home = os.path.expanduser("~")
        downloads = os.path.join(home, "Downloads")
        file_name = self.file_name.replace('/','_')
        file = os.path.join(downloads, file_name + '.pdf')
        with open(file, 'wb') as saveFile:
            saveFile.write(r.content)




def process_spreadsheet(conn, retrieve_records):
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)

    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    wk = client.open("阿扁在威郡")
    if retrieve_records:
        products = wk.worksheet("Products").get_all_records()
        categories = wk.worksheet("ProductCategories").get_all_records()
        stores = init_products(conn, products, categories)
        sheet = wk.worksheet("阿扁私房菜")

        # Extract and print all of the values
        records = sheet.get_all_records()
        print("Processing orders...")
        for order in records:
            process_order(conn, order)
    else:
        categories = wk.worksheet("ProductCategories").get_all_records()
        stores = [cat['name'] for cat in categories]
    delivery_date = "6/7"
    resume_id = 1
    i = 1
    for store_name in stores:
        if i >= resume_id:
            analyze_store(conn, wk, delivery_date, store_name)
        i+=1
    analyze_customers(conn, wk, delivery_date)
    #
    #analyze_store(conn, wk, delivery_date, '一芳水果茶')

def save_order_item(conn, order_id, name, options, amount, quantity, store=None):
    print("Saving order %s %s, %s, %f, %d" % (store, name, options, amount, quantity))
    order_sql = "insert into order_items (order_id, store, product_name, product_options, product_unit_price, quantity) values (?,?,?,?,?,?)"
    cur = conn.cursor()
    cur.execute(order_sql, (order_id, store, name, options, amount, quantity))

def process_order_group1(conn, order_id, data):
    for line in data.split("\n"):
        if line:
            options = [] 
            if line.startswith("Subtotal:") or line.startswith("Tax:"): #Subtotal: 144.50
                continue
            elif line.startswith("Total:"): #Total: 147.50
                total = float(line.split(": ")[1])
            else:
                results = line.split("(Am") #傳統客家小炒  (Amount: 16.00 USD, Quantity: 1)
                name = results[0].strip()
                #print("item name:" + name)
                quantity = 1
                amount = 0
                values = results[1].split(")")[0] #ount: 16.00 USD, Quantity: 1)
                for v in values.split(", "):
                    if v.startswith("ount:"): #ount: 16.00 USD
                        amount = float(v.split(" ")[1])
                    elif v.startswith("Quantity:"): #Quantity: 1
                        quantity = int(v.split(" ")[1])
                    else:
                        options.append(v.split(": ")[1])
                if amount != 0:
                    save_order_item(conn, order_id, name, ",".join(options), amount, quantity)
    return total

def process_order_group2(conn, order_id, data):
    name_col = 2
    options_col = 3
    price_col = 4
    quantity_col = 5
    offset = 0
    store = ''
    for line in (data+" ").split("\n"): 
        if line and not line.startswith("0: #,"): #0: #, 1: 品項, 2: 配料, 3: 單價, 4: 數量, 5: 總價
            results = [item.split(": ") for item in line.split(", ")] #0: 1, 1: 手工豆花, 2: 芋圓, 3: 5.50, 4: 2, 5: 11
            total_col = len(results) - 1 #update to support tax
            if total_col < 7: #without the store name
                offset = -1
            #print(results)
            name = results[name_col + offset][1].strip()
            if results[total_col][1] and name: #0: 11, 1: , 2: , 3: , 4: 飲料Total, 5: 11
                if offset == 0:
                    store = results[1][1]
                options = results[options_col + offset][1].strip()
                price = float(results[price_col + offset][1].strip())
                quantity = int(results[quantity_col + offset][1].strip())
                save_order_item(conn, order_id, name, options, price, quantity, store)
            elif results[total_col-1][1] == "飲料Total":                
                total = results[total_col][1]
    if total:
        return float(total)
    return 0

def process_order(conn, order):
    date = order['Submission Date']
    if not date.strip():
        return
    print(date)
    order_sql = "insert into orders (order_date, name, email, phone, submission_id, payment, comment, delivery_date, location) values (?,?,?,?,?,?,?,?,?)"
    delivery = order['Delivery Date'].split(" ")
    order_array = [order['Submission Date'], order['First Name'] + " " + order['Last Name'], order['Email'], order['Phone Number'], order['Submission ID'], order['Payment'], order['Comments'], delivery[0], delivery[1] ]
    cur = conn.cursor()
    cur.execute(order_sql, order_array)
    cur.execute("select last_insert_rowid()")
    order_id = cur.fetchone()[0]
    print("orderid: %d, name: %s %s" % (order_id, order['First Name'], order['Last Name']))
    conn.commit()
    total = process_order_group1(conn, order_id, order['食物: Products'])
    total += process_order_group2(conn, order_id, order['冰品飲料'])
    print("total: %f" % total)
    update_sql = "update orders set total=? where id=?"
    cur.execute(update_sql, (total, order_id))
    conn.commit()

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def execute_sql(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        #c.commit()
    except Error as e:
        print(e)

def init_db(retrieve_records):
    database = r"bien.db"

    sql_cleanup = "DELETE FROM orders; DELETE FROM order_items;"
    sql_drop1 = "DROP TABLE IF EXISTS orders;"
    sql_drop2 = "DROP TABLE IF EXISTS order_items;"
    sql_create_orders_table = """ CREATE TABLE IF NOT EXISTS orders (
                                        id integer PRIMARY KEY,
                                        order_date datetime NOT NULL,
                                        delivery_date text,
                                        location text,
                                        name text NOT NULL,
                                        email text,
                                        phone text,
                                        submission_id text,
                                        payment text,
                                        total numeric,
                                        comment text
                                    ); """

    sql_create_order_items_table = """CREATE TABLE IF NOT EXISTS order_items (
                                    id integer PRIMARY KEY,
                                    order_id integer,
                                    store text,
                                    product_name text,
                                    product_options text,
                                    product_unit_price numeric NOT NULL,
                                    quantity integer NOT NULL,
                                    FOREIGN KEY (order_id) REFERENCES orders (id)
                                );"""

    # create a database connection
    conn = create_connection(database)

    # create tables
    if retrieve_records:
        if conn:
            execute_sql(conn, sql_drop1)
            execute_sql(conn, sql_drop2)
            execute_sql(conn, sql_create_orders_table)
            execute_sql(conn, sql_create_order_items_table)
        else:
            print("Error! cannot create the database connection.")
    
    return conn

def init_products(conn, products, categories):
    sql_drop1 = "DROP TABLE IF EXISTS products;"
    sql_drop2 = "DROP TABLE IF EXISTS product_categories;"
    sql_create_orders_table = """ CREATE TABLE IF NOT EXISTS products (
                                        id integer PRIMARY KEY,
                                        category_id integer,
                                        name text NOT NULL,
                                        options text,
                                        price numeric,
                                        comment text,
                                        FOREIGN KEY (category_id) REFERENCES product_categories (id)
                                    ); """

    sql_create_order_items_table = """CREATE TABLE IF NOT EXISTS product_categories (
                                    id integer PRIMARY KEY,
                                    name text,
                                    tax integer
                                );"""
    stores = []
    if conn:
        execute_sql(conn, sql_drop1)
        execute_sql(conn, sql_drop2)
        execute_sql(conn, sql_create_orders_table)
        execute_sql(conn, sql_create_order_items_table)
        insert_sql = "insert into product_categories (id, name, tax) values (?,?,?)"
        cur = conn.cursor()
        for item in categories:
            cur.execute(insert_sql, (item['id'], item['name'], item['tax']))
            stores.append(item['name'])
        insert_sql = "insert into products (category_id, name, options, price) values (?,?,?,?)"
        for item in products:
            cur.execute(insert_sql, (item['category_id'], item['name'], item['options'], item['price']))
    else:
        print("Error! cannot create the database connection.")
    return stores

def add_header_store(store):
    row_num = 1
    store.cells.append(gspread.Cell(row_num, 1, 'Store'))
    store.cells.append(gspread.Cell(row_num, 2, store.name))
    store.cells.append(gspread.Cell(row_num, 3, store.delivery_date))
    fmt = cellFormat(
    backgroundColor=Color.fromHex('#ffff00'),
    textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0)),
    horizontalAlignment='CENTER'
    )
    #format_cell_range(sheet, "A%d:C%d" % (row_num, row_num), fmt)
    store.append_format("A%d:C%d" % (row_num, row_num), fmt)

    row_num += 1
    store.cells.append(gspread.Cell(row_num, 1, "Product"))
    store.cells.append(gspread.Cell(row_num, 2, "Options"))
    store.cells.append(gspread.Cell(row_num, 3, "Quantity"))
    store.cells.append(gspread.Cell(row_num, 4, "Price"))
    store.cells.append(gspread.Cell(row_num, 5, "Subtotal"))
    fmt = cellFormat(
    backgroundColor=Color.fromHex('#ccffcc'),
    textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0)),
    horizontalAlignment='CENTER'
    )
    #format_cell_range(sheet, "A%d:E%d" % (row_num, row_num), fmt)
    store.append_format("A%d:E%d" % (row_num, row_num), fmt)
    return row_num

def add_header_user(store, row_num, location, customer, comment):
    row_num += 1
    store.cells.append(gspread.Cell(row_num, 1, location))
    store.cells.append(gspread.Cell(row_num, 2, customer))
    store.cells.append(gspread.Cell(row_num, 3, comment))
    fmt = cellFormat(
    backgroundColor=Color.fromHex('#ffff00'),
    textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0)),
    horizontalAlignment='CENTER',
    verticalAlignment='MIDDLE'
    )
    #format_cell_range(sheet, "A%d:B%d" % (row_num, row_num), fmt)
    store.append_format("A%d:B%d" % (row_num, row_num), fmt)

    row_num += 1
    store.cells.append(gspread.Cell(row_num, 1, "Product"))
    store.cells.append(gspread.Cell(row_num, 2, "Options"))
    store.cells.append(gspread.Cell(row_num, 3, "Quantity"))
    store.cells.append(gspread.Cell(row_num, 4, "Total Items"))
    #cells.append(gspread.Cell(row_num, 5, "Commnet"))
    fmt = cellFormat(
    #backgroundColor=color(int('CC',16), 255, int('CC',16)),
    backgroundColor=Color.fromHex('#ccffcc'),
    textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0)),
    horizontalAlignment='CENTER'
    )
    #format_cell_range(sheet, "A%d:E%d" % (row_num, row_num), fmt)
    store.append_format("A%d:E%d" % (row_num, row_num), fmt)
    return row_num

def close_store(store, row_num, store_total):
    store.cells.append(gspread.Cell(row_num, 4, "Store Total:"))
    store.cells.append(gspread.Cell(row_num, 5, store_total))
    fmt = cellFormat(
    backgroundColor=Color.fromHex('#ffff00'),
    textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0))
    )
    #format_cell_range(store_sheet, "D%d:E%d" % (row_num, row_num), fmt)
    store.append_format("D%d:E%d" % (row_num, row_num), fmt)

    #store_sheet.update_cells(store_cells, value_input_option="USER_ENTERED")
        
def calc_total(qty, price, tax):
    tax_rate = 0.08875
    return round(price * qty * (1 + tax_rate* (tax or 0)), 2)

def analyze_store(conn, wk, delivery_date, store_name):
    
    sql ="""select i.product_name as product, i.product_options as options, sum(i.quantity) as quantity, i.product_unit_price as price, c.tax as tax
            from order_items i
            left join products p
            on i.product_name = p.name and i.product_options = p.options
            left join product_categories c
            on p.category_id = c.id
            join orders o
            on i.order_id = o.id
            where o.delivery_date = ? and c.name = ?
            group by c.name, i.product_name, i.product_options, i.product_unit_price
            order by c.id, p.id
        """
    print("Analyzing %s orders..." % (store_name))
    cur = conn.cursor()
    cur.execute(sql, (delivery_date, store_name))
    rows = cur.fetchall()
    if len(rows) == 0:
        return
    
    store = StoreData(store_name)
    #store_cells = []
    store_total = 0
    #store_sheet = get_sheet(wk, store_name + " " + delivery_date)
    store.create_sheet(wk, delivery_date)

    row_num = add_header_store(store) + 1

    for i, row in enumerate(rows):
        for j, col in enumerate(row):
            #print(row)
            value = row[j]
            if j == 2:
                qty = value
            elif j == 3:
                price = value
            elif j== 4: #tax
                subtotal = calc_total(qty, price, value)
                value = subtotal
            store.cells.append(gspread.Cell(row_num, j+1, value))            
        store_total += subtotal
        row_num += 1

    close_store(store, row_num, store_total)

    sql ="""select o.location, o.name, i.product_name as product, i.product_options as options, i.quantity as quantity, o.comment
        from order_items i
        left join orders o
        on i.order_id = o.id
        left join products p
        on i.product_name = p.name and i.product_options = p.options
        left join product_categories c
        on p.category_id = c.id
        where o.delivery_date = ? and c.name = ?   
        order by o.location, o.name, p.id
    """
    cur = conn.cursor()
    cur.execute(sql, (delivery_date, store_name))
    rows = cur.fetchall()
    
    #store_cells = []
    row_num += 2
    #add_header_user(store_sheet, row_num, store_cells)
    current_name=None
    total_items = 0
    save_total = None
    row_num += 1
    for row in rows:
        #print(row)
        location = row[0]
        customer = row[1]
        comment = row[5]                        
        if not current_name:
            current_name = customer
            row_num = add_header_user(store, row_num, location, customer, comment) + 1
        elif current_name != customer:
            store.cells.append(gspread.Cell(row_num-1, 4, total_items))
            row_num = add_header_user(store, row_num, location, customer, comment) + 1
            save_total = total_items
            total_items = 0
            current_name = customer
        total_items += int(row[4])
        store.cells.append(gspread.Cell(row_num, 1, row[2]))
        store.cells.append(gspread.Cell(row_num, 2, row[3]))
        store.cells.append(gspread.Cell(row_num, 3, row[4]))
        row_num += 1

    store.cells.append(gspread.Cell(row_num-1, 4, total_items))
    #store_sheet.update_cells(store_cells)
    store.submit()


def add_header_user2(store, row_num, location, customer, comment):
    row_num += 1
    store.append_cell(row_num, 1, location)
    store.append_cell(row_num, 2, customer)
    store.append_cell(row_num, 3, comment)
    fmt = cellFormat(
    backgroundColor=Color.fromHex('#ffff00'),
    textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0)),
    horizontalAlignment='CENTER',
    verticalAlignment='MIDDLE'
    )
    store.append_format("A%d:B%d" % (row_num, row_num), fmt)
    row_num += 1
    store.append_cell(row_num, 1, "Store")
    store.append_cell(row_num, 2, "Product")
    store.append_cell(row_num, 3, "Options")
    store.append_cell(row_num, 4, "Price")
    store.append_cell(row_num, 5, "Quantity")
    store.append_cell(row_num, 6, "Total")
    fmt = cellFormat(
    #backgroundColor=color(int('CC',16), 255, int('CC',16)),
    backgroundColor=Color.fromHex('#ccffcc'),
    textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0)),
    horizontalAlignment='CENTER'
    )
    store.append_format("A%d:F%d" % (row_num, row_num), fmt)
    return row_num

def dollar(amount):
    return "${:.2f}".format(amount)

def analyze_customers(conn, wk, delivery_date):
    sql ="""select o.location, o.name, c.name as store, i.product_name as product, i.product_options as options, i.product_unit_price as price, i.quantity as quantity, c.tax, o.comment
        from order_items i
        left join orders o
        on i.order_id = o.id
        left join products p
        on i.product_name = p.name and i.product_options = p.options
        left join product_categories c
        on p.category_id = c.id
        where o.delivery_date = ?
        order by o.location, o.name, c.id, p.id
    """
    
    print("Analyzing customers (%s)..." % (delivery_date))
    cur = conn.cursor()
    cur.execute(sql, (delivery_date, ))
    rows = cur.fetchall()
    if len(rows) == 0:
        return
    store = StoreData("人客")
    store.create_sheet(wk, delivery_date)
    current_name=None
    customer_total = total_items = 0
    row_num = 0
    for i, row in enumerate(rows):
        #print(row)
        location = row[0]
        customer = row[1]
        price = row[5]
        quantity = row[6]
        comment = row[8]
        if not current_name:
            current_name = customer
            row_num = add_header_user2(store, row_num, location, customer, comment) + 1
        elif current_name != customer:
            store.append_cell(row_num, 5, total_items)
            store.append_cell(row_num, 6, dollar(customer_total))
            row_num = add_header_user2(store, row_num, location, customer, comment) + 1
            customer_total = total_items = 0
            current_name = customer

        total_items += int(quantity)
        store.append_cell(row_num, 1, row[2])
        store.append_cell(row_num, 2, row[3])
        store.append_cell(row_num, 3, row[4])
        store.append_cell(row_num, 4, price)
        store.append_cell(row_num, 5, quantity)
        total = calc_total(quantity, price, row[7])
        store.append_cell(row_num, 6, dollar(total))
        row_num += 1
        customer_total += total
    store.append_cell(row_num, 5, total_items)
    store.append_cell(row_num, 6, dollar(customer_total))
    store.submit()

def get_sheet(wk, name, clean = True):
    result = None
    for sheet in wk.worksheets():
        if sheet.title == name:
            result = sheet
    
    if result:
        #wk.values_clear("'%s'!A1:J200" % name)
        #result.clear()
        #batchupdate will cause Quota exceeded for quota metric 'Write requests' and limit 'Write requests per minute per user' of service 
        if clean:
            requests = {"requests": [{"updateCells": {"range": {"sheetId": result._properties['sheetId']}, "fields": "*"}}]}
            wk.batch_update(requests)
        #wk.del_worksheet(result)
    else:
        result = wk.add_worksheet(name, 200, 20, 0)

    return result

def main():
    retrieve_records = False
    conn = init_db(retrieve_records)
    process_spreadsheet(conn, retrieve_records)
    if conn:
        conn.commit()
        conn.close()

if __name__ == "__main__":
    main()