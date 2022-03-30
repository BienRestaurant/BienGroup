#pip install gspread
#pip install oauth2client
#pip install gspread_formatting
import gspread
#import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import json
import sqlite3
from sqlite3 import Error
from gspread_formatting import *
import requests
import os
from dataclasses import dataclass
from datetime import datetime

ALL = "ALL"
@dataclass
class Order:
    name: str
    id: int = 0
    order_uid: int = 0
    order_date: str = None
    submission_id: str = None
    email: str = None
    phone: str = None
    comment: str = None
    payment: str = None
    delivery_date: str = None
    location: str = None
    delivery_address: str = None

    def __init__(self, name):
        self.name = name
    
    def set_delivery_info(self, delivery_str):
        self.delivery_date, self.location = process_delivery(delivery_str)
    
    def set_from_jotform(self, order):
        self.order_date = order['Submission Date']
        self.order_uid = order['Order ID']
        self.name = order['First Name'] + " " + order['Last Name']
        self.email = order['Email']
        self.phone = order['Phone Number']
        self.submission_id = order['Submission ID']
        self.payment = order['Payment']
        self.comment = order['Comments']
        self.set_delivery_info(order['Delivery Date'])
        self.delivery_address = order['Delivery Address']

@dataclass
class OrderItem:
    order_id: int
    store: str = None
    store_id: int = 0
    product: str = None
    options: str = None
    quantity: int = 0
    price: float = 0.0
    order: Order = None

@dataclass
class Store:
    id: int
    name: str
    alias: str = None
    tax: int = None

    @classmethod
    def fromCsv(cls, item):
        return cls(int(item['id']), item['name'].strip(), item['alias'].strip(), int(item['tax']))

@dataclass
class Product:
    store_id: int
    name: str
    options: str = None
    price: float = None
    cost: float = None
    store: Store = None

    @classmethod
    def fromCsv(cls, item):
        print(item)
        price = item['price'] or 0
        cost = item['cost'] or price
        return cls(int(item['store_id']), item['name'].strip(), item['options'].strip(), float(price), float(cost))

class Database:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = None
        
    def create_connection(self):
        try:
            self.conn = sqlite3.connect(self.db_file)
            return self.conn
        except Error as e:
            print(e)

        return self.conn
    
    def close(self):
        self.conn.commit()
        self.conn.close()

    def execute_sql(self, sql):
        try:
            c = self.conn.cursor()
            c.execute(sql)
            #c.commit()
        except Error as e:
            print(e)

    def init_db(self, retrieve_records):
        #database = r"bien.db"

        sql_cleanup = "DELETE FROM orders; DELETE FROM order_items;"
        sql_drop1 = "DROP TABLE IF EXISTS orders;"
        sql_drop2 = "DROP TABLE IF EXISTS order_items;"
        sql_create_orders_table = """ CREATE TABLE IF NOT EXISTS orders (
                                            id integer PRIMARY KEY,
                                            order_date datetime NOT NULL,
                                            order_uid text,
                                            delivery_date text,
                                            delivery_address text,
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
                                        store_id, integer,
                                        product_name text,
                                        product_options text,
                                        product_unit_price numeric NOT NULL,
                                        quantity integer NOT NULL,
                                        FOREIGN KEY (order_id) REFERENCES orders (id)
                                    );"""

        # create a database connection
        conn = self.create_connection()

        # create tables
        if retrieve_records:
            if conn:
                self.execute_sql(sql_drop1)
                self.execute_sql(sql_drop2)
                self.execute_sql(sql_create_orders_table)
                self.execute_sql(sql_create_order_items_table)
            else:
                print("Error! cannot create the database connection.")
        
        return conn

    def init_products(self, products_csv, stores_csv):
        sql_drop1 = "DROP TABLE IF EXISTS products;"
        sql_drop2 = "DROP TABLE IF EXISTS stores;"
        sql_create_orders_table = """ CREATE TABLE IF NOT EXISTS products (
                                            id integer PRIMARY KEY,
                                            store_id integer,
                                            name text NOT NULL,
                                            options text,
                                            price numeric,
                                            cost numeric,
                                            comment text,
                                            FOREIGN KEY (store_id) REFERENCES stores (id)
                                        ); """

        sql_create_order_items_table = """CREATE TABLE IF NOT EXISTS stores (
                                        id integer PRIMARY KEY,
                                        name text,
                                        alias text,
                                        tax integer
                                    );"""
        self.execute_sql(sql_drop1)
        self.execute_sql(sql_drop2)
        self.execute_sql(sql_create_orders_table)
        self.execute_sql(sql_create_order_items_table)

        insert_sql = "insert into stores (id, name, alias, tax) values (?,?,?,?)"
        cur = self.conn.cursor()
        stores = []
        for item in stores_csv:
            store = Store.fromCsv(item)
            cur.execute(insert_sql, (store.id, store.name, store.alias, store.tax))
            stores.append(store)
        self.update_store_lookup(stores)

        insert_sql = "insert into products (store_id, name, options, price, cost) values (?,?,?,?,?)"
        products = []
        for item in products_csv:
            p = Product.fromCsv(item)
            store = self.store_lookup_by_id.get(p.store_id)
            if store:
                cur.execute(insert_sql, (p.store_id, p.name, p.options, p.price, p.cost))
                products.append(p)
                p.store = store

        self.update_product_lookup(products)
        return stores

    def update_store_lookup(self, stores):
        self.store_lookup = {}
        self.store_lookup_by_id = {}
        for store in stores:
            self.store_lookup[store.name.lower()] = store
            self.store_lookup_by_id[store.id] = store
            if store.alias:
                for a in store.alias.split(","):
                    self.store_lookup[a.lower()] = store
    
    def update_product_lookup(self, products):
        self.product_lookup = {}
        self.product_lookup2 = {}
        self.product_lookup3 = {}
        for product in products:
            #print("creating lookup %s = %d" % (product.name, product.store_id))
            self.product_lookup[product.name] = product
            if product.name not in self.product_lookup3:
                self.product_lookup3[product.name] = []
            self.product_lookup3[product.name].append(product)            
            self.product_lookup2[(product.name, product.options.lower())] = product
    
    def find_cost(self, store_name, name, option, price):
        p = self.product_lookup2.get((name, option.lower()))
        if p is None:
            product_list = self.product_lookup3.get(name)
            if product_list:
                for product in product_list:
                    if product.options.lower() in option.lower():
                        p = product
                        break
        if p:
            print("Found %s, %s price=%f" %( name, option, p.cost))
            return p.cost
        return price

    def get_store_by_name(self, name):
        s = self.store_lookup.get(name.lower())
        return s

    def get_store_id_by_name(self, name):
        s = self.get_store_by_name(name)
        if s:
            return s.id
        return None
    
    def get_store_by_product(self, name):
        p = self.product_lookup.get(name)
        if p:
            return self.store_lookup_by_id[p.store_id]
        if name != "**配送地點**":
            print(name +" is not found.")
        return None

    def get_store_id_by_product(self, name):
        p = self.product_lookup.get(name)
        if p:
            return p.store_id
        if name != "**配送地點**":
            print(name +" is not found.")
        return None

    def query_store(self, store, delivery_date):
        sql ="""select i.product_name as product, i.product_options as options, sum(i.quantity) as quantity, i.product_unit_price as price, c.tax as tax
            from order_items i
            left join stores c
            on i.store_id = c.id
            join orders o
            on i.order_id = o.id
            where o.delivery_date = ? and c.name = ?
            group by c.name, i.product_name, i.product_options, i.product_unit_price
            order by c.id, i.product_name
        """
        print("Analyzing %s(id:%d)orders..." % (store.name, store.id))
        cur = self.conn.cursor()
        cur.execute(sql, (delivery_date, store.name))
        rows = cur.fetchall()
        return rows

    def query_store_customers(self, store_name, delivery_date):
        sql ="""select o.id, o.location, o.name, o.comment, o.order_uid, i.product_name as product, i.product_options as options, i.quantity as quantity
        from order_items i
        left join orders o
        on i.order_id = o.id
        left join stores c
        on i.store_id = c.id
        where o.delivery_date = ? and c.name = ?   
        order by o.location, o.order_uid, i.product_name
        """
        cur = self.conn.cursor()
        cur.execute(sql, (delivery_date, store_name))
        items = []
        for row in cur.fetchall():
            order = Order(row[2])
            order.id = row[0]
            order.location = row[1]
            order.comment = row[3]
            order.order_uid = row[4]
            item = OrderItem(order.id)
            item.order = order
            item.product = row[5]
            item.options = row[6]
            item.quantity = row[7]
            items.append(item)
        return items

    def query_customers(self, delivery_date):
        sql ="""select o.location, o.order_uid, o.name, c.name as store, i.product_name as product, i.product_options as options, i.product_unit_price as price, i.quantity as quantity, c.tax, o.comment, o.delivery_address as address, o.email, o.phone
            from order_items i
            left join orders o
            on i.order_id = o.id
            left join stores c
            on i.store_id = c.id
            where o.delivery_date = ?
            order by o.location, o.order_uid, o.name, c.id, i.product_name
        """        
        print("Analyzing customers (%s)..." % (delivery_date))
        cur = self.conn.cursor()
        cur.execute(sql, (delivery_date, ))
        return cur.fetchall()

    def save_order(self, order):
        order_sql = "insert into orders (order_date, order_uid, name, email, phone, submission_id, payment, comment, delivery_date, location, delivery_address) values (?,?,?,?,?,?,?,?,?,?,?)"
        cur = self.conn.cursor()
        cur.execute(order_sql, [order.order_date, order.order_uid, order.name, order.email, order.phone, order.submission_id, order.payment, order.comment, order.delivery_date, order.location, order.delivery_address])
        cur.execute("select last_insert_rowid()")
        order.id = cur.fetchone()[0]
        print("orderid: %s(%d), name: %s" % (order.order_uid, order.id, order.name))
        self.conn.commit()
        return order.id

    def save_order_item1(self, item):
        print("Saving order %s(%d) %s, %s, %f, %d" % (item.store, item.store_id, item.product, item.options, item.price, item.quantity))
        order_sql = "insert into order_items (order_id, store, store_id, product_name, product_options, product_unit_price, quantity) values (?,?,?,?,?,?,?)"
        cur = self.conn.cursor()
        cur.execute(order_sql, (item.order_id, item.store, item.store_id, item.product, item.options, item.price, item.quantity))

    def update_total(self, order_id, total):
        update_sql = "update orders set total=? where id=?"
        self.conn.cursor().execute(update_sql, (total, order_id))
        self.conn.commit()

    def save_order_item(self, order_id, name, store_id, options, amount, quantity, store=None):
        print("Saving order %s %s, %s, %f, %d" % (store, name, options, amount, quantity))
        order_sql = "insert into order_items (order_id, store, store_id, product_name, product_options, product_unit_price, quantity) values (?,?,?,?,?,?,?)"
        cur = self.conn.cursor()
        cur.execute(order_sql, (order_id, store, store_id, name, options, amount, quantity))

class StoreData:
    def __init__(self, name):
        self.name = name
        self.cells = []
        self.formats = []

    def create_sheet(self, worksheet, delivery_date):
        self.delivery_date = delivery_date
        self.file_name = self.name + " " + delivery_date
        self.worksheet = worksheet
        self.worksheet_id = worksheet.id
        self.sheet = get_sheet(worksheet, self.file_name)

    def get_link(self):
        url_format = "https://spreadsheets.google.com/feeds/download/spreadsheets/Export?key=%s&exportFormat=%s&gid=%i"
        #url_format = "https://docs.google.com/spreadsheets/d/%s/gviz/tq?tqx=out:%s&gid=%s"
        #return url_format % ("15x2NG_HhyVZCnzcVkjq1WCx20Zo4TvRDSLS3ASsaxbA", "pdf", self.sheet.id)
        #return url_format % ("18czRzyYOEdMO6IXpatYRk3DWcBUAJyC_iEZDG5nlgys", "pdf", self.sheet.id)
        return url_format % (self.worksheet_id, "pdf", self.sheet.id)

    def append_cell(self, row, col, value):
        self.cells.append(gspread.Cell(row, col, value))

    def append_format(self, range, fmt):
        self.formats.append((range, fmt))

    def adjust_columns(self, auto):
        sheetId = self.sheet._properties['sheetId']
        if (auto):
            body = {
                "requests": [
                    {
                        "autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": sheetId,
                                "dimension": "COLUMNS",
                                "startIndex": 0,  # Please set the column index.
                                "endIndex": 2  # Please set the column index.
                            }
                        }
                    }
                ]
            }
        else:
            body = {
                "requests": [
                    {
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": sheetId,
                                "dimension": "COLUMNS",
                                "startIndex": 0,  # Please set the column index.
                                "endIndex": 1  # Please set the column index.
                            },
                            "properties": {
                                "pixelSize": 140
                            },
                            "fields": "pixelSize"
                        }
                    }
                ]
            }
        res = self.worksheet.batch_update(body)

    def submit(self, auto_adjust = False):
        self.sheet.update_cells(self.cells)
        format_cell_ranges(self.sheet, self.formats)
        self.adjust_columns(auto_adjust)
        url = self.get_link()
        #print("exporting from: %s" % url)
        r = requests.get(url)
        home = os.path.expanduser("~")
        downloads = os.path.join(home, "Downloads")
        file_name = self.file_name.replace('/','_')
        file = os.path.join(downloads, file_name + '.pdf')
        with open(file, 'wb') as saveFile:
            saveFile.write(r.content)

def process_spreadsheet(db, retrieve_records, delivery_date, is_customer_only, current_group, resume_id):
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)

    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    wk = client.open("阿扁在威郡")
    store_csv = wk.worksheet("Stores").get_all_records()
    categories = [st for st in store_csv if current_group in st["group"] ]
    if retrieve_records:
        products = wk.worksheet("Products").get_all_records()
        stores = db.init_products(products, categories)
        #process_order_sheet(db, delivery_date, wk, "Orders")
        #process_order_sheet(db, delivery_date, wk, "Orders-A")
        #process_order_sheet(db, delivery_date, wk, "Orders-A1")
        #process_order_sheet(db, delivery_date, wk, "Order-B")
        #process_extra_order(db, delivery_date, wk)
        process_order_sheet(db, delivery_date, wk, "Orders-B1128")
    else:
        stores = [Store(cat['id'], cat['name']) for cat in categories]
    
    output_wk = client.open("阿扁美食團出菜")
    if not is_customer_only:
        for store in stores:
            if store.id >= resume_id:
                analyze_store(db, output_wk, delivery_date, store)

    client.session.close
    client = gspread.authorize(creds)
    output_wk = client.open("阿扁美食團出菜")
    analyze_customers(db, output_wk, delivery_date)

def process_order_sheet(db, delivery_date, work_sheet, sheet_name):
    sheet = work_sheet.worksheet(sheet_name)
    records = sheet.get_all_records()
    print("Processing %s..." % (sheet_name))
    for order in records:
        process_order(db, delivery_date, order)

def process_order(db, delivery_date, order):
    date = order['Submission Date']
    if not date.strip():
        return
    print(date)
    order_data = Order("")
    order_data.set_from_jotform(order)
    if delivery_date != ALL and delivery_date != order_data.delivery_date:
        return #skipping
    order_id = db.save_order(order_data)
    total = 0
    total = process_order_group1(db, order_id, order['食物: Products'])
    total += process_order_group2(db, order_id, order['冰品飲料'])
    print("total: %f" % total)
    db.update_total(order_id, total)

def process_order_group1(db, order_id, data):
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
                price = 0
                values = results[1][0:results[1].rindex(")")] #ount: 16.00 USD, Quantity: 1)
                for v in values.split(", "):
                    if v.startswith("ount:"): #ount: 16.00 USD
                        price = float(v.split(" ")[1])
                    elif v.startswith("Quantity:") or v.startswith("請填小費金額於下方"): #Quantity: 1
                        quantity = int(v.split(" ")[1])
                    else:
                        options.append(v.split(": ")[1])
                if price != 0:
                    store = db.get_store_by_product(name)
                    if store:
                        store_id = store.id
                        # if store.tax == 1:
                        #     price = round(price / 1.08875, 2)                    
                    else:
                        store_id = db.get_store_id_by_product(name)
                    db.save_order_item(order_id, name, store_id, ",".join(options), price, quantity)
    return total

def process_order_group2(db, order_id, data):
    name_col = 2
    options_col = 3
    price_col = 4
    quantity_col = 5
    offset = 0
    store = ''
    last_store_id = None
    for line in (data+" ").split("\n"): 
        if line and not line.startswith("0: #,"): #0: #, 1: 品項, 2: 配料, 3: 單價, 4: 數量, 5: 總價
            results = [item.split(": ") for item in line.split(", ")] #0: 1, 1: 手工豆花, 2: 芋圓, 3: 5.50, 4: 2, 5: 11
            total_col = len(results) - 1 #update to support tax
            if total_col < 7: #without the store name
                offset = -1
            #print(results)
            name = results[name_col + offset][1].strip()
            options = results[options_col + offset][1].strip()
            #user can just input options
            if results[total_col][1] and (name or options): #0: 11, 1: , 2: , 3: , 4: 飲料Total, 5: 11
                if offset == 0:
                    store = results[1][1].strip()
                if not results[price_col + offset][1]:
                    continue
                price = float(results[price_col + offset][1].strip())
                qty_str = results[quantity_col + offset][1].strip()
                if qty_str:
                    quantity = int(qty_str)
                    if store:
                        store_id = db.get_store_id_by_name(store)
                        last_store_id = store_id
                    else:
                        store_id = db.get_store_id_by_product(name)
                        if not store_id:
                            store_id = last_store_id
                    
                    db.save_order_item(order_id, name, store_id, options, price, quantity, store)
            elif results[total_col-1][1] == "飲料Total" or results[total_col-1][1] == "自填Total":                
                total = results[total_col][1]
    if total:
        return float(total)
    return 0

def process_delivery(delivery_str):
    delivery = delivery_str.split(" ")
    return delivery[0].strip(), (" ".join(delivery[1:])).strip()
def process_extra_order(db, delivery_date, wk):
    extra_order = wk.worksheet("ExtraOrders").get_all_records()
    for extra in extra_order:
        date, location = process_delivery(extra["Location"])
        if (delivery_date != date):
            return
        process_extra_order_sheet(db, wk, extra["Location"], extra["Sheet"])

def process_extra_order_sheet(db, wk, delivery_str, sheet_name):
    records = wk.worksheet(sheet_name).get_all_records()
    
    last_name = None
    for record in records:
        name = record["訂購人"]
        if not name:
            continue
        if last_name != name:
            order = Order(name)
            order.order_date = datetime.now()
            order.set_delivery_info(delivery_str)
            db.save_order(order)
            last_name = name
        item = OrderItem(order.id)
        item.store = record["店名"]
        store = db.get_store_by_name(item.store)
        if store:
            item.store_id = store.id

        item.product = record["菜名"]
        item.price = float(record["單價"].replace("$",""))
        # if store.tax == 1:
        #     item.price = round(item.price / 1.08875, 2)

        item.quantity = int(record["數量"])
        db.save_order_item1(item)
        
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

def add_header_user(store, row_num, order):
    row_num += 1
    store.cells.append(gspread.Cell(row_num, 1, order.location))
    store.cells.append(gspread.Cell(row_num, 2, order.order_uid))
    store.cells.append(gspread.Cell(row_num, 3, order.name))
    store.cells.append(gspread.Cell(row_num, 4, order.comment))
    fmt = cellFormat(
    backgroundColor=Color.fromHex('#ffff00'),
    textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0)),
    horizontalAlignment='CENTER',
    verticalAlignment='MIDDLE'
    )
    store.append_format("A%d:C%d" % (row_num, row_num), fmt)

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
    after_tax = round(price * qty * (1 + tax_rate* (tax or 0)), 2)
    return after_tax

def analyze_store(db, wk, delivery_date, store1):
    rows = db.query_store(store1, delivery_date)
    if len(rows) == 0:
        return
    
    store = StoreData(store1.name)
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
                price = db.find_cost(store1.name, row[0], row[1], value)
                value = price
            elif j== 4: #tax
                subtotal = calc_total(qty, price, value)
                value = subtotal
            store.cells.append(gspread.Cell(row_num, j+1, value))            
        store_total += subtotal
        row_num += 1

    close_store(store, row_num, store_total)

    items = db.query_store_customers(store1.name, delivery_date)    
    row_num += 2
    current_name=None
    total_items = 0
    row_num += 1
    for item in items:
        order = item.order
        if not current_name:
            current_name = order.name
            row_num = add_header_user(store, row_num, order) + 1
        elif current_name != order.name:
            store.cells.append(gspread.Cell(row_num-1, 4, total_items))
            row_num = add_header_user(store, row_num, order) + 1
            total_items = 0
            current_name = order.name
        total_items += int(item.quantity)
        store.cells.append(gspread.Cell(row_num, 1, item.product))
        store.cells.append(gspread.Cell(row_num, 2, item.options))
        store.cells.append(gspread.Cell(row_num, 3, item.quantity))
        row_num += 1

    store.cells.append(gspread.Cell(row_num-1, 4, total_items))
    store.submit(True)

def add_header_user2(index, store, row_num, location, order_uid, customer, comment, address, email, phone):
    row_num += 1
    store.append_cell(row_num, 1, "%s #%d" % (location, index))
    store.append_cell(row_num, 2, order_uid)
    store.append_cell(row_num, 3, customer)
    store.append_cell(row_num, 4, comment)
    fmt = cellFormat(
    backgroundColor=Color.fromHex('#ffff00'),
    textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0)),
    horizontalAlignment='CENTER',
    verticalAlignment='MIDDLE'
    )
    store.append_format("A%d:C%d" % (row_num, row_num), fmt)
    row_num += 1
    store.append_cell(row_num, 1, email)
    store.append_cell(row_num, 2, phone)
    store.append_cell(row_num, 3, address)
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

def analyze_customers(db, wk, delivery_date):
    rows = db.query_customers(delivery_date)
    if len(rows) == 0:
        return
    store = StoreData("人客")
    store.create_sheet(wk, delivery_date)
    current_name=None
    customer_total = total_items = 0
    row_num = 0
    index = 1
    for  row in rows:
        #print(row)
        location = row[0]
        order_uid = row[1]
        customer = row[2]
        price = row[6]
        quantity = row[7]
        comment = row[9]
        address = row[10]
        email = row[11]
        phone = row[12]
        if not current_name:
            current_name = customer
            row_num = add_header_user2(index, store, row_num, location, order_uid, customer, comment,address, email, phone) + 1
            index += 1
        elif current_name != customer:
            #store.append_cell(row_num, 5, total_items)
            #store.append_cell(row_num, 6, dollar(customer_total))
            row_num = close_customer(store, row_num, total_items, customer_total)
            row_num = add_header_user2(index, store, row_num, location, order_uid, customer, comment, address, email, phone) + 1
            index += 1
            customer_total = total_items = 0
            current_name = customer

        total_items += int(quantity)
        store.append_cell(row_num, 1, row[3])
        store.append_cell(row_num, 2, row[4])
        store.append_cell(row_num, 3, row[5])
        store.append_cell(row_num, 4, price)
        store.append_cell(row_num, 5, quantity)
        total = price * quantity
        store.append_cell(row_num, 6, dollar(total))
        row_num += 1
        customer_total += total
    close_customer(store, row_num, total_items, customer_total)
    store.submit(True)

def close_customer(store, row_num, total_items, customer_total):
    store.append_cell(row_num, 5, total_items)
    store.append_cell(row_num, 6, dollar(customer_total))
    row_num += 1
    store.append_cell(row_num, 5, "Handling")
    tax_amount = round(customer_total * 0.12, 2)
    store.append_cell(row_num, 6, tax_amount)
    store.append_cell(row_num, 7, customer_total + tax_amount)
    return row_num + 1


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
    retrieve_records = True
    date = "2/19"
    is_customer_only = True
    resume_id = 1
    current_group = 'B'
    #db = Database("/Users/lewis/Downloads/bien.db")
    db = Database("c:/Users/lewis/Downloads/bien.db")
    db.init_db(retrieve_records)
    process_spreadsheet(db, retrieve_records, date, is_customer_only, current_group, resume_id)
    if db:
        db.close()

if __name__ == "__main__":
    main()