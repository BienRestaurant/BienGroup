from jotform import *
import os
import json

def main():

    jotformAPIClient = JotformAPIClient('d9934a9dbbd21c11eeac90e78de29499')

    forms = jotformAPIClient.get_forms()

    for form in forms:
    	handle_form(jotformAPIClient, form)

# {'id': '211104877556155', 'username': 'lewilin', 'title': '阿扁私房菜', 'height': '0', 'status': 'ENABLED', 
# 'created_at': '2021-04-21 12:01:50', 'updated_at': '2021-06-10 18:33:15', 'last_submission': '2021-06-10 12:21:18', 
# 'new': '40', 'count': '100', 'type': 'LEGACY', 'url': 'https://form.jotform.com/211104877556155'}
def handle_form(client, form):
    id = form['id']
    #https://www.jotform.com/API/payment/stock/fetchall/211104877556155
    #list = client.get_product_list(id)
    #print("results:" + list)
    #result = client.get_form_properties(id)
    result = client.get_form_property(id, "conditions")
    print(result)
    home = os.path.expanduser("~")
    downloads = os.path.join(home, "Downloads")
    
    file = os.path.join(downloads, 'properties.json')
    with open(file, 'w') as saveFile:
        saveFile.write(json.dumps(result))

if __name__ == "__main__":
    main()