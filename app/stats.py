import requests
import json

class data_stats:
    def __init__(self,token,cnn,cursor):
        
        self.token = token
        self.cnn = cnn
        self.cursor = cursor
        pass

    def stats_api(self):
        list_endpoints = [
                'https://rawpack-bars.myshopify.com/admin/api/2022-04/collects.json',
                'https://rawpack-bars.myshopify.com/admin/api/2022-04/products.json',
                'https://rawpack-bars.myshopify.com/admin/api/2022-04/customers.json',
                'https://rawpack-bars.myshopify.com/admin/api/2022-04/orders.json'
                ]
        list_status = []
        list_class_html = []
        for endpoint in list_endpoints:
            headers = {
                'X-Shopify-Access-Token': self.token,
                'Content-Type': 'application/json'
            }

            request_API = requests.get(endpoint, headers=headers)
            list_status.append(request_API.status_code)
            if request_API.status_code == 200:
                class_html = '"badge bg-success"'
            elif request_API.status_code in [400,401,400]:
                class_html = '"badge bg-danger"'
            else:
                class_html = '"badge bg-warning text-dark"'
            list_class_html.append(class_html)
            
        resp = """{
                "data": [
                    {
                    "table": "Collects",
                    "endpoint": "https://rawpack-bars.myshopify.com/admin/api/2022-04/collects.json",
                    "status_code": %s,
                    "html_class": %s
                    },
                    {
                    "table": "Products",
                    "endpoint": "https://rawpack-bars.myshopify.com/admin/api/2022-04/products.json",
                    "status_code": %s,
                    "html_class":%s
                    },
                    {
                    "table": "Customers",
                    "endpoint": "https://rawpack-bars.myshopify.com/admin/api/2022-04/customers.json",
                    "status_code": %s,
                    "html_class":%s
                    },     
                    {
                    "table": "Orders",
                    "endpoint": "https://rawpack-bars.myshopify.com/admin/api/2022-04/orders.json",
                    "status_code": %s,
                    "html_class": %s
                    }                             
                ],
                "count":4
            }""" %(list_status[0],list_class_html[0],list_status[1],list_class_html[1],list_status[2],list_class_html[2],list_status[3],list_class_html[3])

        return json.loads(resp)

    def stats_runapp(self):
        query = """
                select *,
                    case status
                        when 'Concluído' then 'badge bg-success'
                        when 'Processsando' then 'badge bg-primary'
                        when 'Erro' then 'badge bg-danger'
                        else 'badge bg-secondary'
                    end class_html 
                from log_app_bi.stats_sessions 
                """
        self.cursor.execute(query)
        resul = self.cursor.fetchall()
        return resul
