import pandas as pd
import requests 
import json
from datetime import datetime
import logging
from app import tracking
import pytz

UTC = pytz.utc
IST = pytz.timezone('Brazil/East')

class request_api: 

    def __init__(self, cnn, cursor, token, table):
        self.log = logging.getLogger(__name__)
        self.log.debug('Classe Request_API - Iniciada')

        self.cnn = cnn
        self.cursor = cursor
        self.token = token
        self.app_tracking = tracking.tracking_steps(cnn, cursor, table)
        self.app_tracking.post_session()
        self.id_session = self.app_tracking.id_session
        self.log.info('Dados Request API: token: %s - Session_ID = %s' %(token, self.id_session))
   
    def request_endpoint(self,endpoint, table):
        self.log.debug('Function %s: Iniciado' %('request_endpoint'))
        endpoint = endpoint
        headers = {
            'X-Shopify-Access-Token': self.token,
            'Content-Type': 'application/json'
        }
        request_API = requests.get(endpoint, headers=headers)
        self.log.debug('Function %s: Request realizado' %('request_endpoint'))

        if request_API.status_code == 200:
            self.log.debug('Function %s: STATUS CODE = 200' %('request_endpoint'))
            contentAPI = json.loads(request_API.content)
            headerAPI = request_API.headers
            df = pd.json_normalize(contentAPI[table])
            self.log.info('Function %s | Dados: id_session: %s - status_code: %s - len_df = %s' %('request_endpoint',self.id_session, request_API.status_code, len(df)))    
        else:
            self.log.debug('Function %s: STATUS CODE = %s' %('request_endpoint',request_API.status_code))
            contentAPI = None
            df = None
        self.log.debug('Function %s: Finalizado' %('request_endpoint'))
        return request_API, contentAPI, headerAPI, df

    def next_page(self, header):
        self.log.debug('Function %s: Iniciado' %('next_page'))
        if header.get('Link'):
            links = header['Link'].split(',')
            list_links = []
            for link in links:
                link = link.split(';')
                endpoint = link[0].replace('<', '').replace('>','')
                rel = link[1].replace('"', '').split('=')[1]
                list_links.append([endpoint, rel])

            if len(list_links) >1:
                if list_links[0][1] == 'next' or list_links[1][1] == 'next':
                    has_next_page = True
                    if list_links[0][1] == 'next':
                        endpoint = list_links[0][0]
                        rel = list_links[0][1] 
                        page_info = list_links[0][0].split('page_info=')[1]
                    else:
                        endpoint = list_links[1][0]
                        rel = list_links[1][1] 
                        page_info = list_links[1][0].split('page_info=')[1]
                else:
                    has_next_page = False
                    endpoint = None
                    page_info = None
            else:
                if list_links[0][1] == 'next':
                    endpoint = list_links[0][0]
                    rel = list_links[0][1] 
                    has_next_page = True
                    page_info = list_links[0][0].split('page_info=')[1]
                else:
                    has_next_page = False
                    endpoint = None
                    page_info = None
        else:
            has_next_page = False
            endpoint = None
            page_info = None
        self.log.info('Function: %s | Dados: id_session: %s - has_next_page: %s ' %('next_page',self.id_session, has_next_page))    
        self.log.debug('Function %s: Finalizado' %('next_page'))
        return has_next_page, endpoint, page_info 

    def clean_string(self,string):
        try:
            if string == None:
                str = None
            else:
                str = string.replace("'",'').replace('"', '')
        except:
            str = string     
        return str
    
    def request_collects(self,start_date):
        class_name = 'Request_API'
        module_name = 'request_collects'
        self.log.debug('Function %s: Iniciado' %(module_name))

        
        #<!---- Definição das Variaveis Endpoint Inicial -->
        endpoint = f'https://rawpack-bars.myshopify.com/admin/api/2022-04/collects.json?limit=250&updated_at_min={start_date}'
        count_page = 0
        status_code = 200
        rows_insert = 0
        rows_error = 0
        #<!---- Fim Definição das Variaveis Endpoint Inicial -->

        #<!---- Inicio do While de Requisição API --->
        while(status_code == 200):
            
            #<!-- Variaveis para Tracking da Conexão Endpoint -->
            start_datetime = datetime.now(IST)
            endpoint_name = 'collects'

            #<!-- Requisção Endpoint -->
            connect_to = self.request_endpoint(endpoint, 'collects')
            status_code = connect_to[0].status_code
            self.log.debug('Function %s: STATUS CODE = %s' %(module_name, status_code))
            
            #<!-- Executa se o endpoint tiver status == 200 -->
            if status_code == 200:
                
                #<!-- Variaveis para Tracking Final da Conexão Endpoint -->
                end_datetime = datetime.now(IST) 

                #<!-- Executa Metodo para gravar Log da execução Gravando Sucesso -->
                self.app_tracking.log_tracking_session(1,endpoint_name,count_page,start_datetime,end_datetime,1,0,None)
                self.log.debug('Function %s: Registrado Log tracking' %(module_name))  
                self.log.info('Function %s: | Dados: endpoint_name: %s | count_page: %s | start_datetime: %s | end_datetime: %s ' %(module_name, endpoint_name,count_page,start_datetime,end_datetime))

                #<!-- Coleta os dados do Enpoint -->
                collects = connect_to[3]
                header = connect_to[2]

                #<!-- Variável de Inicio do processo de importação -->
                start_datetime = datetime.now(IST)

                #<!-- Try para tratar error -->
                self.log.debug('Function %s: Inicio Processo importação de Base' %(module_name)) 
                try:
                    self.log.debug('Function %s: Entrou no TRY da importação de Base' %(module_name)) 
                    #<!-- Loop para importar Linha a Linha do DF -->
                    for index, row in collects.iterrows():
                        #<!-- Variaveis que serão importadas -->
                        id = row.id
                        collection_id = row.collection_id
                        product_id = row.product_id
                        created_at = row.created_at
                        updated_at = row.updated_at
                        position = row.position
                        #<!-- Fim Variaveis que serão importadas -->

                        #<!-- Instrução para Importação da Linha -->
                        try:
                            query = """insert Into stage.tb_collects values ('%s','%s','%s','%s','%s','%s',null,'%s')"""
                            query_log = query %(id, collection_id, product_id, created_at, updated_at, position, datetime.now(IST))
                            self.cursor.execute(query %(id, collection_id, product_id, created_at, updated_at, position, datetime.now(IST)))
                            rows_insert += 1
                        except:
                            self.log.error('Function %s: Query: %s' %(module_name, query_log))
                            rows_error += 1
                            pass
                        #<!-- Fim Instrução para Importação da Linha -->
                    
                    #<!-- Variaveis Variaveis do Final da importação -->
                    end_datetime = datetime.now(IST)

                    #<!-- Grava Log de Importação como Sucesso -->
                    self.app_tracking.log_tracking_session(2,endpoint_name,count_page,start_datetime,end_datetime,1,len(collects),None)
                except:
                    self.log.debug('Function %s: Erro Loop importação de Base' %(module_name))
                    #<!-- Em caso de erro na importação gravar erro -->
                    end_datetime = datetime.now(IST)
                    self.app_tracking.log_tracking_session(1,endpoint_name,count_page,start_datetime,end_datetime,0,len(collects),"Erro Importação")
                    pass
                
                self.log.debug('Function %s: FIM Processo importação de Base' %(module_name))

                #<!-- Commit do Banco -->
                self.cnn.commit()
                self.log.debug('Function %s: Commit Realizado' %(module_name))

                #<!-- Verifica se existe outra página -->
                next = self.next_page(header)
                               
                if next[0] == True:
                    endpoint = next[1]
                    page_info = next[2]
                    count_page += 1
                else:
                    break
        
            #<!-- Caso Status != 200 - Gravar Falha -->     
            else:
                self.log.debug('Function %s: STATUS CODE != 200' %(module_name))
                end_datetime = datetime.now(IST) 
                self.app_tracking.log_tracking_session(2,endpoint_name,count_page,start_datetime,end_datetime,0,0,status_code) 
        

        start_etl = datetime.now(IST)
        self.cursor.execute("call stage.etl_collects(%s)" %(self.id_session))
        end_etl = datetime.now(IST)
        self.app_tracking.log_tracking_session(3,endpoint_name,0,start_etl,end_etl,1,0,None)


        self.app_tracking.log_tracking_insert('tb_collects', rows_insert, rows_error)
        self.log.debug('Function %s: Finalizado' %(module_name))
        #<!---- Fim do While de Requisição API --->

    def request_products(self,start_date):
        module_name = 'request_products'
        self.log.debug('Function %s: Iniciado' %(module_name))
        
        #<!---- Definição das Variaveis Endpoint Inicial -->
        endpoint = f'https://rawpack-bars.myshopify.com/admin/api/2022-04/products.json?limit=250&updated_at_min={start_date}'
        count_page = 0
        status_code = 200
        # Contadores Principais
        rows_insert = 0
        rows_error = 0
        # Contadores Secundários
        rows_insert_2 = 0
        rows_error_2 = 0
        #<!---- Fim Definição das Variaveis Endpoint Inicial -->

        #<!---- Inicio do While de Requisição API --->
        while(status_code == 200):
            
            #<!-- Variaveis para Tracking da Conexão Endpoint -->
            start_datetime = datetime.now(IST)
            endpoint_name = 'products'

            #<!-- Requisção Endpoint -->
            connect_to = self.request_endpoint(endpoint, 'products')
            status_code = connect_to[0].status_code
            self.log.debug('Function %s: STATUS CODE = %s' %(module_name, status_code))
            
            #<!-- Executa se o endpoint tiver status == 200 -->
            if status_code == 200:
                
                #<!-- Variaveis para Tracking Final da Conexão Endpoint -->
                end_datetime = datetime.now(IST) 

                #<!-- Executa Metodo para gravar Log da execução Gravando Sucesso -->
                self.app_tracking.log_tracking_session(1,endpoint_name,count_page,start_datetime,end_datetime,1,0,None)
                self.log.debug('Function %s: Registrado Log tracking' %(module_name))  
                self.log.info('Function %s: | Dados: endpoint_name: %s | count_page: %s | start_datetime: %s | end_datetime: %s ' %(module_name, endpoint_name,count_page,start_datetime,end_datetime))

                #<!-- Coleta os dados do Enpoint -->
                products = connect_to[3]
                header = connect_to[2]

                #<!-- Variável de Inicio do processo de importação -->
                start_datetime = datetime.now(IST)

                #<!-- Try para tratar error -->
                self.log.debug('Function %s: Inicio Processo importação de Base' %(module_name)) 
                try:
                    self.log.debug('Function %s: Entrou no TRY da importação de Base' %(module_name)) 
                    #<!-- Loop para importar Linha a Linha do DF -->
                    for index, row in products.iterrows():
                        #<!-- Variaveis que serão importadas -->
                        id = row.id
                        title = self.clean_string(row.title)
                        vendor = self.clean_string(row.vendor)
                        product_type = self.clean_string(row.product_type)
                        created_at = row.created_at
                        handle = self.clean_string(row.handle)
                        updated_at = row.updated_at
                        published_at = row.published_at
                        template_suffix = self.clean_string(row.template_suffix)
                        status = row.status
                        published_scope = self.clean_string(row.published_scope)
                        tags = self.clean_string(row.tags)
                        #<!-- Fim Variaveis que serão importadas -->

                        #<!-- Instrução para Importação da Linha -->
                        try:
                            query = """ insert into stage.tb_products values (%s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"""
                            query_log = query %(id, title,vendor, product_type, created_at, handle, updated_at, published_at, template_suffix, status, published_scope, tags, datetime.now(IST))
                            self.cursor.execute(query %(id, title,vendor, product_type, created_at, handle, updated_at, published_at, template_suffix, 
                                    status, published_scope, tags, datetime.now(IST)))
                            rows_insert += 1
                        except:
                            self.log.error('Function %s: Query: %s' %(module_name, query_log))
                            rows_error += 1
                            pass
                        #<!-- Fim Instrução para Importação da Linha -->

                        #<!-- Inicio Sub Loop do For PAI -->
                        #<!-- Dataframe Sub -->    
                        df_variants = pd.json_normalize(row.variants)

                        for index, row in df_variants.iterrows():
                            #<!-- Inicio Variaveis que serão importadas -->
                            product_id = row.product_id 
                            id = row.id 
                            title  = self.clean_string(row.title)
                            price  = row.price 
                            sku  = row.sku  
                            position  = row.position  
                            inventory_policy  = row.inventory_policy  
                            compare_at_price  = row.compare_at_price if row.compare_at_price != None else 0
                            fulfillment_service  = row.fulfillment_service
                            inventory_management  = row.inventory_management
                            option1 = self.clean_string(row.option1)
                            option2 = self.clean_string(row.option2)
                            option3 = self.clean_string(row.option3)
                            created_at = row.created_at
                            updated_at = row.updated_at
                            taxable = row.taxable
                            barcode = row.barcode
                            grams = row.grams
                            image_id = row.image_id
                            weight= row.weight
                            weight_unit = row.weight_unit
                            inventory_item_id = row.inventory_item_id
                            inventory_quantity = row.inventory_quantity 
                            requires_shipping  = row.requires_shipping
                            #<!-- Fim Variaveis que serão importadas -->

                            #<!-- Instrução para Importação da Linha -->
                            try:
                                query = """ insert into stage.tb_products_variants values(%s, %s ,'%s',%s,'%s',%s,'%s',%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,'%s',%s,'%s',%s,%s,'%s', '%s') """
                                query_log = query %(product_id ,id, title, price,sku, position, inventory_policy, compare_at_price, 
                                    fulfillment_service	, inventory_management, option1, option2, option3, created_at, updated_at, 
                                    taxable, barcode, grams, image_id, weight, weight_unit, inventory_item_id, inventory_quantity, 
                                    requires_shipping, datetime.now(IST))
                                self.cursor.execute(query %(product_id ,id, title, price,sku, position, inventory_policy, compare_at_price, 
                                    fulfillment_service	, inventory_management, option1, option2, option3, created_at, updated_at, 
                                    taxable, barcode, grams, image_id, weight, weight_unit, inventory_item_id, inventory_quantity, 
                                    requires_shipping, datetime.now(IST)))
                                rows_insert_2 += 1
                            except:
                                self.log.error('Function %s: Query: %s' %(module_name, query_log))
                                rows_error_2 += 1
                                pass
                            #<!-- Fim Instrução para Importação da Linha -->
                        #<!-- Fim Sub Loop do For PAI -->

                    #<!-- Variaveis do Final da importação -->
                    end_datetime = datetime.now(IST)

                    #<!-- Grava Log de Importação como Sucesso -->
                    self.app_tracking.log_tracking_session(2,endpoint_name,count_page,start_datetime,end_datetime,1,len(products),None)
                except:
                    self.log.debug('Function %s: Erro Loop importação de Base' %(module_name))
                    #<!-- Em caso de erro na importação gravar erro -->
                    end_datetime = datetime.now(IST)
                    self.app_tracking.log_tracking_session(2,endpoint_name,count_page,start_datetime,end_datetime,0,len(products),"Erro Importação")
                    pass
                self.log.debug('Function %s: FIM Processo importação de Base' %(module_name))

                #<!-- Commit do Banco -->
                self.cnn.commit()
                self.log.debug('Function %s: Commit Realizado' %(module_name))

                #<!-- Verifica se existe outra página -->
                next = self.next_page(header)
                               
                if next[0] == True:
                    endpoint = next[1]
                    page_info = next[2]
                    count_page += 1
                else:
                    break
        
            #<!-- Caso Status != 200 - Gravar Falha -->     
            else:
                self.log.debug('Function %s: STATUS CODE != 200' %(module_name))
                end_datetime = datetime.now(IST) 
                self.app_tracking.log_tracking_session(1,endpoint_name,count_page,start_datetime,end_datetime,0,0,status_code)  

        
        start_etl = datetime.now(IST)
        self.cursor.execute("call stage.etl_products(%s)" %(self.id_session))
        self.cursor.execute("call stage.etl_products_variants(%s)" %(self.id_session))
        end_etl = datetime.now(IST)
        self.app_tracking.log_tracking_session(3,endpoint_name,0,start_etl,end_etl,1,0,None)



        self.app_tracking.log_tracking_insert('tb_products', rows_insert, rows_error)
        self.app_tracking.log_tracking_insert('tb_products_variants', rows_insert_2, rows_error_2)
        self.log.debug('Function %s: Finalizado' %(module_name))
        #<!---- Fim do While de Requisição API --->

    def request_customers(self,start_date):
        module_name = 'request_customers'
        self.log.debug('Function %s: Iniciado' %(module_name))
        
        #<!---- Definição das Variaveis Endpoint Inicial -->
        endpoint = f'https://rawpack-bars.myshopify.com/admin/api/2022-04/customers.json?limit=250&updated_at_min={start_date}'
        count_page = 0
        status_code = 200
        # Contadores Principais
        rows_insert = 0
        rows_error = 0
        # Contadores Secundários
        rows_insert_2 = 0
        rows_error_2 = 0
        #<!---- Fim Definição das Variaveis Endpoint Inicial -->

        #<!---- Inicio do While de Requisição API --->
        while(status_code == 200):
            
            #<!-- Variaveis para Tracking da Conexão Endpoint -->
            start_datetime = datetime.now(IST)
            endpoint_name = 'customers'

            #<!-- Requisção Endpoint -->
            connect_to = self.request_endpoint(endpoint, 'customers')
            status_code = connect_to[0].status_code
            self.log.debug('Function %s: STATUS CODE = %s' %(module_name, status_code))
            
            #<!-- Executa se o endpoint tiver status == 200 -->
            if status_code == 200:
                
                #<!-- Variaveis para Tracking Final da Conexão Endpoint -->
                end_datetime = datetime.now(IST) 

                #<!-- Executa Metodo para gravar Log da execução Gravando Sucesso -->
                self.app_tracking.log_tracking_session(1,endpoint_name,count_page,start_datetime,end_datetime,1,0,None)
                self.log.debug('Function %s: Registrado Log tracking' %(module_name))  
                self.log.info('Function %s: | Dados: endpoint_name: %s | count_page: %s | start_datetime: %s | end_datetime: %s ' %(module_name, endpoint_name,count_page,start_datetime,end_datetime))

                #<!-- Coleta os dados do Enpoint -->
                customers = connect_to[3]
                header = connect_to[2]

                #<!-- Variável de Inicio do processo de importação -->
                start_datetime = datetime.now(IST)

                #<!-- Try para tratar error -->
                self.log.debug('Function %s: Inicio Processo importação de Base' %(module_name)) 
                try:
                    self.log.debug('Function %s: Entrou no TRY da importação de Base' %(module_name)) 
                    #<!-- Loop para importar Linha a Linha do DF -->
                    for index, row in customers.iterrows():
                        #<!-- Variaveis que serão importadas -->
                        id = row.id
                        email = self.clean_string(row.email)
                        accepts_marketing = row.accepts_marketing
                        created_at = row.created_at
                        updated_at = row.updated_at
                        first_name = self.clean_string(row.first_name)
                        last_name = self.clean_string(row.last_name)
                        orders_count = row.orders_count
                        state = row.state
                        verified_email = row.verified_email
                        multipass_identifier = row.multipass_identifier
                        tax_exempt = row.tax_exempt
                        phone = row.phone
                        tags = row.tags
                        last_order_name = row.last_order_name
                        currency = row.currency
                        accepts_marketing_updated_at = row.accepts_marketing_updated_at
                        marketing_opt_in_level = row.marketing_opt_in_level
                        default_address_id = row['default_address.id']
                        #<!-- Fim Variaveis que serão importadas -->

                        #<!-- Instrução para Importação da Linha -->
                        try:
                            query_customers = """Insert into stage.tb_customers values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""
                            query_log_customers = query_customers %(id,email,accepts_marketing,created_at,updated_at,first_name,last_name,state,verified_email,multipass_identifier,tax_exempt,phone,tags,currency,accepts_marketing_updated_at,marketing_opt_in_level,default_address_id, datetime.now(IST))
                            self.cursor.execute(query_customers %(id,email,accepts_marketing,created_at,updated_at,first_name,last_name,state,verified_email,multipass_identifier,tax_exempt,phone,tags,currency,accepts_marketing_updated_at,marketing_opt_in_level,default_address_id, datetime.now(IST)))
                            rows_insert += 1
                        except:
                            self.log.error('Function %s: Query: %s' %(module_name, query_log_customers))
                            rows_error += 1
                            pass
                        #<!-- Fim Instrução para Importação da Linha -->

                        #<!-- Inicio Sub Loop do For PAI -->
                        #<!-- Dataframe Sub -->    
                        df_addresses = pd.json_normalize(row.addresses)

                        for index, row in df_addresses.iterrows():
                            #<!-- Inicio Variaveis que serão importadas -->
                            id = row.id
                            customer_id = row.customer_id
                            first_name = self.clean_string(row.first_name)
                            last_name = self.clean_string(row.last_name)
                            company = self.clean_string(row.company)
                            address1 = self.clean_string(row.address1)
                            address2 = self.clean_string(row.address2)
                            city = self.clean_string(row.city)
                            province = self.clean_string(row.province)
                            country = self.clean_string(row.country)
                            zip = self.clean_string(row.zip)
                            phone = self.clean_string(row.phone)
                            name = row.name
                            province_code = row.province_code
                            country_code = row.country_code
                            country_name = self.clean_string(row.country_name)
                            default_address = row.default
                            #<!-- Fim Variaveis que serão importadas -->

                            #<!-- Instrução para Importação da Linha -->
                            try:
                                query_c_addresses = """Insert into stage.tb_customer_addresses values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""
                                query_log_c_addresses = query_c_addresses %(id,customer_id,first_name,last_name,company,address1,address2,city,province,country,zip,phone,name,province_code,country_code,country_name,default_address, datetime.now(IST))
                                self.cursor.execute(query_c_addresses %(id,customer_id,first_name,last_name,company,address1,address2,city,province,country,zip,phone,name,province_code,country_code,country_name,default_address, datetime.now(IST)))
                                rows_insert_2 += 1
                            except:
                                self.log.error('Function %s: Query: %s' %(module_name, query_log_c_addresses))
                                rows_error_2 += 1
                                pass
                            #<!-- Fim Instrução para Importação da Linha -->
                        #<!-- Fim Sub Loop do For PAI -->

                    #<!-- Variaveis do Final da importação -->
                    end_datetime = datetime.now(IST)

                    #<!-- Grava Log de Importação como Sucesso -->
                    self.app_tracking.log_tracking_session(2,endpoint_name,count_page,start_datetime,end_datetime,1,len(customers),None)
                except:
                    self.log.debug('Function %s: Erro Loop importação de Base' %(module_name))
                    #<!-- Em caso de erro na importação gravar erro -->
                    end_datetime = datetime.now(IST)
                    self.app_tracking.log_tracking_session(2,endpoint_name,count_page,start_datetime,end_datetime,0,len(customers),"Erro Importação")
                    pass
                
                self.log.debug('Function %s: FIM Processo importação de Base' %(module_name))

                #<!-- Commit do Banco -->
                self.cnn.commit()
                self.log.debug('Function %s: Commit Realizado' %(module_name))

                #<!-- Verifica se existe outra página -->
                next = self.next_page(header)
                               
                if next[0] == True:
                    endpoint = next[1]
                    page_info = next[2]
                    count_page += 1
                else:
                    break
        
            #<!-- Caso Status != 200 - Gravar Falha -->     
            else:
                self.log.debug('Function %s: STATUS CODE != 200' %(module_name))
                end_datetime = datetime.now(IST) 
                self.app_tracking.log_tracking_session(1,endpoint_name,count_page,start_datetime,end_datetime,0,0,status_code)  
        
        start_etl = datetime.now(IST)
        self.cursor.execute("call stage.etl_customers(%s)" %(self.id_session))
        self.cursor.execute("call stage.etl_customers_addresses(%s)" %(self.id_session))
        end_etl = datetime.now(IST)
        self.app_tracking.log_tracking_session(3,endpoint_name,0,start_etl,end_etl,1,0,None)        
        
        self.app_tracking.log_tracking_insert('tb_customers', rows_insert, rows_error)
        self.app_tracking.log_tracking_insert('tb_customer_addresses', rows_insert_2, rows_error_2)
        self.log.debug('Function %s: Finalizado' %(module_name))
        #<!---- Fim do While de Requisição API --->

    def request_orders(self,start_date):
        module_name = 'request_orders'
        self.log.debug('Function %s: Iniciado' %(module_name))
        
        #<!---- Definição das Variaveis Endpoint Inicial -->
        endpoint = f'https://rawpack-bars.myshopify.com/admin/api/2022-04/orders.json?limit=250&updated_at_min={start_date}'
        endpoint = endpoint + '&status=any'
        endpoint = endpoint + '&financial_status=any'
        count_page = 0
        status_code = 200
        # Contadores Principais
        rows_insert = 0
        rows_error = 0
        # Contadores Secundários
        rows_insert_2 = 0
        rows_error_2 = 0
        # Contadores Secundários
        rows_insert_3 = 0
        rows_error_3 = 0
        #<!---- Fim Definição das Variaveis Endpoint Inicial -->

        #<!---- Inicio do While de Requisição API --->
        while(status_code == 200):
            
            #<!-- Variaveis para Tracking da Conexão Endpoint -->
            start_datetime = datetime.now(IST)
            endpoint_name = 'orders'

            #<!-- Requisção Endpoint -->
            connect_to = self.request_endpoint(endpoint, 'orders')
            status_code = connect_to[0].status_code
            self.log.debug('Function %s: STATUS CODE = %s' %(module_name, status_code))
            
            #<!-- Executa se o endpoint tiver status == 200 -->
            if status_code == 200:
                
                #<!-- Variaveis para Tracking Final da Conexão Endpoint -->
                end_datetime = datetime.now(IST) 

                #<!-- Executa Metodo para gravar Log da execução Gravando Sucesso -->
                self.app_tracking.log_tracking_session(1,endpoint_name,count_page,start_datetime,end_datetime,1,0,None)
                self.log.debug('Function %s: Registrado Log tracking' %(module_name))  
                self.log.info('Function %s: | Dados: endpoint_name: %s | count_page: %s | start_datetime: %s | end_datetime: %s ' %(module_name, endpoint_name,count_page,start_datetime,end_datetime))

                #<!-- Coleta os dados do Enpoint -->
                orders = connect_to[3]
                header = connect_to[2]

                #<!-- Variável de Inicio do processo de importação -->
                start_datetime = datetime.now(IST)

                #<!-- Try para tratar error -->
                self.log.debug('Function %s: Inicio Processo importação de Base' %(module_name)) 
                try:
                    self.log.debug('Function %s: Entrou no TRY da importação de Base' %(module_name)) 
                    #<!-- Loop para importar Linha a Linha do DF -->
                    for index, row in orders.iterrows():
                        #<!-- Variaveis que serão importadas -->
                        id = row.id
                        cancelled_at = row.cancelled_at
                        cart_token = self.clean_string(row.cart_token)
                        checkout_id = row.checkout_id
                        checkout_token = row.checkout_token
                        closed_at = row.closed_at
                        confirmed = row.confirmed
                        contact_email = self.clean_string(row.contact_email)
                        created_at = row.created_at
                        currency = self.clean_string(row.currency)
                        current_subtotal_price = row.current_subtotal_price
                        current_total_discounts = row.current_total_discounts
                        current_total_duties_set = row.current_total_duties_set
                        current_total_price = row.current_total_price
                        current_total_tax = row.current_total_tax
                        customer_locale = row.customer_locale
                        device_id = self.clean_string(row.device_id)
                        email = self.clean_string(row.email)
                        estimated_taxes = row.estimated_taxes
                        financial_status = row.financial_status
                        fulfillment_status = row.fulfillment_status
                        gateway = self.clean_string(row.gateway)
                        landing_site = self.clean_string(row.landing_site)
                        landing_site_ref = self.clean_string(row.landing_site_ref)
                        location_id = self.clean_string(row.location_id)
                        name = self.clean_string(row['name'])
                        note = self.clean_string(row.note)
                        number = row.number
                        order_number = row.order_number
                        original_total_duties_set = row.original_total_duties_set
                        payment_gateway_names = None
                        phone = self.clean_string(row.phone)
                        presentment_currency = row.presentment_currency
                        processed_at = row.processed_at
                        processing_method = row.processing_method
                        reference = self.clean_string(row.reference)
                        referring_site = row.referring_site
                        source_identifier = row.source_identifier
                        source_name = self.clean_string(row.source_name)
                        subtotal_price = row.subtotal_price
                        tags = self.clean_string(row.tags)
                        taxes_included = row.taxes_included
                        test = row.test
                        token = row.token
                        total_discounts = row.total_discounts
                        total_line_items_price = row.total_line_items_price
                        total_outstanding = row.total_outstanding
                        total_price = row.total_price
                        total_price_usd = row.total_price_usd
                        total_tax = row.total_tax
                        total_tip_received = row.total_tip_received
                        total_weight = row.total_weight
                        updated_at = row.updated_at
                        user_id = row.user_id
                        billing_address_first_name = self.clean_string(row['billing_address.first_name'])
                        billing_address_address1 = self.clean_string(row['billing_address.address1'])
                        billing_address_phone = self.clean_string(row['billing_address.phone'])
                        billing_address_city = self.clean_string(row['billing_address.city'])
                        billing_address_zip = self.clean_string(row['billing_address.zip'])
                        billing_address_province = self.clean_string(row['billing_address.province'])
                        billing_address_country = self.clean_string(row['billing_address.country'])
                        billing_address_last_name = self.clean_string(row['billing_address.last_name'])
                        billing_address_address2 = self.clean_string(row['billing_address.address2'])
                        billing_address_company = self.clean_string(row['billing_address.company'])
                        billing_address_latitude = self.clean_string(row['billing_address.latitude'])
                        billing_address_longitude = self.clean_string(row['billing_address.longitude'])
                        billing_address_name = self.clean_string(row['billing_address.name'])
                        billing_address_country_code = self.clean_string(row['billing_address.country_code'])
                        billing_address_province_code = self.clean_string(row['billing_address.province_code'])
                        shipping_address_first_name = self.clean_string(row['shipping_address.first_name'])
                        shipping_address_address1 = self.clean_string(row['shipping_address.address1'])
                        shipping_address_phone = self.clean_string(row['shipping_address.phone'])
                        shipping_address_city = self.clean_string(row['shipping_address.city'])
                        shipping_address_zip = self.clean_string(row['shipping_address.zip'])
                        shipping_address_province = self.clean_string(row['shipping_address.province'])
                        shipping_address_country = self.clean_string(row['shipping_address.country'])
                        shipping_address_last_name = self.clean_string(row['shipping_address.last_name'])
                        shipping_address_address2 = self.clean_string(row['shipping_address.address2'])
                        shipping_address_company = self.clean_string(row['shipping_address.company'])
                        shipping_address_latitude = self.clean_string(row['shipping_address.latitude'])
                        shipping_address_longitude = self.clean_string(row['shipping_address.longitude'])
                        shipping_address_name = self.clean_string(row['shipping_address.name'])
                        shipping_address_country_code = self.clean_string(row['shipping_address.country_code'])
                        shipping_address_province_code = self.clean_string(row['shipping_address.province_code'])
                        #<!-- Fim Variaveis que serão importadas -->

                        #<!-- Instrução para Importação da Linha -->
                        try:
                            query_orders = """Insert into stage.tb_orders 
                                values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""
                            query_log_orders = query_orders %(id,cancelled_at,cart_token,checkout_id,checkout_token,closed_at,confirmed,contact_email,created_at,currency,current_subtotal_price,current_total_discounts,current_total_duties_set,current_total_price,current_total_tax,customer_locale,device_id,email,estimated_taxes,financial_status,fulfillment_status,gateway,landing_site,landing_site_ref,location_id,name,note,number,order_number,original_total_duties_set,payment_gateway_names,phone,presentment_currency,processed_at,processing_method,reference,referring_site,source_identifier,source_name,subtotal_price,tags,taxes_included,test,token,total_discounts,total_line_items_price,total_outstanding,total_price,total_price_usd,total_tax,total_tip_received,total_weight,updated_at,user_id,billing_address_first_name,billing_address_address1,billing_address_phone,billing_address_city,billing_address_zip,billing_address_province,billing_address_country,billing_address_last_name,billing_address_address2,billing_address_company,billing_address_latitude,billing_address_longitude,billing_address_name,billing_address_country_code,billing_address_province_code,shipping_address_first_name,shipping_address_address1,shipping_address_phone,shipping_address_city,shipping_address_zip,shipping_address_province,shipping_address_country,shipping_address_last_name,shipping_address_address2,shipping_address_company,shipping_address_latitude,shipping_address_longitude,shipping_address_name,shipping_address_country_code,shipping_address_province_code,datetime.now(IST))
                            self.cursor.execute(query_orders %(id,cancelled_at,cart_token,checkout_id,checkout_token,closed_at,confirmed,contact_email,created_at,currency,current_subtotal_price,current_total_discounts,current_total_duties_set,current_total_price,current_total_tax,customer_locale,device_id,email,estimated_taxes,financial_status,fulfillment_status,gateway,landing_site,landing_site_ref,location_id,name,note,number,order_number,original_total_duties_set,payment_gateway_names,phone,presentment_currency,processed_at,processing_method,reference,referring_site,source_identifier,source_name,subtotal_price,tags,taxes_included,test,token,total_discounts,total_line_items_price,total_outstanding,total_price,total_price_usd,total_tax,total_tip_received,total_weight,updated_at,user_id,billing_address_first_name,billing_address_address1,billing_address_phone,billing_address_city,billing_address_zip,billing_address_province,billing_address_country,billing_address_last_name,billing_address_address2,billing_address_company,billing_address_latitude,billing_address_longitude,billing_address_name,billing_address_country_code,billing_address_province_code,shipping_address_first_name,shipping_address_address1,shipping_address_phone,shipping_address_city,shipping_address_zip,shipping_address_province,shipping_address_country,shipping_address_last_name,shipping_address_address2,shipping_address_company,shipping_address_latitude,shipping_address_longitude,shipping_address_name,shipping_address_country_code,shipping_address_province_code,datetime.now(IST)))
                            rows_insert += 1
                        except:
                            self.log.error('Function %s: Query: %s' %(module_name, query_log_orders))
                            rows_error += 1
                            pass
                        #<!-- Fim Instrução para Importação da Linha -->

                        #<!-- Inicio Sub Loop do For PAI -->
                        #<!-- Dataframe Sub -->    
                        df_line_items = pd.json_normalize(row.line_items)

                        for index, row1 in df_line_items.iterrows():
                            #<!-- Inicio Variaveis que serão importadas -->
                            id_order = (row.id)
                            id = self.clean_string(row1.id)
                            fulfillable_quantity = self.clean_string(row1.fulfillable_quantity)
                            fulfillment_service = self.clean_string(row1.fulfillment_service)
                            fulfillment_status = self.clean_string(row1.fulfillment_status)
                            gift_card = self.clean_string(row1.gift_card)
                            grams = self.clean_string(row1.grams)
                            name = self.clean_string(row1.name)
                            price = self.clean_string(row1.price)
                            product_exists = self.clean_string(row1.product_exists)
                            product_id = self.clean_string(row1.product_id)
                            properties = None
                            quantity = self.clean_string(row1.quantity)
                            requires_shipping = self.clean_string(row1.requires_shipping)
                            sku = self.clean_string(row1.sku)
                            taxable = self.clean_string(row1.taxable)
                            title = self.clean_string(row1.title)
                            total_discount = self.clean_string(row1.total_discount)
                            variant_id = self.clean_string(row1.variant_id)
                            variant_inventory_management = self.clean_string(row1.variant_inventory_management)
                            variant_title = self.clean_string(row1.variant_title)
                            vendor = self.clean_string(row1.vendor)
                            tax_lines = None
                            duties = None
                            discount_allocations = None
                            price_set_shop_money_amount = self.clean_string(row1['price_set.shop_money.amount'])
                            price_set_shop_money_currency_code = self.clean_string(row1['price_set.shop_money.currency_code'])
                            price_set_presentment_money_amount = self.clean_string(row1['price_set.presentment_money.amount'])
                            price_set_presentment_money_currency_code = self.clean_string(row1['price_set.presentment_money.currency_code'])
                            total_discount_set_shop_money_amount = self.clean_string(row1['total_discount_set.shop_money.amount'])
                            total_discount_set_shop_money_currency_code = self.clean_string(row1['total_discount_set.shop_money.currency_code'])
                            total_discount_set_presentment_money_amount = self.clean_string(row1['total_discount_set.presentment_money.amount'])
                            total_discount_set_presentment_money_currency_code = self.clean_string(row1['total_discount_set.presentment_money.currency_code'])
                            #<!-- Fim Variaveis que serão importadas -->

                            #<!-- Instrução para Importação da Linha -->
                            try:
                                query_line_itens = """Insert into stage.tb_orders_line_items values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"""
                                query_log_line_itens =  query_line_itens %(id,fulfillable_quantity,fulfillment_service,fulfillment_status,gift_card,grams,name,price,product_exists,product_id,properties,quantity,requires_shipping,sku,taxable,title,total_discount,variant_id,variant_inventory_management,variant_title,vendor,tax_lines,duties,discount_allocations,price_set_shop_money_amount,price_set_shop_money_currency_code,price_set_presentment_money_amount,price_set_presentment_money_currency_code,total_discount_set_shop_money_amount,total_discount_set_shop_money_currency_code,total_discount_set_presentment_money_amount,total_discount_set_presentment_money_currency_code,datetime.now(IST),id_order)
                                self.cursor.execute(query_line_itens %(id,fulfillable_quantity,fulfillment_service,fulfillment_status,gift_card,grams,name,price,product_exists,product_id,properties,quantity,requires_shipping,sku,taxable,title,total_discount,variant_id,variant_inventory_management,variant_title,vendor,tax_lines,duties,discount_allocations,price_set_shop_money_amount,price_set_shop_money_currency_code,price_set_presentment_money_amount,price_set_presentment_money_currency_code,total_discount_set_shop_money_amount,total_discount_set_shop_money_currency_code,total_discount_set_presentment_money_amount,total_discount_set_presentment_money_currency_code,datetime.now(IST),id_order))
                                rows_insert_2 += 1
                            except:
                                self.log.error('Function %s: Query: %s' %(module_name, query_log_line_itens))
                                rows_error_2 += 1
                            pass
                            #<!-- Fim Instrução para Importação da Linha -->
                        #<!-- Fim Sub Loop do For PAI -->

                        #<!-- Inicio Sub Loop do For PAI -->
                        #<!-- Dataframe Sub -->    
                        df_dicounts_code = pd.json_normalize(row.discount_codes)

                        for index, row2 in df_dicounts_code.iterrows():
                            #<!-- Inicio Variaveis que serão importadas -->
                            id_order = self.clean_string(row.id)
                            code = self.clean_string(row2.code)
                            amount = self.clean_string(row2.amount)
                            type_ = self.clean_string(row2.type)
                            #<!-- Fim Variaveis que serão importadas -->

                            #<!-- Instrução para Importação da Linha -->
                            try:
                                query_discount_code = """Insert into stage.tb_orders_discount_codes 
                                values ('%s','%s','%s','%s','%s')"""
                                query_log_discount_code =  query_discount_code %(id_order,code,amount,type_,datetime.now(IST))
                                self.cursor.execute(query_discount_code %(id_order,code,amount,type_,datetime.now(IST)))
                                rows_insert_3 += 1
                            except:
                                self.log.error('Function %s: Query: %s' %(module_name, query_log_discount_code))
                                rows_error_3 += 1
                            pass
                            #<!-- Fim Instrução para Importação da Linha -->
                        #<!-- Fim Sub Loop do For PAI -->

                    #<!-- Variaveis do Final da importação -->
                    end_datetime = datetime.now(IST)

                    #<!-- Grava Log de Importação como Sucesso -->
                    self.app_tracking.log_tracking_session(2,endpoint_name,count_page,start_datetime,end_datetime,1,len(orders),None)
                except:
                    self.log.debug('Function %s: Erro Loop importação de Base' %(module_name))
                    #<!-- Em caso de erro na importação gravar erro -->
                    end_datetime = datetime.now(IST)
                    self.app_tracking.log_tracking_session(2,endpoint_name,count_page,start_datetime,end_datetime,0,len(orders),"Erro Importação")
                    pass
                self.log.debug('Function %s: FIM Processo importação de Base' %(module_name))

                #<!-- Commit do Banco -->
                self.cnn.commit()
                self.log.debug('Function %s: Commit Realizado' %(module_name))

                #<!-- Verifica se existe outra página -->
                next = self.next_page(header)
                               
                if next[0] == True:
                    endpoint = next[1]
                    page_info = next[2]
                    count_page += 1
                else:
                    break
        
            #<!-- Caso Status != 200 - Gravar Falha -->     
            else:
                self.log.debug('Function %s: STATUS CODE != 200' %(module_name))
                end_datetime = datetime.now(IST) 
                self.app_tracking.log_tracking_session(1,endpoint_name,count_page,start_datetime,end_datetime,0,0,status_code)  

        start_etl = datetime.now(IST)
        self.cursor.execute("call stage.etl_orders(%s)" %(self.id_session))
        self.cursor.execute("call stage.etl_orders_billing_address(%s)" %(self.id_session))
        self.cursor.execute("call stage.etl_orders_discount_codes(%s)" %(self.id_session))
        self.cursor.execute("call stage.etl_orders_line_items(%s)" %(self.id_session))
        self.cursor.execute("call stage.etl_orders_shipping_address(%s)" %(self.id_session))
        end_etl = datetime.now(IST)
        self.app_tracking.log_tracking_session(3,endpoint_name,0,start_etl,end_etl,1,0,None) 

        self.app_tracking.log_tracking_insert('tb_orders', rows_insert, rows_error)
        self.app_tracking.log_tracking_insert('tb_orders_line_items', rows_insert_2, rows_error_2)
        self.app_tracking.log_tracking_insert('tb_orders_discount_codes', rows_insert_3, rows_error_3)
        self.log.debug('Function %s: Finalizado' %(module_name))
        #<!---- Fim do While de Requisição API --->



           
