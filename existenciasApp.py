import xmlrpc.client
import re
import os
import time 
from pandas.io import excel
import requests
import urllib.parse as urlparse
from urllib.parse import parse_qs, parse_qsl, unquote
from urllib.parse import urlparse, unquote



import tabula
import pdfplumber

import pandas as pd 
from pandas._libs import index
from pandas.core.ops import invalid
from tqdm import tqdm

class existenciasOdoo():
    def __init__(self):
        # Pandas, analisis de datos 
        self.odooURL = "https://cea-control1.odoo.com/"
        self.odooDB = "cea-pruebas"
        self.odooUser = "apoyo.direccion@ceacontrol.com"
        self.odooPass = "apoyo.direccion"

        self.parkerPath = os.path.join("excel/Parker.xlsx")
        self.invCeaPath = os.path.join("excel/InventarioCEA.xlsx")
        self.stockPhoenixPath = os.path.join("excel/Phoenix.xls")
        self.optexPath = os.path.join("excel/existenciasOptex.xlsx")
        self.optexPDFPath = os.path.join("PDF/optex.pdf")
        self.pilzPath = os.path.join("excel/Pilz.XLSX")
     
        self.finder = pd.read_csv(os.path.join("excel/Finder.csv"), sep=";")
        self.finderPath = os.path.join("excel/Finder.xlsx")
        self.eatonPath = os.path.join("excel/Eaton.xlsx")
        self.eatonPDFPath = os.path.join("PDF/Eaton.pdf")

        self.parker = pd.read_excel(self.parkerPath)
        self.finder.to_excel(self.finderPath, index=False)  
        self.invCEA = pd.read_excel(self.invCeaPath, skiprows=6)
        self.stockPhoenix = pd.read_excel(self.stockPhoenixPath)
        self.pilz = pd.read_excel(self.pilzPath)
       

        common = xmlrpc.client.ServerProxy(f"{self.odooURL}/xmlrpc/2/common")
        self.uid = common.authenticate(self.odooDB, self.odooUser, self.odooPass, {})
        self.models = xmlrpc.client.ServerProxy(f"{self.odooURL}/xmlrpc/2/object")

    # webscrapping, selenium
    
        
        with open('ServiceList.txt', 'w', encoding='UTF-8') as f:
            f.write("-#-#-#-#-#-#-#-#-#-#-Lista de supuestos servicios en las listas de existencias-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#\n\n")
        


    def compProducts(self, column_code,location_ID, cuantity_code, df, filePath):
        df = df.dropna(how= 'all')
        

        df = df[df[cuantity_code] !=0]
        df = df[df[cuantity_code] != '-']
        
        validCodes = []
        invalidCodes = [] 
        productsIDs = []
        count = 0
        countVlid = 0
        countNonValid = 0
        stockMessage = ""
        bar = tqdm(total= len(df), desc=f"Analizando {filePath}")

        for index, row  in df.iterrows():
            #Buscoando en Odoo si existe el producto on ese codigo 
            codigo = str(row[column_code]).strip()
            cantidad = str(row[cuantity_code]).strip()
            if count % 100 == 0 and count != 0:
                time.sleep(2)
            try:
                producto = self.models.execute_kw(
                    self.odooDB,
                    self.uid,
                    self.odooPass,
                    'product.product',
                    'search_read',
                    [[['name', 'ilike', codigo]]],
                    {'fields':['id','name','default_code', 'type'],'limit':1}
                    )
                count += 1
                if os.name == 'nt':
                    os.system('cls')    
                else:                         
                    os.system('clear')
        
            #print(f"Analizando {filePath}...")
                bar.update()
                print(f"Productos analizados: {count}")
                print(f"productos encontrados: {countVlid}")
                print(f"Productos no encontrados: {countNonValid}")
                print(stockMessage)
                if producto:
                    id = producto[0]['id']
                    productsIDs.append(id)
                    validCodes.append(True)
                    invalidCodes.append(False)
                    countVlid += 1
                    stockMessage = self.createNewStockCuant(id, location_ID , cantidad )
                    

            
                else:
                    validCodes.append(False)
                    invalidCodes.append(True)
                    countNonValid += 1
                
            except xmlrpc.client.ProtocolError as e:
                print(f"Error de protocolo! {e.url} codigo{e.errcode}")
                print(f"Mensaje de error: {e.errmsg}") 
            except xmlrpc.client.Fault as e:
                print(f"Error XMLRPC {e.faultString}")
                print(f"Codigo {e.faultCode}")
            except ConnectionRefusedError:
                print("Conexion Rechazada por el servidor")
            except TimeoutError:
                print(f"Tiempo de espera agotado para conectarse con el servidor")

            #filtrar solo los productos que se encuentran en la base de datos    
        df_validCodes = df[validCodes]
        df_invalidCodes = df[invalidCodes]
        #Guardar los productos en un nuevo excel
        df_validCodes.to_excel(os.path.join("validCodes", f"validCodes_{filePath.replace("excel/","")}.xlsx"), index=False)
        df_invalidCodes.to_excel(os.path.join("invalidCodes",f"invalidCodes_{filePath.replace("excel/","")}.xlsx"), index=False)      
        print("Actualizando cache del servidor...")
        self.updateCache(productsIDs)
        if os.name == 'nt':
            os.system('cls')    
        else:                         
            os.system('clear')
        
        print("Proceso terminado")
    
    def webscrappingEaton(self):
        self.driver.get("https://www.kloeme.com/existencias")
        print("ingresando a https://www.kloeme.com/existencias")
        wait = WebDriverWait(self.driver, 10)
        emailInput = wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "input[type='email']"))
                )
        emailInput.send_keys("apoyo.direccion@ceacontrol.com")
        
        self.driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys("apoyo.direccion" + Keys.RETURN)
        time.sleep(5) 
        print("Descargando pdf...")
        iframeWix = wait.until(
                EC.visibility_of_element_located((By.TAG_NAME, "iframe"))
                )
        urlWix = iframeWix.get_attribute("src")
        
        self.driver.get(str(urlWix))

        iframePDF = wait.until(
                EC.presence_of_element_located((By.TAG_NAME, "iframe"))
                )
        time.sleep(5)
        urlPDF = iframePDF.get_attribute("src")

        parsed = urlparse(urlPDF)        
        params = parse_qs(parsed.query)

        fileParam = params["file"][0]
        decoded_file = unquote(fileParam)

        innerParams = parse_qs(urlparse(decoded_file).query)
        
        pdf_url = innerParams["url"][0]
        response = requests.get(pdf_url)
        with open("ExistenciasEaton.pdf", "wb") as f:
            f.write(response.content)

        self.driver.close()

        print("Convirtiendo a excel")
        excelPath = os.path.join("excel/eatonExistencias.xlsx")
        pdfPath = os.path.join("ExistenciasEaton.pdf")
        df = self.pdfToExcel(pdfPath=pdfPath, excelPath=excelPath)

        #Rellenar ceros a la izquierda en los numeros de parte en excel
        print("Corrigiendo datos")

        for i, code in df["PRODUCTO"].items():
            if len(str(code)) < 6 and str(code).isdigit():
                    df.at[i, "PRODUCTO"] = str(code).zfill(6)
        df.to_excel(excelPath, index=False)
        self.compProducts("PRODUCTO",285,"PUEBLA", df, excelPath)
    
    def createNewStockCuant(self, product_id, location_id, quantity):
        """
        Creates or updates a stock quant (inventory record) for a given product in Odoo.

        Parameters:
            product_id (int): The ID of the product to update or create the stock quant for.
            location_id (int): The ID of the location where the stock is stored.
            quantity (int or float): The quantity of the product to set in stock.

        Returns:
            str: A message indicating whether the stock quant was created or updated, or None if an error occurred.
        """
        try:
            quant = self.models.execute_kw(
                self.odooDB,
                self.uid,
                self.odooPass,
                'stock.quant',
                'search_read',
                [[['product_id', '=', product_id]]],
                {'fields': ['id']} 
                )

            if quant:
                #print(f"[DEBUG] : {quant[0]['product_id']}")
                self.models.execute_kw(
                self.odooDB,
                self.uid,
                self.odooPass,
                'stock.quant',
                'write',
                [[quant[0]['id']], {'quantity': quantity} ]
                )
                return(f"Articulo {product_id} subido a existencias")
            else:
                self.models.execute_kw(
                    self.odooDB,
                    self.uid,
                    self.odooPass,
                    'stock.quant',
                    'create',
                    [{
                    'product_id': product_id,
                    'location_id': location_id,
                    'quantity': quantity
                    }]
                    )
                return(f"Existencia creada para {product_id}")
        except xmlrpc.client.Fault:
            producto = self.models.execute_kw(
                self.odooDB,
                self.uid,
                self.odooPass,
                'product.product',
                'search_read',
                [[['id', '=', product_id]]],
                {'fields':['id', 'name', 'default_code', 'type']}
            )
            with open('ServiceList.txt','a', encoding='UTF-8') as f:
                f.write(f"{producto}\n")
            return f"Error: No se pudo crear o actualizar stock.quant para el producto {product_id}. Detalles guardados en ServiceList.txt"
        except xmlrpc.client.ProtocolError as e:
                print(f"Error de protocolo! {e.url} codigo{e.errcode}")
                print(f"Mensaje de error: {e.errmsg}") 
        
        except ConnectionRefusedError:
                print("Conexion Rechazada por el servidor")
        except TimeoutError:
                print(f"Tiempo de espera agotado para conectarse con el servidor")
        
            
                        
    def pdfToExcel(self, pdfPath, excelPath):
        
        data = ""
        procData = []
        rows = []

        if pdfPath == self.optexPDFPath:
            #Extrar una cadena de texto desde el pdf
            with pdfplumber.open(pdfPath) as pdf:
                for pagina in pdf.pages:
                    texto = pagina.extract_text()
                    if texto:
                        data += texto + "\n"
                if data != "":
                    validData = re.sub(r"^SKU DESCRIPCION LINEA EXISTENCIAS.*(?:\r?\n)?", "", data, flags=re.MULTILINE).split("\n")
                    for vd in validData:
                        procData.append(vd.split(" "))

                    for p in procData:
                        filtData = []
                        filtData.append(p[0])
                        filtData.append(p [len(p) -1 ])
                        rows.append(filtData)

                    df = pd.DataFrame(rows, columns=["SKU", "Existencias"])
                    df.to_excel(excelPath, index=False)
                    return df
            #Crear el excel desde el texto
        else:    
            tables = tabula.read_pdf(pdfPath, pages= "all", multiple_tables=True)
            df = pd.concat(tables, ignore_index=True)
            df.to_excel(excelPath, index=True)
            return df 
    
    def updateCache(self, product_ids):
        try:
            self.models.execute_kw(
                self.odooDB,
                self.uid,
                self.odooPass,
                'product.product',
                'write',
                [product_ids, {}]
            )
        
        except xmlrpc.client.ProtocolError as e:
                print(f"Error de protocolo! {e.url} codigo{e.errcode}")
                print(f"Mensaje de error: {e.errmsg}") 
        except xmlrpc.client.Fault as e:
                print(f"Error XMLRPC {e.faultString}")
                print(f"Codigo {e.faultCode}")
        except ConnectionRefusedError:
                print("Conexion Rechazada por el servidor")
        except TimeoutError:
                print(f"Tiempo de espera agotado para conectarse con el servidor")
        
    def deleteStockQuants(self, arrLoc):
        quants = []
        try:
            for a in arrLoc:
                quants += self.models.execute_kw(
                    self.odooDB,
                    self.uid,
                    self.odooPass,
                    'stock.quant',
                    'search',
                    [[('location_id', '=', a)]]
                    )
                
            print("Eliminando existencias...")
            if quants:
                self.models.execute_kw(
                    self.odooDB,
                    self.uid,
                    self.odooPass,
                    'stock.quant',
                    'write',
                    [quants, {'quantity': 0}]
                )
                print(f"Todadas las existencias han sido eliminadas")
                        
            else:
                print("No hay existencias que eliminar")
        except xmlrpc.client.ProtocolError as e:
                print(f"Error de protocolo! {e.url} codigo{e.errcode}")
                print(f"Mensaje de error: {e.errmsg}") 
        except xmlrpc.client.Fault as e:
                print(f"Error XMLRPC {e.faultString}")
                print(f"Codigo {e.faultCode}")
        except ConnectionRefusedError:
                print("Conexion Rechazada por el servidor")
        except TimeoutError:
                print(f"Tiempo de espera agotado para conectarse con el servidor")
    
        
        
        


if __name__ == "__main__":
    oExistencias = existenciasOdoo()
    """
    Locaciones por ID segun provedor

| Proveedor / Almacén       | ID  | Ubicación (`complete_name`)    
| ------------------------- | --- | ---------------------------------- |
| **CEA**                   | 252 | WH/Inventario CEA                  |
| **pilz**                  | 266 | pilz/Existencias                   |
| **PHOE (Phoenix)**        | 279 | PHOE/Existencias                   |
| **Eaton**                 | 285 | Eaton/Existencias                  |
| **P H (Parker Hannifin)** | 223 | P H/Existencias                    |
| **PAT (Patlite)**         | 235 | PAT/Existencias                    |
| **FINDE (Finder)**        | 241 | FINDE/Existencias                  |
| **OPTEX**                 | 247 | OPTEX/Existencias                  |


    """
    
    cea = False
    pilz = False
    eaton = False 
    parker = False 
    finder = False
    phinix = False
    optex = False 
    
    arrLoc = []
    
    inp = input(f"""Escribe una secuencia de numero de acuerdo a los inventarios que deseas actualizar,
Despues, presiona ENTER:

[1]Inventario CEA (SIMAN)
[2]Pilz
[3]Eaton
[4]Parker
[5]Finder
[6]Phoenix
[7]Optex
""")
    for i in inp:
        if int(i) == 1:
            cea = True
            arrLoc.append(252)
        elif int(i) == 2:
            pilz = True
            arrLoc.append(266)
        elif int(i) == 3:
            eaton = True
            arrLoc.append(285)
        elif int(i) == 4:
            parker = True
            arrLoc.append(223)
        elif int(i) == 5:
            finder = True
            arrLoc.append(241)
        elif int(i) == 6:
            phinix = True
            arrLoc.append(279)
        elif int(i) == 7:
            optex = True
            arrLoc.append(247)


    #Aqui empieza el proceso de eliminacion y actualizacion de existencias
    if len(arrLoc) > 0:
        oExistencias.deleteStockQuants(arrLoc=arrLoc) 
    if cea:
        oExistencias.compProducts("Códgo",252, "Cantidad",oExistencias.invCEA, oExistencias.invCeaPath )
    if phinix:
        oExistencias.compProducts("Art. /Contenedor",279, 'Stock disp', oExistencias.stockPhoenix, oExistencias.stockPhoenixPath)
    if finder:
        oExistencias.compProducts("CÓDIGO",241,"CANTIDAD", oExistencias.finder, oExistencias.finderPath)
    if optex:
        oExistencias.compProducts("SKU",247,"Existencias", oExistencias.pdfToExcel(oExistencias.optexPDFPath, oExistencias.optexPath), oExistencias.optexPath) 
    if pilz:
        oExistencias.compProducts("Material",266,"Closing Stock", oExistencias.pilz, oExistencias.pilzPath)
    if parker:
        oExistencias.compProducts("Producto", 223, "Existencia Disponible", oExistencias.parker,oExistencias.parkerPath)
    if eaton:
        oExistencias.compProducts("PRODUCTO",285, 'PUEBLA', oExistencias.pdfToExcel(oExistencias.eatonPDFPath, oExistencias.eatonPath), oExistencias.eatonPath)
    

   
