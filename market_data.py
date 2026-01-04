import os
import time
import pandas as pd
import logging
from selenium.webdriver.remote.remote_connection import LOGGER
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (StaleElementReferenceException, TimeoutException, 
                                       NoSuchElementException, WebDriverException)
from bs4 import BeautifulSoup
from io import StringIO
import ssl

from db_utils import save_market_data

# Suprimir logs de Selenium
LOGGER.setLevel(logging.WARNING)
logging.getLogger('selenium').setLevel(logging.WARNING)

# Configuración de URLs
urls = {
    "Panel Líderes": "https://iol.invertironline.com/mercado/cotizaciones/argentina",
    "Panel General": "https://iol.invertironline.com/mercado/cotizaciones/argentina",
    "Subastas": "https://iol.invertironline.com/mercado/cotizaciones/argentina",
    "Cotizacion de Cedears Argentina": "https://iol.invertironline.com/mercado/cotizaciones/argentina/cedears/todos",
    "Cotizacion de bonos Argentina": "https://iol.invertironline.com/mercado/cotizaciones/argentina/bonos/todos",
    "Cotizacion de fondos Argentina": "https://iol.invertironline.com/mercado/cotizaciones/argentina/fondos/todos",
    "Ripio Criptomonedas": "https://www.ripio.com/es/criptomonedas"
}

# Configuración de SSL
ssl._create_default_https_context = ssl._create_unverified_context

def descargar_datos_mercado(carpeta_destino):
    """Descarga todos los datos de mercado y devuelve el DataFrame combinado"""
    # Configuración de Selenium
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    # Añadir estas opciones para suprimir logs
    chrome_options.add_argument('--log-level=3')
    
    try:
        # Ajusta esta ruta a tu chromedriver
        service = Service(executable_path=r'C:\ruta\a\tu\chromedriver.exe')
        service.creation_flags = 0x8000000
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except:
        driver = webdriver.Chrome(options=chrome_options)

    driver.implicitly_wait(10)
    wait = WebDriverWait(driver, 20)
    all_dfs = []

    def mostrar_todo():
        """Selecciona 'Mostrar Todo' en el dropdown"""
        attempts = 0
        while attempts < 3:
            try:
                select = Select(wait.until(
                    EC.element_to_be_clickable((By.NAME, "cotizaciones_length"))
                ))
                select.select_by_value("-1")
                wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, '#cotizaciones tbody tr')) > 10)
                return
            except (StaleElementReferenceException, TimeoutException):
                attempts += 1
                time.sleep(2)

    def obtener_tabla(nombre_fuente, selector="table#cotizaciones", tipo='iol'):
        """Obtiene la tabla como DataFrame"""
        try:
            tabla = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
            html = tabla.get_attribute('outerHTML')
            df = pd.read_html(StringIO(html), decimal=',', thousands='.')[0]
            
            for col in df.columns:
                if df[col].dtype == 'object':
                    converted = pd.to_numeric(
                        df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.'),
                        errors='coerce'
                    )
                    if not converted.isna().all():
                        df[col] = converted
            
            if tipo == 'ripio' and isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(0)
                df = df.dropna(how='all')
            
            return df
        except Exception as e:
            print(f"Error obteniendo tabla {nombre_fuente}: {str(e)}")
            return None

    def procesar_panel(panel):
        """Procesa paneles de IOL"""
        attempts = 0
        max_attempts = 3
        
        while attempts < max_attempts:
            try:
                driver.get(urls["Panel Líderes"])
                time.sleep(2)
                
                selector_paneles = wait.until(
                    EC.presence_of_element_located((By.ID, "paneles"))
                )
                
                driver.execute_script("arguments[0].scrollIntoView(true);", selector_paneles)
                time.sleep(0.5)
                
                select = Select(selector_paneles)
                select.select_by_visible_text(panel)
                
                wait.until(EC.presence_of_element_located((By.ID, "cotizaciones")))
                time.sleep(1)
                
                mostrar_todo()
                df = obtener_tabla(f"Acciones Argentinas - {panel}")
                if df is not None:
                    return df
                else:
                    attempts += 1
                    time.sleep(3)
            except Exception as e:
                attempts += 1
                time.sleep(3)
        return None

    def extraer_tabla_ripio():
        """Extrae la tabla de criptomonedas de Ripio"""
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#cotizaciones-list .collection-item-6")))
            time.sleep(2)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            contenedor = soup.find('div', {'id': 'cotizaciones-list'})
            monedas = []
            
            for item in contenedor.find_all('div', class_='collection-item-6'):
                try:
                    monedas.append({
                        'Moneda': item.find('div', class_='c-land-list_name').text.strip(),
                        'Símbolo': item.find('div', class_='c-land-list_abb').text.strip(),
                        'Precio Compra': item.find('div', class_='c-land-list__price').text.strip(),
                        'Precio Venta': item.find('div', class_='c-land-list__market').text.strip(),
                        'Variación Diaria': item.find('div', class_='c-land-list__variation').text.strip()
                    })
                except AttributeError:
                    continue
            
            return pd.DataFrame(monedas)
        except Exception as e:
            print(f"Error extrayendo Ripio: {str(e)}")
            return pd.DataFrame()

    def procesar_ripio():
        """Procesa la página de Ripio"""
        driver.get(urls["Ripio Criptomonedas"])
        
        try:
            cookie_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "axeptio_btn_acceptAll"))
            )
            cookie_btn.click()
            time.sleep(1)
        except:
            pass
        
        df = extraer_tabla_ripio()
        if not df.empty:
            df = df[['Símbolo', 'Moneda', 'Precio Compra', 'Precio Venta', 'Variación Diaria']]
            df['Variación Diaria'] = df['Variación Diaria'].str[1:].str.replace('.', ',', regex=False)
            return df
        return None

    try:
        # Procesar paneles de IOL
        for panel in ["Panel General", "Panel Líderes", "Subastas"]:
            df = procesar_panel(panel)
            if df is not None:
                all_dfs.append((f"Acciones Argentinas - {panel}", df))
        
        # Procesar otras secciones de IOL
        for nombre, url in urls.items():
            if nombre.startswith("Cotizacion"):
                driver.get(url)
                mostrar_todo()
                df = obtener_tabla(nombre)
                if df is not None:
                    all_dfs.append((nombre, df))
        
        # Procesar Ripio
        df_ripio = procesar_ripio()
        if df_ripio is not None:
            all_dfs.append(("Ripio Criptomonedas", df_ripio))

        
        # Combinar todos los DataFrames
        if all_dfs:
            dfs_para_combinar = []
            for fuente, df in all_dfs:
                df['Fuente'] = fuente
                dfs_para_combinar.append(df)
            
            combined_df = pd.concat(dfs_para_combinar, ignore_index=True)
            
            # Dividir columna "Símbolo" si existe
            if 'Símbolo' in combined_df.columns:
                split_symbol = combined_df['Símbolo'].str.split(n=1, expand=True)
                if split_symbol.shape[1] == 2:
                    split_symbol.columns = ['Símbolo.1', 'Símbolo.2']
                    for col in split_symbol.columns:
                        split_symbol[col] = split_symbol[col].str.replace(r'\s+', ' ', regex=True).str.strip()
                    combined_df = combined_df.drop(columns=['Símbolo'])
                    combined_df = pd.concat([combined_df, split_symbol], axis=1)
            
            # Eliminar columnas innecesarias
            columnas_a_eliminar = ["Unnamed: 13", "Cantidad Compra", "Cantidad Venta", "Unnamed: 12"]
            for col in columnas_a_eliminar:
                if col in combined_df.columns:
                    combined_df = combined_df.drop(columns=[col])
            
            # Guardar resultados
            save_market_data(combined_df)
            return combined_df
        return None

    except Exception as e:
        print(f"Error en la descarga: {str(e)}")
        return None
    finally:
        driver.quit()
