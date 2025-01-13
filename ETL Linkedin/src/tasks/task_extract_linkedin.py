import requests
from bs4 import BeautifulSoup
from prefect import task

url = 'https://www.linkedin.com/jobs/search/?currentJobId=4122356494&f_TPR=r86400&geoId=102927786&keywords=python'

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

@task(name="Extracción de ofertas de LinkedIn")
def task_extract_linkedin():
    response = requests.get(url, headers=headers)
    ofertas = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        ul_offers = soup.find('ul',{'class':'jobs-search__results-list'})
        lista_ofertas = ul_offers.find_all('li')

        for oferta in lista_ofertas:
            nombre = oferta.find('h3',{'class':'base-search-card__title'}).get_text().strip()
            ubicacion = oferta.find('span',{'class':'job-search-card__location'}).get_text().strip()
            url_valor = oferta.find('a')['href'].strip() 
            
            ofertas.append({"nombre": nombre, "ubicacion":ubicacion, "url": url_valor})
        
    else:
        print(f'Error {response.status_code} {response.reason}')
    
    return ofertas