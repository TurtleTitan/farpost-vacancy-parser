from multiprocessing import Pool, cpu_count
import random
import os
import time

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys

from stem import Signal
from stem.control import Controller

from parse import parse_vanancy_page
from save import write_to_database


URL = "https://www.farpost.ru/vladivostok/job/vacancy/"         # current parsing url

DRIVER_PATH = "drivers/chromedriver_win32/chromedriver.exe"     # local chromedriver path 
VACANCY_LIMIT = 100                                             # limit on the number of uploaded vacancies

PROXY = "socks5://127.0.0.1:9150"                               # local Tor Browser proxy


def renew_tor_ip(password=''):
    '''
    Renew the current ip by sending NEWNYM singnal to the tor controller upon
    which controller assigns new IP Address.
    source: https://techmonger.github.io/68/tor-new-ip-python/
    Add these two lines to the end of the tor config file <torrc> to work properly:
        HashedControlPassword 16:872860B76453A77D60CA2BB8C1A7042072093276A3D701AD684053EC4C #(or your tor hash password)
        ControlPort 9051
    '''
    with Controller.from_port(port = 9051) as controller:
        controller.authenticate(password=password)                #   password tor hash: 
        controller.signal(Signal.NEWNYM)
        time.sleep(10)


def scroll_main_page(driver):
    action = ActionChains(driver)
    
    while True:
        for _ in range(random.randint(50, 100)):
            action.send_keys(Keys.SPACE)
            time.sleep(.1)
        action.perform()
        
        num_of_page = len(driver.find_elements_by_class_name('viewdirBulletinTable'))
        
        if num_of_page >= 20:
            break


def create_chrome_session(headless=False):

    chrome_options = webdriver.ChromeOptions()
    
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument('--proxy-server=%s' %PROXY)
    chrome_options.headless = headless

    return webdriver.Chrome(chrome_options=chrome_options, executable_path=DRIVER_PATH)


def get_vacancy_links():
    links = []

    driver = create_chrome_session()
    driver.get(URL)
    scroll_main_page(driver)

    cells = driver.find_elements_by_class_name("bull-item-content__description")
    for cell in cells:
        link = (cell
            .find_element_by_class_name('bull-item-content__subject-container')
            .find_element_by_tag_name('a')
        )
        links.append(link.get_attribute('href'))
    driver.quit()

    return links


def get_vacancy_page(driver: webdriver.Chrome):
    try:
        (driver
            .find_element_by_class_name('viewAjaxContactsPlaceHolder')
            .find_element_by_tag_name('a')
        ).click()
        time.sleep(5)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        if modal_window := soup.find('div', class_='modal-window'):
            if modal_window.find('form', class_='captchaForm'):
                return ''
    except Exception:
        print(f"Warning. Page probably does not contain contact info: {driver.current_url}")

    return driver.page_source


def get_vacancy_pages(links, limit=10):
    driver = create_chrome_session()

    for i in range(len(links))[:VACANCY_LIMIT]:
        attempts = 0
        
        while attempts != limit:
            driver.get(links[i])
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            if soup.find('form', id='grecap-form'):
                renew_tor_ip()
               
                driver.quit()
                driver = create_chrome_session()
                
                attempts += 1
            else:
                if page := get_vacancy_page(driver):
                    if os.path.exists('pages') == False:
                        os.makedirs('pages')
                    with open(f'pages/{i+1}.html', mode='w', encoding='utf-8') as f:
                        f.write(page)
                    break
        else:
            print(f"Warning. Failed to load page {i+1}: {links[i]}")

    driver.quit()


def main():
    # try:
        renew_tor_ip()

        vacancy_links = get_vacancy_links()
        get_vacancy_pages(vacancy_links)

        with Pool(processes=cpu_count()) as pool:
            data = pool.map(parse_vanancy_page, range(1, VACANCY_LIMIT+1))

        write_to_database(data)

        print('Parsing finished with status: success')
    # except Exception as err:
    #     print('Parsinf finished with status: failure')
    #     print(f'Called exception: {err}')

    

    


if __name__ == '__main__':
    main()