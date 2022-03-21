from multiprocessing import Pool, cpu_count

from bs4 import BeautifulSoup

VACANCY_LIMIT = 100


def get_salary_info(soup):
    info = {}

    if salary_span := soup.find(
        'span', {'data-field': "salaryMin-salaryMax-salaryDescription"}
    ):
        salary = salary_span.text.lower()
        salary = salary.replace('\xa0', '').replace(' ', '')
        
        if salary.startswith('от') or salary.startswith('до'):
            info['min'] = int(salary[2:-1])
            info['currency'] = salary[-1]
        elif '–' in salary:
            index = salary.index('–')
            info['min'] = int(salary[:index])
            info['max'] = int(salary[index + 1:-1])
            info['currency'] = salary[-1]
      
    return info
    

def get_vacancy_info(soup):
    if container := soup.find('div', id='fieldsetView'):
        vacancy = (container
            .find('div', class_='value')
            .find('span', {"data-field" : "type"})
        )
        return vacancy.text.strip()

    return ""


def get_contacts_info(soup):
    contacts = {
        "phones": [],
        "mails": [],
    }

    if contact_block := soup.find('div', class_="new-contacts"):
        if phone_blocks := contact_block.find_all('div', class_='new-contact__phone'):    
            contacts['phones'] = [
                phone.text.strip() for phone in phone_blocks
            ]
    
        if mail_blocks := contact_block.find_all('div', class_="new-contact_email"):
            for mail_block in mail_blocks:
                mail = mail_block.parent.find('a')
                contacts['mails'].append(mail.text.strip())
            
    return contacts


def read_vacancy_page(page_number):                         
    with open(f'pages/{page_number}.html', encoding='utf-8') as p:
        html = p.read()
    return html


def parse_vanancy_page(page):
    html = read_vacancy_page(page)
    soup = BeautifulSoup(html, 'html.parser')
    
    info = {
        'vacancy': get_vacancy_info(soup),
        'salary': get_salary_info(soup),
        'contacts': get_contacts_info(soup)
    }

    return info


def main():
    with Pool(processes=cpu_count()) as pool:
        vacancies = pool.map(parse_vanancy_page, range(1, 101))

    for vacancy in vacancies:
        print(vacancy)
    

if __name__ == "__main__":
    main()