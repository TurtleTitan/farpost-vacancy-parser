import sqlite3
from parse import parse_vanancy_page

PAGE_LIMIT = 1000

def connect_to_db():
    conn = sqlite3.connect('vacancies.db')
    conn.execute("PRAGMA foreign_keys = 1")

    cur = conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS vacancies(
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        vacancy TEXT NOT NULL,
                        phones TEXT,
                        mails TEXT,
                        salary INTEGER,
                        FOREIGN KEY(salary) REFERENCES salary(id));""")

    cur.execute("""CREATE TABLE IF NOT EXISTS salary(
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        min INTEGER,
                        max INTEGER,
                        currency TEXT);""")
    conn.commit()
    return conn, cur


def write_to_database(vacancy_list):
    connection, cursor = connect_to_db()
    
    for vacancy in vacancy_list:
        salary_id = None
        if vacancy['salary']:
            cursor.execute("INSERT INTO salary(min, max, currency) VALUES(?, ?, ?);", (
                        vacancy['salary'].get('min'), 
                        vacancy['salary'].get('max'),
                        vacancy['salary'].get('currency')
            ))
            salary_id = cursor.lastrowid
        
        cursor.execute("INSERT INTO vacancies(vacancy, phones, mails, salary) VALUES(?, ?, ?, ?);", (
                        vacancy['vacancy'],
                        '\n'.join(vacancy['contacts']['phones']),
                        '\n'.join(vacancy['contacts']['mails']),
                        salary_id
        ))
        connection.commit()
    connection.close()


def main():
    data = []
    for i in range(PAGE_LIMIT):
        page = parse_vanancy_page(i+1)
        data.append(page)
    
    write_to_database(data)


if __name__ == "__main__":
    main()

    