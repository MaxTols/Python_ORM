import sqlalchemy
from sqlalchemy.orm import sessionmaker

import json

from models import create_tables, Publisher, Book, Stock, Sale, Shop

import os
from dotenv import load_dotenv

load_dotenv()

login = os.getenv('LOGIN')
password = os.getenv('PASSWORD')
db = os.getenv('DATABASE')

DSN = 'postgresql://%(login)s:%(password)s@localhost:5432/%(db)s' % {'login': login, 'password': password, 'db': db}
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('test_data.json', 'r') as f:
    data = json.load(f)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()


def show_info(publish):
    if type(publish) == str:
        publish = session.query(Publisher.id).filter(Publisher.name.ilike(publish)).all()[0][0]

    q = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale, Stock.count)\
        .join(Publisher).join(Stock).join(Sale).join(Shop).filter(Publisher.id == f'{publish}')
    for s in q.all():
        print(f'{s[0]} | {s[1]} | {s[2]} | {s[3]} | {s[4]}')
    session.close()


if __name__ == '__main__':
    show_info('O’Reilly')
    print()
    show_info(3)
    print()
    show_info(4)
