import psycopg
import logging
from requests_html import HTMLSession
from configuration import load_config

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s %(name)8s %(levelname)s %(message)s')
# handler = logging.FileHandler('LOGS.log')
# handler.setFormatter(formatter)
# handler.setLevel(logging.DEBUG)
# logger.addHandler(handler)

## use the following logger aka leave above commented
# logging.basicConfig(filename='LOGS.log',level= logging.INFO,
#                     format='%(asctime)s %(name)8s %(levelname)s %(message)s',
#                     datefmt='%m/%d/%Y %I:%M:%S %p')

CONFIG = load_config()

#make connection to DB
try:
    conn = psycopg.connect(host='localhost', dbname=CONFIG['db']['name'], user=CONFIG['db']['user'],
                           password=CONFIG['db']['password'])
    cursor = conn.cursor()
    print('DB connection successful')
except Exception as e:
    print(f'Failed to connect to DB: {e}')


def get_completed_auction_links(page):
    with HTMLSession() as session:
        url = f'https://carsandbids.com/past-auctions/?page={page}'
        response = session.get(url)
        response.html.render(sleep=2, keep_page=True, scrolldown=1, timeout=10000)
        auctions = response.html.find('.auction-item ')
        links = [auction.absolute_links for auction in auctions]
        return links


def parse_auctions(url):
    with HTMLSession() as session:
        response = session.get(url)
        response.html.render(sleep=2, keep_page=True,scrolldown=1, timeout=10000)
        link = link_cleaner(url)
        title = response.html.find('div.row:nth-child(1) > div:nth-child(1) > div:nth-child(1) > h1:nth-child(1)', first=True).text
        print(title)
        print(link)
        image = response.html.find('#gallery-preview-ref > div.preload-wrap.main.loaded > img', first=True).attrs['src']
        year = title.split()[0]
        make = response.html.find('div.quick-facts > dl:nth-child(1) > dd:nth-child(2) > a', first=True).text
        model = response.html.find('div.quick-facts > dl:nth-child(1) > dd.subscribeable > a', first=True).text
        mileage = response.html.find('div.quick-facts > dl:nth-child(1) > dd:nth-child(6)', first=True).text.replace(',','')
        body_style = response.html.find('div.quick-facts > dl:nth-child(2) > dd:nth-child(8)', first=True).text
        transmission = response.html.find('div.quick-facts > dl:nth-child(2) > dd:nth-child(6)', first=True).text
        drivetrain = response.html.find('div.quick-facts > dl:nth-child(2) > dd:nth-child(4)', first=True).text
        location = response.html.find('div.quick-facts > dl:nth-child(1) > dd:nth-child(12) > a', first=True).text
        seller = response.html.find('div.quick-facts > dl:nth-child(1) > dd.seller > div > div.text > a', first=True).text
        seller_type = response.html.find('div.quick-facts > dl:nth-child(2) > dd:nth-child(14)', first=True).text
        auction_ended = response.html.find('div.row.auction-bidbar > div.col.width-constraint > div > div > ul >'
                                           ' li.time > span > span', first=True).text
        sale_details = response.html.find('div.row.auction-bidbar > div.col.width-constraint > div > div > ul >'
                                          ' li.ended > span.value', first=True).text
        if not year.isdigit():
            year = None
        if not mileage.isdigit():
            mileage = None
        if 'Auction Cancelled' in sale_details:
            bids = None
            comments = None
            price = None
            sold_or_bid_to = 'cancelled'
        else:
            if 'bid' in sale_details.lower() or 'sold after' in sale_details.lower():
                sold_or_bid_to = 'bid to'
            elif 'sold' in sale_details.lower():
                sold_or_bid_to = 'sold'
            else:
                sold_or_bid_to = None
            bids = response.html.find('div.row.auction-bidbar > div.col.width-constraint > div > div > ul >'
                                      ' li.num-bids > span.value', first=True).text
            comments = response.html.find('div.row.auction-bidbar > div.col.width-constraint > div > div > ul >'
                                          ' li.num-comments > span.value', first=True).text
            price = sale_details.split()[2].replace('$', '').replace(',', '')
        try:
            state = location.split(',')[1].split()[0]
            city = location.split(',')[0]
            zip_code = location.split(',')[1].split()[1].replace('(', '').replace(')', '')
            province = None
            if not zip_code.isdigit():
                province = state
                state = zip_code
                zip_code = None
        except IndexError:
            print(f'Located in Canada.')
            try:
                x = location.split(',')
                state = x[2]
                city = x[0]
                province = x[1]
                zip_code = None
            except IndexError:
                state = location.split(',')[1].split()[0]
                if state.lower() == 'canada':
                    print('Canada, but location is only with one ","')
                    length = len(location.split(',')[0].split())
                    city = ' '.join(location.split(',')[0].split()[:length - 1])
                    zip_code = None
                    province = location.split(',')[0].split()[-1]
                else:
                    print('US, no zip code provided')
                    city = location.split(',')[0]
                    zip_code = None
                    province = None

        details = (link, title, bids, comments, auction_ended, sold_or_bid_to, price, seller, seller_type, year, make,
                   model, transmission, drivetrain, body_style, mileage, state, city, zip_code, province, image)
        return details


def write_to_db(auction_data):
    print(f'writing to DB: {auction_data}')
    for data in auction_data:
        cursor.execute('''INSERT INTO auctions (link,title,bids,comments,date_ended,sold_or_bid,price,
        seller_name, seller_type,year,make,model,trans_type,drivetrain,body_style,mileage,state,city,zip,province,image)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', data)
    conn.commit()


def get_link_in_db():
    cursor.execute('SELECT link FROM auctions')
    return cursor.fetchall()


def link_cleaner(raw_link):
    l = raw_link.split('/')
    l.pop()
    return '/'.join(l)


def main():
    #pull list of auction links already in the DB
    db_links = [link[0] for link in get_link_in_db()]
    print(len(db_links))

    auctions_on_page = []

    #scrape links from page
    auction_links = get_completed_auction_links(1)
    for links in auction_links:
        #link is in a tuple. loop to get link
        for link in links:
            #checking that link is not already in DB
            if link_cleaner(link) not in db_links:
                auctions_on_page.append(parse_auctions(link))
    if auctions_on_page:
        write_to_db(auctions_on_page)
        #clear list of auctions after writting to DB to prepare for next page
        auctions_on_page.clear()
        #print('wrote auctions to database. moving on to next page...')


if __name__ == '__main__':
    main()










