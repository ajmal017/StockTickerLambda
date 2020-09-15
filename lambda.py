import requests
import psycopg2


def handler(event=None, context=None):
    update_tickers()
    return


def update_tickers():
    NasdaqListedURL = 'http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt'
    OtherListedURL = 'http://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt'

    NasdaqBytesData = requests.get(NasdaqListedURL).content
    OtherBytesData = requests.get(OtherListedURL).content

    NasdaqData = NasdaqBytesData.decode('utf-8')
    OtherData = OtherBytesData.decode('utf-8')

    updatedData = [NasdaqData, OtherData]

    try:
        connection = psycopg2.connect(user="XXXXXX",
                                      password="XXXXX",
                                      host="XXXXXXXXXXXXX",
                                      port="XXXX",
                                      database="XXXX")

        cursor = connection.cursor()

    except (Exception, psycopg2.Error) as error:
        print(error)
        raise error

    try:
        cursor.execute("""SELECT "Ticker" FROM "Tickers";""")
        DBdata = cursor.fetchall()
        DBdata = [item[0] for item in DBdata]

    except (Exception, psycopg2.Error) as error:
        print(error)
        raise error

    stocks = []
    addStocks = []
    deleteStocks = []

    for item in updatedData:
        for line in item.splitlines():
            separatedLine = line.split('|', maxsplit=1)
            ticker = separatedLine[0].strip()

            try:
                second = separatedLine[1]
                companyName = second[:separatedLine[1].index('-')].strip()
            except ValueError:
                companyName = separatedLine[1][:separatedLine[1].index('|')].strip()

            if 'Symbol' not in ticker and 'TEST' not in companyName and 'File' not in ticker:
                stocks.append(ticker)

    # for item in stocks:
    #     if len(item) >= 6:
    #         addStocks.append(item)

    for item in stocks:
        if item not in DBdata:
            addStocks.append(item)

    for item in DBdata:
        if item not in stocks:
            deleteStocks.append(item)

    for item in addStocks:
        try:
            cursor.execute("""INSERT INTO "Tickers"("Ticker") VALUES(%s);""", (item,))
            connection.commit()
        except Exception as error:
            print(error)
            connection.rollback()

    for item in deleteStocks:
        try:
            cursor.execute("""DELETE FROM "Tickers" WHERE "Ticker" = (%s);""", (item,))
            connection.commit()
        except Exception as error:
            print(error)
            connection.rollback()

    if connection:
        cursor.close()
        connection.close()


if __name__ == '__main__':
    update_tickers()

