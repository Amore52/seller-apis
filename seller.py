import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон.

    Запрашивает список товаров с помощью API магазина ОЗОН.

    Аргументы:
        last_id (str): Идентификатор последнего товара для пагинации.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для авторизации.

    Возвращает:
        list: Список товаров в формате JSON.

    Пример:
        >>> get_product_list("", "your_client_id", "your_seller_token")
        [{'offer_id': '123', 'name': 'Product 1'}, ...]

    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон.

    Запрашивает артикулы всех товаров.

    Аргумениы:
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для авторизации.

    Возвращает:
        list: Список артикулов товаров.

    Пример:
        >>> get_offer_ids("your_client_id", "your_seller_token")
        ['offer_id_1', 'offer_id_2', ...]

    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров.

    Обновляет цены указанных товаров через API ОЗОН.

    Аргументы:
        prices (list): Список цен для обновления.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для авторизации.

    Возвращает:
        dict: Результат выполнения запроса в формате JSON.

    Пример:
        >>> update_price([{'offer_id': '123', 'price': 100}], "your_client_id", "your_seller_token")
        {'success': True}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки.

    Обновляет остатки указанных товаров через API ОЗОН.

    Аргументы:
        stocks (list): Список остатков для обновления.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для авторизации.

    Возвращает:
        dict: Результат выполнения запроса в формате JSON.

    Пример:
        >>> update_stocks([{'offer_id': '123', 'stock': 50}], "your_client_id", "your_seller_token")
        {'success': True}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл остатки с сайта casio.

    Загружает файл остатков часов с указанного URL и возвращает данные.

    Возвращает:
        list: Список остатков часов в формате словаря.

    Пример:
        >>> download_stock()
        [{'Код': '123', 'Количество': '10', 'Цена': '5\'990.00 руб.'}, ...]
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать список остатков для обновления.

        Формирует список остатков на основе данных о часах и артикулов.

        Аргументы:
            watch_remnants (list): Данные о остатках часов.
            offer_ids (list): Список артикулов товаров.

        Возвращает:
            list: Список остатков для обновления.

        Пример:
            >>> create_stocks([{'Код': '123', 'Количество': '10'}], ['123'])
            [{'offer_id': '123', 'stock': 10}, ...]

    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список цен для обновления.

        Формирует список цен на основе данных о часах и артикулов.

        Аргументы:
            watch_remnants (list): Данные о остатках часов.
            offer_ids (list): Список артикулов товаров.

        Возвращает:
            list: Список цен для обновления.

        Пример:
            >>> create_prices([{'Код': '123', 'Цена': '5\'990.00 руб.'}], ['123'])
            [{'offer_id': '123', 'price': 5990}, ...]

    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену из строки в формат, удобный для использования.

    Преобразует цену, заданную в строковом формате (например, '5\'990.00 руб.') в целое число,
    убирая символы, которые не являются цифрами.

    Аргумент:
        price (str): Цена в строковом формате, включая символы валюты и разделители.

    Возвращает:
        str: Цена в формате целого числа без символов валюты.

    Примеры:
        >>> price_conversion("5'990.00 руб.")
        '5990'

    Некорректные примеры:
        >>> price_conversion("")
        '0' Пустая строка вернёт '0'

        >>> price_conversion("abc")
        '0' Неправильный формат вернёт '0'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов.

    Аргументы:
        lst (list): Исходный список.
        n (int): Количество элементов в каждой части.

    Возвращает:
        list: Подсписки длиной n.

    Пример:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загрузить цены для обновления.

        Получает артикулы и обновляет цены для них.

        Аргументы:
            watch_remnants (list): Данные о остатках часов.
            client_id (str): Идентификатор клиента.
            seller_token (str): Токен продавца для авторизации.

        Возвращает:
            list: Список обновлённых цен.

        Пример:
            >>> await upload_prices(watch_remnants, "your_client_id", "your_seller_token")
            [{'offer_id': '123', 'price': 5990}, ...]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загрузить остатки для обновления.

        Получает артикулы и обновляет остатки для них.

        Аргументы:
            watch_remnants (list): Данные о остатках часов.
            client_id (str): Идентификатор клиента.
            seller_token (str): Токен продавца для авторизации.

        Возвращает:
            tuple: Список не пустых остатков и полный список остатков.

        Пример:
            >>> await upload_stocks(watch_remnants, "your_client_id", "your_seller_token")
            ([{'offer_id': '123', 'stock': 10}, ...], [{'offer_id': '123', 'stock': 10}, ...])

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
