import sqlite3


def recommend_products(product_id: str) -> list:
    connection = sqlite3.connect("data/amazon_product_data.db")
    cursor = connection.execute("SELECT id FROM item ORDER BY RANDOM() LIMIT 10;")
    recommended_products = [item[0] for item in cursor.fetchall()]

    # to ensure that the product_id is not in its own list of recommended products
    while product_id in recommended_products:
        print(product_id)
        recommended_products.remove(product_id)
        cursor = connection.execute("SELECT id FROM item ORDER BY RANDOM() LIMIT 1;")
        recommended_products.append(cursor.fetchone()[0])

    return recommended_products
