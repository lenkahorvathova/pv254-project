import sys

from scripts.utils import create_connection


def recommend_products(product_id: str) -> list:
    connection = create_connection()

    with connection:
        cursor = connection.execute("SELECT id FROM item ORDER BY RANDOM() LIMIT 10;")
        recommended_products = [item[0] for item in cursor.fetchall()]

        # to ensure that the product_id is not in its own list of recommended products
        while product_id in recommended_products:
            recommended_products.remove(product_id)
            cursor = connection.execute("SELECT id FROM item ORDER BY RANDOM() LIMIT 1;")
            recommended_products.append(cursor.fetchone()[0])

        return recommended_products


# left here for now, so it is possible easily try out the algo
# run the script with item's ID from table ITEM as an input
# e.g. python3 script/random_algo.py "B000EGELPU"
if __name__ == "__main__":
    product_id = sys.argv[1]
    recommended = recommend_products(product_id)
    print(recommended)
