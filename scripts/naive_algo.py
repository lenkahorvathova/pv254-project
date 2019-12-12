import sys
from random import shuffle

from scripts.utils import create_connection


# if items are less than 10, no fallback specified, returns all it has
def get_top_10_by_rating(items: list) -> list:
    with create_connection() as connection:
        cursor = connection.execute("SELECT id, overallRating FROM item WHERE id "
                                    "IN ({})".format(",".join(['"{}"'.format(item_id) for item_id in items])))
        rated_pairs = [(pair[0], pair[1]) for pair in cursor.fetchall()]

        rated_pairs.sort(key=lambda pair: pair[1], reverse=True)

        # adds a bit of randomness to the result
        rated_pairs = rated_pairs[:20]
        shuffle(rated_pairs)

        return [pair[0] for pair in rated_pairs[:10]]


def recommend_products_by_related(product_id: str, modification_type: str) -> list:
    with create_connection() as connection:

        if modification_type == "all":
            cursor = connection.execute("SELECT relatedItemId FROM item_related_list WHERE itemId=(?)", (product_id,))

        else:
            cursor = connection.execute("SELECT relatedItemId FROM item_related_list WHERE itemId=(?) AND relation=(?)",
                                        (product_id, modification_type))

        related = [item[0] for item in cursor.fetchall()]

        return get_top_10_by_rating(related)


def recommend_products_by_category(product_id: str, modification_type: str) -> list:
    with create_connection() as connection:
        cursor = connection.execute("SELECT categoryId FROM item_category_list WHERE itemId=(?)", (product_id,))
        product_category_id = cursor.fetchone()[0]

        if modification_type == "same_category":
            cursor = connection.execute("SELECT itemId FROM item_category_list "
                                        "WHERE categoryId=(?)", (product_category_id,))

        elif modification_type == "sibling_category":
            cursor = connection.execute("SELECT parentCategoryId FROM category WHERE id=(?)", (product_category_id,))
            parent_category_id = cursor.fetchone()[0]

            cursor = connection.execute("SELECT id FROM category WHERE parentCategoryId=(?)", (parent_category_id,))
            category_siblings = [item[0] for item in cursor.fetchall()]
            if parent_category_id:
                category_siblings.append(parent_category_id)

            cursor = connection.execute("SELECT itemId FROM item_category_list WHERE categoryId "
                                        "IN ({})".format(",".join([str(item) for item in category_siblings])))

        result_items = [item[0] for item in cursor.fetchall()]

        if product_id in result_items:
            result_items.remove(product_id)

        return get_top_10_by_rating(result_items)


# left here for now, so it is possible easily try out the algos
# run the script with specified algo to use, item's ID from table ITEM and modification type as an input
# - types for "by_related" algo: "also_bought", "also_viewed", "bought_together", "buy_after_viewing", "all"
# - types for "by_category" algo: "same_category", "sibling_category"
# e.g. python3 scripts/naive_algo.py --by_related "B003ERS13O" "also_bought"
# e.g. python3 scripts/naive_algo.py --by_category "B0015KOOHO" "same_category"
if __name__ == "__main__":
    algo_type = sys.argv[1]
    product_id = sys.argv[2]
    modification_type = sys.argv[3]

    recommended = []
    if algo_type == "--by_related":
        if modification_type not in ["also_bought", "also_viewed", "bought_together", "buy_after_viewing", "all"]:
            raise Exception("Only following modification of this algorithm are supported: "
                            "[\"also_bought\", \"also_viewed\", \"bought_together\", \"buy_after_viewing\", \"all\"]")
        recommended = recommend_products_by_related(product_id, modification_type)

    elif algo_type == "--by_category":
        if modification_type not in ["same_category", "sibling_category"]:
            raise Exception("Only following modification of this algo are supported: "
                            "[\"same_category\", \"sibling_category\"]")
        recommended = recommend_products_by_category(product_id, modification_type)

    else:
        raise Exception("Only following types of algorithms are supported: [--by_related_products, --by_same_category]")

    print(recommended)
