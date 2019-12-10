import html
from random import shuffle

from flask import Flask, render_template, abort, redirect, request

from scripts.collaborative_filtering import collaboration_filtering
from scripts.content_based_algo import algorithm_related, algorithm_related_with_category
from scripts.naive_algo import recommend_products_by_related, recommend_products_by_category
from scripts.random_algo import recommend_products_randomly
from scripts.utils import create_connection

app = Flask(__name__)
TYPES_OF_ALGORITHMS = [
    "random",
    "related_all",
    "related_also_bought",
    "related_also_viewed",
    "same_category",
    "sibling_category",
    "collaborative_filtering",
    "content_based",
    "content_based_with_category"
]


def get_item_dict(item_id: str) -> dict:
    connection = create_connection()

    with connection:
        cursor = connection.execute('''SELECT * FROM item WHERE id="{}"'''.format(item_id))
        item = cursor.fetchone()

        if item is None:
            abort(404)

        return {
            "id": item[0],
            "title": html.unescape(item[1]) if item[1] and item[1] != "None" else "<TITLE>",
            "description": html.unescape(item[2]) if item[2] and item[2] != "None" else "",
            "price": item[3] if item[3] else "<PRICE>",
            "imageUrl": item[4] if item[4] else "",
            "salesCategory": html.unescape(item[5]) if item[5] and item[5] != "None" else "",
            "salesRank": item[6],
            "overallRating": item[7],
            "percentageRating": ((item[7] * 100) / 5)
        }


def get_recommendation(item_id: str, algo_type: str) -> (list, str):
    products = []

    if algo_type == "random":
        products = recommend_products_randomly(item_id)

    elif algo_type == "related_all":
        products = recommend_products_by_related(item_id, "all")

    elif algo_type == "related_also_bought":
        products = recommend_products_by_related(item_id, "also_bought")

    elif algo_type == "related_also_viewed":
        products = recommend_products_by_related(item_id, "also_viewed")

    elif algo_type == "same_category":
        products = recommend_products_by_category(item_id, "same_category")

    elif algo_type == "sibling_category":
        products = recommend_products_by_category(item_id, "sibling_category")

    elif algo_type == "collaborative_filtering":
        products = collaboration_filtering(item_id, 10)

    elif algo_type == "content_based":
        products = algorithm_related(item_id, 10)

    elif algo_type == "content_based_with_category":
        products = algorithm_related_with_category(item_id, 10)

    result = [get_item_dict(product) for product in products]
    return result, algo_type


def get_random_item_id():
    connection = create_connection()

    with connection:
        cursor = connection.execute("SELECT id FROM item ORDER BY RANDOM() LIMIT 1")
        item_id = cursor.fetchone()[0]

    return item_id


def get_int_value(value: str):
    if value is None:
        return "NULL"

    value_int = {
        "great": 4,
        "good": 3,
        "bad": 2,
        "horrible": 1
    }

    return value_int[value]


@app.route("/")
def index():
    connection = create_connection()

    with connection:
        cursor = connection.execute("SELECT id FROM item ORDER BY RANDOM() LIMIT 120")
        all_items = [get_item_dict(item[0]) for item in cursor.fetchall()]

    return render_template("main_page.html", products=all_items)


@app.route("/product/<item_id>")
def detail(item_id):
    item_info = get_item_dict(item_id)
    feedback = request.args.get('submitted_feedback', False)

    algos = []
    for algo_type in TYPES_OF_ALGORITHMS:
        algos.append(get_recommendation(item_id, algo_type))

    shuffle(algos)
    algos = list(filter(lambda x: len(x[0]) == 10, algos))

    message = None
    if feedback:
        message = "Thank you for your feedback!"
    return render_template("product_page.html", product=item_info, algos=algos, message=message)


@app.route("/product/random")
def random_detail():
    item_id = get_random_item_id()
    new_url = "/product/{}".format(item_id)

    return redirect(new_url)


@app.route("/product/<item_id>/feedback", methods=["POST"])
def feedback(item_id: str):
    connection = create_connection()

    with connection:
        to_insert = [item_id]
        for algo_type in TYPES_OF_ALGORITHMS:
            to_insert.append(get_int_value(request.form.get(algo_type, None)))

        query = '''
            INSERT INTO algo_evaluation(
                itemId, 
                random, 
                relatedAll, relatedAlsoBought, relatedAlsoViewed, 
                sameCategory, siblingCategory, 
                collaborativeFiltering,
                contentBased, contentBasedWithCategory)
            VALUES {}
        '''.format(tuple(to_insert))
        connection.execute(query)

    item_id = get_random_item_id()
    new_url = "/product/{}?submitted_feedback=true".format(item_id)

    return redirect(new_url)


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000, debug=True)
