import html
from random import shuffle

from flask import Flask, render_template, abort, redirect, request

from scripts.collaborative_filtering import collaboration_filtering, hybrid_algorithm
from scripts.naive_algo import recommend_products_by_related, recommend_products_by_category
from scripts.random_algo import recommend_products_randomly
from scripts.utils import create_connection

app = Flask(__name__)


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

    elif algo_type == "related_bought_together":
        products = recommend_products_by_related(item_id, "bought_together")

    elif algo_type == "related_buy_after_viewing":
        products = recommend_products_by_related(item_id, "buy_after_viewing")

    elif algo_type == "same_category":
        products = recommend_products_by_category(item_id, "same_category")

    elif algo_type == "sibling_category":
        products = recommend_products_by_category(item_id, "sibling_category")

    elif algo_type == "collaborative_filtering":
        products = collaboration_filtering(item_id, 10)

    result = [get_item_dict(product) for product in products]
    return result, algo_type


def get_random_item_id():
    connection = create_connection()

    with connection:
        cursor = connection.execute("SELECT id FROM item ORDER BY RANDOM() LIMIT 1")
        item_id = cursor.fetchone()[0]

    return item_id


@app.route("/")
def index():
    connection = create_connection()

    with connection:
        cursor = connection.execute("SELECT id FROM item ORDER BY RANDOM() LIMIT 100")
        all_items = [get_item_dict(item[0]) for item in cursor.fetchall()]

    return render_template("main_page.html", products=all_items)


@app.route("/product/<item_id>")
def detail(item_id):
    item_info = get_item_dict(item_id)
    feedback = request.args.get('submitted_feedback', False)

    algos = [
        get_recommendation(item_id, "random"),
        get_recommendation(item_id, "related_all"),
        get_recommendation(item_id, "related_also_bought"),
        get_recommendation(item_id, "related_also_viewed"),
        get_recommendation(item_id, "related_bought_together"),
        get_recommendation(item_id, "related_buy_after_viewing"),
        get_recommendation(item_id, "same_category"),
        get_recommendation(item_id, "sibling_category"),
        get_recommendation(item_id, "collaborative_filtering")
    ]

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
    print(item_id, request.form)
    # what info do we want to store?

    item_id = get_random_item_id()
    new_url = "/product/{}?submitted_feedback=true".format(item_id)

    return redirect(new_url)


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000, debug=True)
