from flask import Flask, render_template

from scripts.utils import create_connection

app = Flask(__name__)


@app.route("/")
def index():
    connection = create_connection()

    with connection:
        cursor = connection.execute("SELECT * FROM item ORDER BY RANDOM() LIMIT 100")
        all_items = [{
            "id": item[0],
            "title": item[1].replace("&amp;", "&"),
            "description": item[2],
            "price": item[3],
            "imageUrl": item[4],
            "salesCategory": item[5],
            "salesRank": item[6],
            "overallRating": item[7],
            "percentageRating": ((item[7] * 100) / 5)
        } for item in cursor.fetchall()]

    return render_template("main_page.html", products=all_items)
