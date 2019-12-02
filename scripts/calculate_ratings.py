import json
import sqlite3

PATH_TO_DB = "data/amazon_product_data.db"


def calculate_ratings():
    connection = sqlite3.connect(PATH_TO_DB)

    without_reviews = 0
    without_reviews_list = []
    with_reviews = 0
    with_reviews_list = []
    with_reviews_json = {}

    with connection:
        connection.execute('''CREATE TABLE IF NOT EXISTS item_rating (
                                itemId TEXT NOT NULL,
                                rating REAL,
                                FOREIGN KEY (itemId) REFERENCES item(id))''')

        cursor = connection.execute("SELECT id FROM item")
        all_items = [item[0] for item in cursor.fetchall()]
        num_of_all_items = len(all_items)
        cursor.close()

        input_batches = []
        curr_batch = []
        for index, item_id in enumerate(all_items):
            curr_batch.append(item_id)

            if len(curr_batch) == 1000:
                input_batches.append(curr_batch)
                curr_batch = []
        input_batches.append(curr_batch)

        item_rating_map = {}

        i = 1
        count_batches = len(input_batches)
        for input_batch in input_batches:
            query = '''SELECT itemId, rating FROM review
                       WHERE itemId IN ({})'''.format(",".join(['"{}"'.format(item_id) for item_id in input_batch]))
            cursor = connection.execute(query)
            all_ratings = [(pair[0], pair[1]) for pair in cursor.fetchall()]
            cursor.close()

            rating_by_id = {}
            for item_id, rating in all_ratings:
                if item_id in rating_by_id:
                    rating_by_id[item_id].append(rating)
                else:
                    rating_by_id[item_id] = [rating]

            for index, item_id in enumerate(input_batch):
                all_item_ratings = []
                if item_id in rating_by_id:
                    all_item_ratings = rating_by_id[item_id]
                num_of_ratings = len(all_item_ratings)

                if num_of_ratings > 0:
                    overall_rating = round((sum(all_item_ratings) / num_of_ratings), 2)
                    item_rating_map[item_id] = overall_rating

                    with_reviews += 1
                    with_reviews_list.append(item_id)
                    with_reviews_json[item_id] = {
                        "ratings": str(all_item_ratings),
                        "num_of_reviews": num_of_ratings,
                        "overall_rating": overall_rating
                    }
                else:
                    without_reviews += 1
                    without_reviews_list.append(item_id)

            print("{}/{}".format(i, count_batches))
            i += 1

        output_batches = []
        curr_batch = []
        for index, pair in enumerate(item_rating_map.items()):
            if len(curr_batch) == 1000:
                output_batches.append(curr_batch)
                curr_batch = []
            curr_batch.append(pair)
        output_batches.append(curr_batch)

        for batch in output_batches:
            query = '''INSERT INTO item_rating(itemId, rating) VALUES {}'''.format(
                ",".join(['("{}", {})'.format(key, value) for (key, value) in batch]))
            connection.execute(query)
            connection.commit()

    result_json = {
        "all_items": num_of_all_items,
        "without_reviews": {
            "num_of_items": without_reviews
        },
        "with_reviews": {
            "num_of_items": with_reviews,
            "items": with_reviews_json
        }
    }

    with open("data/calculate_ratings_output.json", 'w') as file:
        file.write(json.dumps(result_json, indent=2))


if __name__ == "__main__":
    calculate_ratings()
