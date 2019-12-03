import json
import time

from scripts.utils import create_connection

OUTPUT_FILE = "data/calculate_ratings_output.json"


def calculate_ratings():
    connection = create_connection()

    # prepare variables for statistics
    without_reviews = 0
    without_reviews_list = []
    with_reviews = 0
    with_reviews_list = []
    with_reviews_json = {}

    print("Starting calculation of ratings...")
    time.sleep(2)

    with connection:
        # prepare table for items' ratings
        # new table is created to speed up process (UPDATE query would be expensive)
        connection.execute('''CREATE TABLE IF NOT EXISTS item_rating (
                                itemId TEXT NOT NULL,
                                rating REAL,
                                FOREIGN KEY (itemId) REFERENCES item(id))''')
        cursor = connection.execute("DELETE FROM item_rating")
        cursor.close()

        # get IDs of all items in table ITEM
        cursor = connection.execute("SELECT id FROM item")
        all_items = [item[0] for item in cursor.fetchall()]
        num_of_all_items = len(all_items)
        cursor.close()

        # prepare batches for queries to faster calculate ratings
        input_batches = []
        curr_batch = []
        for index, item_id in enumerate(all_items):
            curr_batch.append(item_id)

            if len(curr_batch) == 1000:
                input_batches.append(curr_batch)
                curr_batch = []
        input_batches.append(curr_batch)

        # map to use for insertion into table later {itemID: overall_rating}
        item_rating_map = {}

        # calculate overall ratings for items batch by batch
        num_of_batches = len(input_batches)
        for i, input_batch in enumerate(input_batches):
            print("processing batch {}/{}".format(i + 1, num_of_batches))

            # select all ratings for the current batch
            query = '''SELECT itemId, rating FROM review
                       WHERE itemId IN ({})'''.format(",".join(['"{}"'.format(item_id) for item_id in input_batch]))
            cursor = connection.execute(query)
            all_ratings = [(pair[0], pair[1]) for pair in cursor.fetchall()]
            cursor.close()

            # cumulate ratings for items
            rating_by_id = {}
            for item_id, rating in all_ratings:
                if item_id in rating_by_id:
                    rating_by_id[item_id].append(rating)
                else:
                    rating_by_id[item_id] = [rating]

            # calculate ratings for all items in the batch
            for index, item_id in enumerate(input_batch):
                # get item's ratings
                all_item_ratings = []
                if item_id in rating_by_id:
                    all_item_ratings = rating_by_id[item_id]
                num_of_ratings = len(all_item_ratings)

                # if item has reviews, calculate overall value
                if num_of_ratings > 0:
                    overall_rating = round((sum(all_item_ratings) / num_of_ratings), 2)
                    item_rating_map[item_id] = overall_rating

                    # update variables for statistics
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

        # prepare batches for updating DB
        output_batches = []
        curr_batch = []
        for index, pair in enumerate(item_rating_map.items()):
            if len(curr_batch) == 1000:
                output_batches.append(curr_batch)
                curr_batch = []
            curr_batch.append(pair)
        output_batches.append(curr_batch)

        # insert calculated overall ratings for items into table ITEM_RATING batch by batch
        for batch in output_batches:
            query = '''INSERT INTO item_rating(itemId, rating) 
                       VALUES {}'''.format(",".join(['("{}", {})'.format(key, value) for (key, value) in batch]))
            connection.execute(query)
            connection.commit()

    # create a file with all statistics from this calculation
    result_json = {
        "all_items": num_of_all_items,
        "without_reviews": {
            "num_of_items": without_reviews,
            "list": without_reviews_list
        },
        "with_reviews": {
            "num_of_items": with_reviews,
            "list": with_reviews_list,
            "items": with_reviews_json
        }
    }

    with open(OUTPUT_FILE, 'w') as file:
        file.write(json.dumps(result_json, indent=2))

    print("--------------------------------------")
    print("Calculating of ratings was SUCCESSFUL!")
    print("--------------------------------------")
    time.sleep(2)

    # return a path to the file with statistics
    return OUTPUT_FILE


if __name__ == "__main__":
    calculate_ratings()
