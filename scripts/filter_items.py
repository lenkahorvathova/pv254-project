import json
import sqlite3
import time

from scripts.utils.calculate_ratings import calculate_ratings

PATH_TO_DB = "data/amazon_product_data.db"


def filter_items():
    connection = sqlite3.connect(PATH_TO_DB)

    # get IDs of all items in table ITEM
    cursor = connection.execute("SELECT id FROM item")
    all_items = [item[0] for item in cursor.fetchall()]
    num_of_all_items = len(all_items)
    cursor.close()

    # calculate overall ratings from reviews for all items
    # and get a path of the result file with stats
    print("Starting calculation of ratings...")
    time.sleep(2)
    stat_file_path = calculate_ratings()

    # get IDs of all items with ratings
    cursor = connection.execute("SELECT itemId FROM item_rating")
    rated_items = [item[0] for item in cursor.fetchall()]
    num_of_rated_items = len(rated_items)
    cursor.close()

    # get IDs of all items without reviews
    with open(stat_file_path) as stat_file:
        stat_file_dict = json.load(stat_file)
        # path to the list of items without reviews in stat_file
        not_rated_items = stat_file_dict["without_reviews"]["list"]
        num_of_not_rated_items = len(not_rated_items)

    # check if items' count are correct
    if num_of_all_items != num_of_rated_items + num_of_not_rated_items:
        raise Exception("Sum of rated and unrated items is not equal to number of all items "
                        "({} + {} != {}) !!!".format(num_of_rated_items, num_of_not_rated_items, num_of_all_items))

    print("Starting filtration of unrated items...")
    time.sleep(2)

    # prepare batches of "not_rated_items" for faster deleting
    input_batches = []
    curr_batch = []
    for index, item_id in enumerate(not_rated_items):
        curr_batch.append(item_id)

        if len(curr_batch) == 1000:
            input_batches.append(curr_batch)
            curr_batch = []
    input_batches.append(curr_batch)

    # deletes all items without reviews batch after batch
    num_of_batches = len(input_batches)
    for i, batch in enumerate(input_batches):
        print("processing batch {}/{}".format(i, num_of_batches))

        query = '''DELETE FROM item
                   WHERE id IN ({})'''.format(",".join(['"{}"'.format(item_id) for item_id in batch]))
        connection.execute(query)
        connection.commit()

    # check if deletion was done correctly
    cursor = connection.execute("SELECT id FROM item")
    all_items_after_filtering = [item[0] for item in cursor.fetchall()]
    num_of_all_items_after_filtering = len(all_items_after_filtering)

    if num_of_all_items_after_filtering != num_of_rated_items:
        raise Exception("Number of items in table ITEM are not equal to number of rated items "
                        "({} != {}) !!!".format(num_of_all_items_after_filtering, num_of_rated_items))

    print("-----------------------------------------")
    print("Deleting of unrated items was SUCCESSFUL!")
    print("-----------------------------------------")


if __name__ == "__main__":
    filter_items()
