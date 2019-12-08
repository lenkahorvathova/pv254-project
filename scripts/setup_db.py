import argparse
import gzip
import sqlite3
import time
from datetime import datetime

from scripts.utils import create_connection

LAST_TIME_CHECKPOINT = None


class DBSetup:
    SCHEMA = 'schema.sql'

    def __init__(self):
        self.args = self.parse_commandline()

    @staticmethod
    def parse_commandline():
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument("--review_file", type=str, required=True,
                            help="This is a path to an input file (.json.gz) with reviews.")
        parser.add_argument("--meta_file", type=str, required=True,
                            help="This is a path to an input file (.json.gz) with metadata.")

        return parser.parse_args()

    def log(self, msg: str):
        print(msg)
        time.sleep(2)

    def log_billboard(self, msgs: list):
        global LAST_TIME_CHECKPOINT
        print("--------------------------------------")
        for msg in msgs:
            print(msg)
        print("This part took: {}".format(datetime.now() - LAST_TIME_CHECKPOINT))
        print("--------------------------------------")
        LAST_TIME_CHECKPOINT = datetime.now()
        time.sleep(2)

    def escape(self, input: str):
        if input:
            replaced = input.replace('"', '')
            return replaced
        return input

    def prepare_tables(self, db_con):
        with open(self.SCHEMA) as schema:
            for sql_command in schema.read().split(';'):
                db_con.execute(sql_command)

    def get_chunks(self, lst: list, n: int):
        # yield successive n-sized chunks from lst
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def parse(self, file_path):
        g = gzip.open(file_path, 'r')

        for l in g:
            yield eval(l)

    def parse_review_file(self, db_con: sqlite3.Connection):
        self.log("Parsing a file '{}'...".format(self.args.review_file))

        count = 0
        to_insert_user = []
        to_insert_review = []
        ratings_by_id = {}

        # loop through reviews in review_file
        for review in self.parse(self.args.review_file):
            count += 1

            # parse review's data
            try:
                user_id = review.get("reviewerID")
                user_name = review.get("reviewerName")
                item_id = review.get("asin")
                item_rating = review.get("overall")
                review_time = review.get("unixReviewTime", None)

            except NameError:
                print("ERROR: Following review is missing at least one of the required attributes"
                      "(reviewerID, reviewerName, asin, overall, unixReviewTime):")
                print(review)
                raise

            # collect ratings from reviews for items
            if item_id in ratings_by_id:
                ratings_by_id[item_id].append(item_rating)
            else:
                ratings_by_id[item_id] = [item_rating]

            # collect review and user data to insert
            to_insert_user.append([user_id, user_name])
            to_insert_review.append([user_id, item_id, item_rating, review_time])

        # insert parsed data into DB tables batch by batch
        for batch in self.get_chunks(to_insert_user, 1000):
            query = '''INSERT OR IGNORE INTO user(id, name) 
                       VALUES {}'''.format(",".join(
                ["(\"{}\", \"{}\")".format(user_id, self.escape(user_name))
                 for (user_id, user_name) in batch]))
            db_con.execute(query)
            db_con.commit()

        for batch in self.get_chunks(to_insert_review, 1000):
            query = '''INSERT INTO review(userId, itemId, rating, reviewTime) 
                       VALUES {}'''.format(",".join(
                ["(\"{}\", \"{}\", \"{}\", \"{}\")".format(user_id, item_id, item_rating, review_time)
                 for user_id, item_id, item_rating, review_time in batch]))
            db_con.execute(query)
            db_con.commit()

        self.log_billboard(["Parsing of reviews is DONE!", "Reviews parsed: {}".format(count)])

        return ratings_by_id

    def generate_meta_items(self):
        # loop through all items in meta_file
        for item in self.parse(self.args.meta_file):
            # parse item's metadata
            try:
                item_id = item.get("asin")

                item_title = item.get("title", None)
                item_description = item.get("description", None)
                item_price = item.get("price", "NULL")
                item_image_url = item.get("imUrl", None)

                sales = item.get("salesRank", None)
                sales_category = None
                sales_rank = None

                if sales is not None:
                    for key, value in sales.items():
                        sales_category, sales_rank = key, value

                if sales_rank is None:
                    sales_rank = "NULL"

            except NameError:
                print("ERROR: Following item is missing at least one of the required attributes"
                      "(asin):")
                print(item)
                raise

            yield [item_id, item_title, item_description, item_price, item_image_url, sales_category, sales_rank]

    def insert_meta_batch(self, db_con: sqlite3.Connection, batch: list):
        if batch:
            query = '''INSERT INTO 
                       item(id, title, description, price, imageUrl, salesCategory, salesRank, overallRating) 
                       VALUES {}'''.format(",".join(
                ["(\"{}\", \"{}\", \"{}\", {}, \"{}\", \"{}\", {}, {})".format(item_id, self.escape(item_title),
                                                                               self.escape(item_description),
                                                                               item_price, item_image_url,
                                                                               sales_category, sales_rank, rating)
                 for (item_id, item_title, item_description, item_price, item_image_url, sales_category, sales_rank,
                      rating) in batch]))
            db_con.execute(query)
            db_con.commit()

    def parse_meta_file(self, db_con: sqlite3.Connection, ratings_by_id: dict):
        self.log("Parsing a file '{}'...".format(self.args.meta_file))

        count = 0
        to_insert_items = []

        for item_data in self.generate_meta_items():
            count += 1

            item_id = item_data[0]
            review_count = len(ratings_by_id[item_id]) if item_id in ratings_by_id else 0

            if review_count > 0:
                overall_rating = round((sum(ratings_by_id[item_id]) / len(ratings_by_id[item_id])), 2)
                item_data.append(overall_rating)
                to_insert_items.append(item_data)

            if len(to_insert_items) == 1000:
                self.insert_meta_batch(db_con, to_insert_items)
                to_insert_items = []

        self.insert_meta_batch(db_con, to_insert_items)

        self.log_billboard(["Parsing of all products is DONE!", "Products parsed: {}".format(count)])

    def parse_related_and_categories(self, db_con: sqlite3.Connection):
        self.log("Parsing 'related' and 'categories' for products...")

        # get filtered out items
        cursor = db_con.execute("SELECT id FROM item")
        filtered_items = [item[0] for item in cursor.fetchall()]

        count = 0
        inserted_categories = {}  # dictionary for inserted_categories in format {namespace: id}
        to_insert_related = []
        to_insert_item_category = []

        # loop the file again and for each filtered item parse rest of metadata
        for item in self.parse(self.args.meta_file):
            item_id = item.get("asin")

            # continue if item was filtered out
            if item_id not in filtered_items:
                continue
            else:
                count += 1

            categories = item.get("categories", None)
            related = item.get("related", None)

            if related is not None:
                for key in related.keys():
                    for related_item_id in related.get(key):
                        if related_item_id in filtered_items:
                            to_insert_related.append([item_id, related_item_id, key])

            if categories is not None:
                for category_hierarchy_array in categories:
                    namespace = ''
                    previous_category_id = None

                    for category in category_hierarchy_array:
                        namespace += '.' + category

                        if namespace in inserted_categories:
                            previous_category_id = inserted_categories[namespace]
                        else:
                            cursor = db_con.execute("INSERT INTO category(parentCategoryId, namespace, name) "
                                                    "VALUES (?, ?, ?)",
                                                    [previous_category_id, namespace, category])
                            inserted_id = cursor.lastrowid
                            previous_category_id = inserted_id
                            inserted_categories[namespace] = inserted_id
                            cursor.close()

                    to_insert_item_category.append([item_id, previous_category_id])
            db_con.commit()

        for batch in self.get_chunks(to_insert_item_category, 1000):
            query = '''INSERT INTO item_category_list(itemId, categoryId) 
                       VALUES {}'''.format(",".join(
                ["(\"{}\", {})".format(item_id, category_id)
                 for item_id, category_id in batch]))
            db_con.execute(query)
            db_con.commit()

        for batch in self.get_chunks(to_insert_related, 1000):
            query = '''INSERT INTO item_related_list(itemId, relatedItemId, relation) 
                       VALUES {}'''.format(",".join(
                ["(\"{}\", \"{}\", \"{}\")".format(item_id, related_item_id, key)
                 for item_id, related_item_id, key in batch]))
            db_con.execute(query)
            db_con.commit()

        self.log_billboard(["Parsing of extra data is DONE!", "Products parsed: {}".format(count)])

    def run(self):
        global LAST_TIME_CHECKPOINT
        LAST_TIME_CHECKPOINT = datetime.now()
        start_time = datetime.now()

        # prepare DB
        self.log("Preparing DB...")
        db_con = create_connection()
        self.prepare_tables(db_con)
        self.log_billboard(["Preparation of DB is DONE!"])

        # parse input files
        ratings_by_id = self.parse_review_file(db_con)
        self.parse_meta_file(db_con, ratings_by_id)
        self.parse_related_and_categories(db_con)

        db_con.close()
        end_time = datetime.now()

        print("DONE - Script execution took: {}".format(end_time - start_time))


if __name__ == "__main__":
    db_setup = DBSetup()
    db_setup.run()
