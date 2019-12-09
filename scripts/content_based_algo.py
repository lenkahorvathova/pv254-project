import time
from collections import Counter
from sqlite3 import Connection

from scripts.utils import create_connection


class Item:
    connection: Connection
    id: str
    title: str
    image_url: str
    overall_rating: float
    related: list

    def __init__(self):
        self.related = []

    def set_properties(self, id: str, title: str, image_url: str, overall_rating: float):
        self.id = id
        self.title = title
        self.image_url = image_url
        self.overall_rating = overall_rating


class SimilarityRecommender:
    connection: Connection
    item_id: str
    # {id -> Item}
    similar_items: dict
    # {id -> id -> float}
    similarity_matrix: dict

    def __init__(self, connection: Connection, item_id: str):
        self.connection = connection
        self.item_id = item_id

    # find_similar_items_callback - Take one parameter item_id: str. Return dict in format {id -> Item}.
    # count_similarities_callback - Take one parameter similar_items: dict - result of the function above. Return
    # Two-dimensional dictionary, keys on both first and second level are the ids of items from similar_items.
    # Value on index [x][y] and [y][x] is the same, it is float and corresponds to similarity of items x and y. Value
    # [x][x] is not defined.
    #
    # Based on given callbacks find similar items and count similarities between them, and then find the given number
    # of them so that they are as different from each other as possible(*) (* Not really as it is an NP-complete
    # problem, but approximately.)
    # Return ordered list of recommended <Item>s.
    def recommend_products(self, count: int, find_similar_items_callback: callable,
                           count_similarities_callback: callable) -> list:
        self.similar_items = find_similar_items_callback(self.connection, self.item_id)
        self.similarity_matrix = count_similarities_callback(self.connection, self.similar_items)

        return self.get_diverse_recommenations(count)

    # Do not call directly
    def get_diverse_recommenations(self, count) -> list:
        if count > len(self.similarity_matrix):
            count = len(self.similarity_matrix)
        # Find the best product as initial
        best_item_id = None
        best = -1
        for item_id, item in self.similar_items.items():
            if item.overall_rating > best:
                best_item_id = item.id
                best = item.overall_rating

        selected_ids = {best_item_id}
        self.similarity_matrix.pop(best_item_id)

        for i in range(0, count - 1):
            less_similar_item_id = None
            less_similarity = float('inf')
            for v in self.similarity_matrix:
                # # V1
                # similarity = 0
                # for sel in selected_ids:
                #     similarity += self.similarity_matrix[v][sel]
                # if similarity < less_similarity:
                #     less_similar_item_id = v
                #     less_similarity = similarity
                # # V2
                # max_similarity = 0
                # for sel in selected_ids:
                #     sim = self.similarity_matrix[v][sel]
                #     if sim > max_similarity:
                #         max_similarity = sim
                # if max_similarity < less_similarity:
                #     less_similar_item_id = v
                #     less_similarity = max_similarity
                # V3
                squared_similarity = 0
                for sel in selected_ids:
                    squared_similarity += (self.similarity_matrix[v][sel]) ** 2
                if squared_similarity < less_similarity:
                    less_similar_item_id = v
                    less_similarity = squared_similarity

            selected_ids.add(less_similar_item_id)
            # print(less_similar_item_id)
            self.similarity_matrix.pop(less_similar_item_id)

        recommended_items = [self.similar_items[item_id] for item_id in selected_ids]

        def sort_func(item: Item) -> float:
            return item.overall_rating

        recommended_items.sort(key=sort_func, reverse=True)

        return recommended_items


def find_similar_items_related(connection: Connection, main_item_id: str) -> dict:
    cursor = connection.cursor()

    similar_items = {main_item_id}  # the result
    items_relations_sets = {}  # optimization - remember the found relations for the items
    last_added_items = {main_item_id}  # new discovered items in last iteration
    thresholds = [float('inf'), 101, 21, 11]  # how many items must be found to not start new iteration
    i = 0
    while i < len(thresholds) and len(similar_items) < thresholds[i]:
        new_items = set()
        for item_id in last_added_items:
            cur = cursor.execute("SELECT relatedItemId FROM item_related_list WHERE itemId = ?;", [item_id])
            ids = [new_item[0] for new_item in cur]
            items_relations_sets[item_id] = ids
            new_items.update(ids)

        last_added_items = new_items.difference(similar_items)
        if len(last_added_items) == 0:  # optimization
            break
        similar_items.update(last_added_items)
        i += 1

    similar_items.remove(main_item_id)

    result_items = {}
    for item_id in similar_items:
        cur = cursor.execute("SELECT title, imageUrl, overallRating FROM item WHERE id = ?;", [item_id])
        row = cur.fetchone()
        item = Item()
        item.set_properties(item_id, *row)
        if item_id in items_relations_sets:
            item.related = items_relations_sets[item_id]
        result_items[item_id] = item

    cursor.close()
    return result_items


def find_similar_items_category(connection: Connection, item_id: str) -> dict:
    cursor = connection.execute("SELECT categoryId FROM item_category_list WHERE itemId=(?)", [item_id])
    category = cursor.fetchone()[0]
    categories = {category}
    actual_categories = {category}
    while actual_categories:
        new_categories = set()
        for category in actual_categories:
            cursor = connection.execute("SELECT id FROM category WHERE parentCategoryId=(?)", [category])
            new_categories.update([cat[0] for cat in cursor.fetchall()])
        categories.update(new_categories)
        actual_categories = new_categories

    cursor = connection.execute("SELECT itemId FROM item_category_list WHERE categoryId=(?)", [category])
    ids = [item[0] for item in cursor.fetchall()]
    result_items = {}
    for id in ids:
        if id != item_id:
            cursor = connection.execute("SELECT title, imageUrl, overallRating FROM item WHERE id=(?)", [id])
            row = cursor.fetchone()
            item = Item()
            item.set_properties(id, *row)
            result_items[id] = item

    return result_items


def find_similar_items_test(connection: Connection, item_id: str) -> dict:
    item1 = Item()
    item2 = Item()
    item3 = Item()
    item4 = Item()
    item5 = Item()
    item6 = Item()
    item7 = Item()
    item8 = Item()
    item9 = Item()
    item10 = Item()
    item1.set_properties('1', 'Item 1', '', 4)
    item2.set_properties('2', 'Item 2', '', 4.3)
    item3.set_properties('3', 'Item 3', '', 3)
    item4.set_properties('4', 'Item 4', '', 4.9)
    item5.set_properties('5', 'Item 5', '', 3.1)
    item6.set_properties('6', 'Item 6', '', 5)
    item7.set_properties('7', 'Item 7', '', 4.5)
    item8.set_properties('8', 'Item 8', '', 4)
    item9.set_properties('9', 'Item 9', '', 2.6)
    item10.set_properties('10', 'Item 10', '', 3.8)
    items = {'1': item1, '2': item2, '3': item3, '4': item4, '5': item5, '6': item6, '7': item7, '8': item8, '9': item9,
             '10': item10}
    return items


def count_similarities_related(connection: Connection, similar_items: dict) -> dict:
    cursor = connection.cursor()

    # Fill aux_matrix with values corresponding to number of relations between the items (the matrix is symmetric).
    # They are included all related items to the similar items.
    aux_matrix = initialize_matrix(similar_items.keys(), similar_items.keys())
    outer_items = set()
    for item in similar_items.values():
        if item.related:
            counts = Counter(item.related)
            for item_id in counts:
                if item_id in aux_matrix:
                    aux_matrix[item_id][item.id] += counts[item_id]
                    aux_matrix[item.id][item_id] += counts[item_id]
        # else:
    #     cur = cursor.execute("SELECT relatedItemId FROM item_related_list WHERE itemId = ?;", [item.id])
    #     item.related = [row[0] for row in cur]
    #     outer_items.update(item.related)
    #     counts = Counter(item.related)
    #     for item_id in counts:
    #         if item_id not in aux_matrix:
    #             aux_matrix[item_id] = {}
    #         if item.id not in aux_matrix[item_id]:
    #             aux_matrix[item_id][item.id] = 0
    #         if item_id not in aux_matrix[item.id]:
    #             aux_matrix[item.id][item_id] = 0
    #         aux_matrix[item_id][item.id] += counts[item_id]
    #         aux_matrix[item.id][item_id] += counts[item_id]
    #
    # outer_items.difference_update(similar_items.keys())
    # for outer_item_id in outer_items:
    #     cur = cursor.execute("SELECT relatedItemId FROM item_related_list WHERE itemId = ? AND relatedItemId IN ({});"
    #                          .format(",".join(['"{}"'.format(item) for item in similar_items.keys()])),
    #                          [outer_item_id])
    #     related = [row[0] for row in cur]
    #     counts = Counter(related)
    #     for item_id in counts:
    #         if item_id not in aux_matrix[outer_item_id]:
    #             aux_matrix[outer_item_id][item_id] = 0
    #         if outer_item_id not in aux_matrix[item_id]:
    #             aux_matrix[item_id][outer_item_id] = 0
    #         aux_matrix[item_id][outer_item_id] += counts[item_id]
    #         aux_matrix[outer_item_id][item_id] += counts[item_id]

    # # Count matrix as the result of aux_matrix to the power of 2, values are similarities between items that are
    # # in relation in distance 2. Value is divided by 10 for comparing to the original values.
    # matrix = initialize_matrix(similar_items.keys(), similar_items.keys())
    # for i in similar_items.keys():
    #     for j1 in aux_matrix[i]:
    #         for j2 in aux_matrix:
    #             if j2 in aux_matrix[j1]:
    #                 print(i + ' ' + j1 + ' ' + j2)
    #                 matrix[i][j2] += aux_matrix[i][j1] * aux_matrix[j1][j2] / 10
    #
    # # Sum the values - similarities of items in distance 1 and 2.
    # for i in matrix:
    #     for j in matrix:
    #         matrix[i][j] += aux_matrix[i][j]

    cursor.close()
    # return matrix
    return aux_matrix


def initialize_matrix(rows: iter, cols: iter) -> dict:
    matrix = {}
    for i in rows:
        matrix[i] = {}
        for j in cols:
            matrix[i][j] = 0
    return matrix


def count_similarities_test(connection: Connection, similar_items: dict) -> dict:
    similarity_matrix = {
        '1': {
            '2': 3, '3': 4, '4': 0, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0
        },
        '2': {
            '1': 3, '3': 6, '4': 1, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0
        },
        '3': {
            '1': 4, '2': 6, '4': 1, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0
        },
        '4': {
            '1': 0, '2': 1, '3': 1, '5': 10, '6': 10, '7': 2, '8': 3, '9': 2, '10': 3
        },
        '5': {
            '1': 0, '2': 0, '3': 0, '4': 10, '6': 10, '7': 7, '8': 3, '9': 1, '10': 3
        },
        '6': {
            '1': 0, '2': 0, '3': 0, '4': 10, '5': 10, '7': 7, '8': 6, '9': 3, '10': 4
        },
        '7': {
            '1': 0, '2': 0, '3': 0, '4': 2, '5': 7, '6': 7, '8': 1, '9': 0, '10': 1
        },
        '8': {
            '1': 0, '2': 0, '3': 0, '4': 3, '5': 3, '6': 6, '7': 1, '9': 9, '10': 9
        },
        '9': {
            '1': 0, '2': 0, '3': 0, '4': 2, '5': 1, '6': 3, '7': 0, '8': 9, '10': 7
        },
        '10': {
            '1': 0, '2': 0, '3': 0, '4': 3, '5': 3, '6': 4, '7': 1, '8': 9, '9': 7
        },
    }
    return similarity_matrix


def algorithm_related(product_id: str, recommendations_count: int) -> list:
    connection = create_connection()
    similarity_recommender = SimilarityRecommender(connection, product_id)
    recommended = similarity_recommender.recommend_products(recommendations_count, find_similar_items_related, count_similarities_related)
    recommended_ids = []
    for product in recommended:
        recommended_ids.append(product.id)

    connection.close()
    return recommended_ids


def algorithm_related_with_category(product_id: str, recommendations_count: int) -> list:
    connection = create_connection()
    similarity_recommender = SimilarityRecommender(connection, product_id)
    recommended = similarity_recommender.recommend_products(recommendations_count, find_similar_items_category, count_similarities_related)
    recommended_ids = []
    for product in recommended:
        recommended_ids.append(product.id)

    connection.close()
    return recommended_ids


if __name__ == "__main__":
    t0 = time.time()
    product_id = 'B00000JHX6'
    recommended = algorithm_related(product_id, 10)
    print("----result algorithm_related----")
    for product in recommended:
        print(product)
    t1 = time.time()
    print('algorithm_related took {:.2f}s'.format(t1 - t0))

    product_id = 'B00000JHX6'
    recommended = algorithm_related_with_category(product_id, 10)
    print("----result algorithm_related_with_category----")
    for product in recommended:
        print(product)
    print('algorithm_related_with_category took {:.2f}s'.format(time.time() - t1))
