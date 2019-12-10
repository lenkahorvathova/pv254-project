import os
import time

import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.neighbors import NearestNeighbors

from scripts.utils import create_connection


class KnnRecommender:
    """
    This is an item-based collaborative filtering recommender with
    KNN implmented by sklearn
    """

    def __init__(self):
        self.model = NearestNeighbors()
        self.item_user_mat_sparse, self.hashmap = self._prep_data()
        self.set_model_params(10, 'brute', 'cosine', -1)

    def set_model_params(self, n_neighbors, algorithm, metric, n_jobs=None):
        """
        set model params for sklearn.neighbors.NearestNeighbors
        Parameters
        ----------
        n_neighbors: int, optional (default = 5)
        algorithm: {'auto', 'ball_tree', 'kd_tree', 'brute'}, optional
        metric: string or callable, default 'minkowski', or one of
            ['cityblock', 'cosine', 'euclidean', 'l1', 'l2', 'manhattan']
        n_jobs: int or None, optional (default=None)
        """
        if n_jobs and (n_jobs > 1 or n_jobs == -1):
            os.environ['JOBLIB_TEMP_FOLDER'] = '/tmp'
        self.model.set_params(**{
            'n_neighbors': n_neighbors,
            'algorithm': algorithm,
            'metric': metric,
            'n_jobs': n_jobs})

    def _prep_data(self):
        """
        prepare data for recommender
        1. item-user scipy sparse matrix
        2. hashmap of itemId to row index in item-user scipy sparse matrix
        """
        connection = create_connection()

        with connection:
            # read data
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM review")
            reviews = cursor.fetchall()
            cursor.close()

            columns = ['id', 'userId', 'itemId', 'rating', 'reviewTime']
            df_ratings = pd.DataFrame(reviews, columns=columns)
            # pivot and create movie-user matrix
            item_user_mat = df_ratings.pivot(
                index='itemId', columns='userId', values='rating').fillna(0)
            # hashmap of itemId to row index in item-user scipy sparse matrix
            hashmap = {}
            index = 0
            for i in item_user_mat.index:
                hashmap[index] = i
                index = index + 1

            # transform matrix to scipy sparse matrix
            item_user_mat_sparse = csr_matrix(item_user_mat.values)

            # clean up
            del df_ratings, item_user_mat
            return item_user_mat_sparse, hashmap

    def _inference(self, model, data,
                   itemId, n_recommendations):
        """
        return top n similar item recommendations
        Parameters
        ----------
        model: sklearn model, knn model
        data: item-user matrix
        itemId: id of item in matrix
        n_recommendations: int, top n recommendations
        Return
        ------
        list of top n similar item recommendations
        """
        # fit
        model.fit(data)

        # inference
        distances, indices = model.kneighbors(
            data[itemId],
            n_neighbors=n_recommendations + 1)
        # get list of raw idx of recommendations
        raw_recommends = \
            sorted(
                list(
                    zip(
                        indices.squeeze().tolist(),
                        distances.squeeze().tolist()
                    )
                ),
                key=lambda x: x[1]
            )[:0:-1]

        # return recommendation (itemId, distance)
        return raw_recommends

    def make_recommendations(self, itemId, n_recommendations):
        """
        make top n movie recommendations
        Parameters
        ----------
        itemId: raw id of item
        n_recommendations: int, top n recommendations
        """
        reverse_hashmap = {v: k for k, v in self.hashmap.items()}
        idx = reverse_hashmap[itemId]
        # get recommendation
        raw_recommends = self._inference(
            self.model, self.item_user_mat_sparse,
            idx, n_recommendations)
        # print results
        recommended_item_id = []
        # print('Recommendations for {}:'.format(itemId))
        for i, (idx, dist) in enumerate(raw_recommends):
            # print('{0}: {1}, with distance '
            #       'of {2}'.format(i + 1, self.hashmap[idx], dist))
            recommended_item_id.insert(0, self.hashmap[idx])
        del raw_recommends
        return recommended_item_id


def get_mean_per_item_list():
    connection = create_connection()

    with connection:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM review")
        reviews = cursor.fetchall()
        cursor.close()

        columns = ['id', 'userId', 'itemId', 'rating', 'reviewTime']
        df_ratings = pd.DataFrame(reviews, columns=columns)
        # pivot and create item-user matrix
        item_user_mat = df_ratings.pivot(
            index='itemId', columns='userId', values='rating')
        return item_user_mat.mean(axis=1, skipna=True)


def hybrid_algorithm(itemId, n_recommendations):
    """
    Return
    ------
    as first thing the best item id to recommend by item-based collaboration filtering
    as second thing the best item id according to hybrid algorithm(collaboration + mean)
    """

    # first parametr is id of item, second number of items to recommend
    recommended_item_id = collaboration_filtering(itemId, n_recommendations)
    #####
    mean_per_item = get_mean_per_item_list()
    item_id_with_highest_review_mean = recommended_item_id[0]
    for itemId in recommended_item_id:
        if mean_per_item[itemId] > mean_per_item[item_id_with_highest_review_mean]:
            # print('new value for item_id_with_highest_review_mean', itemId, mean_per_item[itemId])
            item_id_with_highest_review_mean = itemId
    # print('best item id to recommend according to hybrid', item_id_with_highest_review_mean)

    return recommended_item_id[0], item_id_with_highest_review_mean


def collaboration_filtering(itemId, n_recommendations):
    recommender = KnnRecommender()
    # make recommendations, first parametr is id of item, second number of items to recommend
    recommended_item_id = recommender.make_recommendations(itemId, n_recommendations)

    return recommended_item_id


if __name__ == "__main__":
    t0 = time.time()
    # first parametr is id of item, second number of items to recommend
    hybrid_algorithm('B00CBT674G', 10)
    print('It took my system {:.2f}s to make recommendation \n\
                              '.format(time.time() - t0))
