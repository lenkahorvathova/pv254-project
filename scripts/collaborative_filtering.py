import sqlite3
import pandas as pd
import os
import time
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.neighbors import NearestNeighbors

connection = sqlite3.connect("data/amazon_product_data.db")

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
        #hashmap of itemId to row index in item-user scipy sparse matrix
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
            n_neighbors=n_recommendations+1)
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
        recommendedItemId = []
        print('Recommendations for {}:'.format(itemId))
        for i, (idx, dist) in enumerate(raw_recommends):
            print('{0}: {1}, with distance '
                  'of {2}'.format(i+1, self.hashmap[idx], dist))
            recommendedItemId.insert(0, self.hashmap[idx])
        del raw_recommends
        return recommendedItemId


def getMeanPerItemList():
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

def hybridAlgorithm(itemId, n_recommendations):
    """
    Return
    ------
    as first thing the best item id to recommend by item-based collaboration filtering
    as second thing the best item id according to hybrid algorithm(collaboration + mean)
    """

    #first parametr is id of item, second number of items to recommend
    recommendedItemId = collaboraborationFiltering(itemId, n_recommendations)
    #####
    meanPerItem = getMeanPerItemList()
    itemIdWithHighestReviewMean = recommendedItemId[0]
    for itemId in recommendedItemId:
        if meanPerItem[itemId] > meanPerItem[itemIdWithHighestReviewMean]:
            print('new value for itemIdWithHighestReviewMean', itemId, meanPerItem[itemId])
            itemIdWithHighestReviewMean = itemId
    print('best item id to recommend according to hybrid', itemIdWithHighestReviewMean)

    return recommendedItemId[0], itemIdWithHighestReviewMean

def collaboraborationFiltering(itemId, n_recommendations):
    recommender = KnnRecommender()
    # make recommendations, first parametr is id of item, second number of items to recommend
    recommendedItemId = recommender.make_recommendations(itemId, n_recommendations)

    return recommendedItemId

t0 = time.time()
#first parametr is id of item, second number of items to recommend
hybridAlgorithm('B00CBT674G', 10)
print('It took my system {:.2f}s to make recommendation \n\
                          '.format(time.time() - t0))