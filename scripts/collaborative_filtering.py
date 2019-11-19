import sqlite3
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import pairwise_distances



def find_n_neighbours(df,n):
	order = np.argsort(df.values, axis=1)[:, :n]
	df = df.apply(lambda x: pd.Series(x.sort_values(ascending=False).iloc[:n].index, index=['top{}'.format(i) for i in range(1, n+1)]), axis=1)
	return df


def collaborative_filtering():
	connection = sqlite3.connect("data/amazon_product_data.db")
	cursor = connection.cursor()
	cursor.execute("SELECT * FROM review")
	reviews = cursor.fetchall()
	print(reviews)
	#first part
	Reviews = pandas.read_fwf(reviews)
	Mean = Ratings.groupby(by="userId", as_index=False)["rating"].mean()
	Rating_avg = pd.merge(Ratings,Mean,on='userId')
	Rating_avg['adg_rating']=Rating_avg['rating_x']-Rating_avg['rating_y']
	Rating_avg.head()
	#second part
	check = pd.pivot_table(Rating_avg,values='rating_x',index='userId',columns='itemId')
	check.head()
	#third part
	final = pd.pivot_table(Rating_avg,values='adg_rating',index='userId',columns='itemId')
	final.head()
	# Replacing NaN by Movie Average
	final_item = final.fillna(final.mean(axis=0))
	final_item.head()
	# Replacing NaN by user Average
	final_user = final.apply(lambda row: row.fillna(row.mean()), axis=1)
	#calculate the similarity between the users.
	# user similarity on replacing NAN by user avg
	b = cosine_similarity(final_user)
	np.fill_diagonal(b, 0 )
	similarity_with_user = pd.DataFrame(b,index=final_user.index)
	similarity_with_user.columns=final_user.index
	similarity_with_user.head()
	# user similarity on replacing NAN by item(movie) avg
	cosine = cosine_similarity(final_item)
	np.fill_diagonal(cosine, 0 )
	similarity_with_item = pd.DataFrame(cosine,index=final_item.index)
	similarity_with_item.columns=final_user.index
	similarity_with_item.head()
	# top 30 neighbours for each user
	sim_user_30_u = find_n_neighbours(similarity_with_user,30)
	sim_user_30_u.head()
	sim_user_30_m = find_n_neighbours(similarity_with_item,30)
	sim_user_30_m.head()


collaborative_filtering()
