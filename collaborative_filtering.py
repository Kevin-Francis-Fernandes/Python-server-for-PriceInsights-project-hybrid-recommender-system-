import pandas as pd
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.metrics.pairwise import cosine_similarity

class CollaborativeFilteringRecommender:
    def __init__(self, df):
        self.df = df
        # Create user-item interaction matrix
        self.user_item_matrix = self.df.pivot_table(index='user_id', columns='item_id', values='rating').fillna(0)
        print("other",self._get_user_demographic_matrix(weightage_factor=0.6))
        print("self",self._get_user_similarity_matrix())
        self.user_similarity_matrix = self._get_user_similarity_matrix().dot(
            self._get_user_demographic_matrix(weightage_factor=0.6))

    def _get_user_similarity_matrix(self):
        # Create user similarity matrix using cosine similarity
        user_similarity_matrix = pd.DataFrame(cosine_similarity(self.user_item_matrix),
                                              index=self.user_item_matrix.index,
                                              columns=self.user_item_matrix.index)
        return user_similarity_matrix

    def _get_user_demographic_matrix(self, weightage_factor):
        age_encoder = LabelEncoder()

        user_profiles = self.df[['user_id', 'age', 'gender', 'location']].drop_duplicates()
        user_profiles = user_profiles.set_index('user_id')
        user_profiles['age_encoded'] = age_encoder.fit_transform(user_profiles['age'])

        onehot_cols = ['gender', 'location']
        user_profiles_encoded = pd.get_dummies(user_profiles, columns=onehot_cols)
        user_profiles_encoded = user_profiles_encoded.drop('age', axis=1)

        user_demographic_matrix = pd.DataFrame(cosine_similarity(user_profiles_encoded),
                                               index=user_profiles_encoded.index,
                                               columns=user_profiles_encoded.index)
        return user_demographic_matrix * weightage_factor

    def get_recommendations(self, user_id, top_n=10):
        # Get the similarity scores between the target user and all other users
        user_similarity = self.user_similarity_matrix[user_id]

        # Get the indices of the top N similar users (excluding the target user itself)
        similar_user_indices = user_similarity.argsort()[::-1][1:top_n + 1]

        # Get the products that the similar users have interacted with
        recommended_products = set()
        for index in similar_user_indices:
            similar_user_id = self.df.iloc[index]['user_id']
            products_interacted = self.df[self.df['user_id'] == similar_user_id]['item_id'].tolist()
            recommended_products.update(products_interacted)

        # Remove the products that the target user has already interacted with
        target_user_products = self.df[self.df['user_id'] == user_id]['item_id']
        recommended_products = recommended_products - set(target_user_products)

        # Return the top N recommended products
        top_n_items = self.df[self.df['item_id'].isin(recommended_products)]['item_id'].head(top_n)
        return top_n_items
