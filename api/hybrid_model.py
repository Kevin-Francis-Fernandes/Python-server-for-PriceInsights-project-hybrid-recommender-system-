from content_based import *
from collaborative_filtering import *

class HybridRecommender:
    def __init__(self, df, content_based_weight=0.5, collaborative_filtering_weight=0.5):
        self.content_based_recommender = ContentBasedRecommender(df)
        self.collaborative_filtering_recommender = CollaborativeFilteringRecommender(df)
        self.content_based_weight = content_based_weight
        self.collaborative_filtering_weight = collaborative_filtering_weight

    def get_recommendations(self, user_id, top_n=7):
        # Get recommendations from both content-based and collaborative filtering recommenders
        content_based_recommendations = self.content_based_recommender.get_recommendations(user_id, top_n)
        collaborative_filtering_recommendations = self.collaborative_filtering_recommender.get_recommendations(user_id,
                                                                                                               top_n)

        # Combine the recommendations using weighted average
        hybrid_recommendations = {}
        for item in content_based_recommendations:
            hybrid_recommendations[item] = hybrid_recommendations.get(item, 0) + self.content_based_weight
        for item in collaborative_filtering_recommendations:
            hybrid_recommendations[item] = hybrid_recommendations.get(item, 0) + self.collaborative_filtering_weight

        # Sort the hybrid recommendations based on the weighted score and select the top N items
        hybrid_recommendations = sorted(hybrid_recommendations.items(), key=lambda x: x[1], reverse=True)
        hybrid_recommendations = [item for item, score in hybrid_recommendations[:top_n]]

        return hybrid_recommendations
