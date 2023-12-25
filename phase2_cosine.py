from mrjob.job import MRJob, MRStep
from math import sqrt
class MRRecommendation(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_preprocess, reducer=self.reducer_preprocess),
            MRStep(mapper=self.mapper_similarity, reducer=self.reducer_similarity),
            # MRStep(mapper_init=self.mapper_init, mapper=self.mapper_recommendation_1, mapper_final=self.mapper_final,
                #    reducer_init=self.reducer_init, reducer=self.reducer_recommendation, reducer_final=self.reducer_final)
        ]

    # Phase 1: Preprocessing
    def mapper_preprocess(self, _, line):
        userId, *item_ratings = line.split('\t')
        yield userId, [(item, rating) for item, rating in zip(item_ratings[::2], item_ratings[1::2])]

    def reducer_preprocess(self, userId, item_rating_lists):
        yield userId, [item_rating for sublist in item_rating_lists for item_rating in sublist]

    # Phase 2: Calculating Similarity
    def mapper_similarity(self, userId, item_rating_list):
        for i, (item1, rating1) in enumerate(item_rating_list):
            for j, (item2, rating2) in enumerate(item_rating_list):
                if item1 != item2:
                    yield (item1, item2), (rating1, rating2)

    def reducer_similarity(self, item_pair, rating_pairs):
        dot_product = 0
        norm_item1 = 0
        norm_item2 = 0
        i = 0
        for rating1, rating2 in rating_pairs:
            rating1 = float(rating1)
            rating2 = float(rating2) 

            dot_product += float(rating1) * float(rating2) 
            norm_item1 += rating1**2
            norm_item2 += rating2**2
            i = i + 1
        cosine_similarity = i * dot_product / ((sqrt(norm_item1)) * sqrt((norm_item2))) if norm_item1 and norm_item2 else 0
        yield item_pair, cosine_similarity



if __name__ == '__main__':
    MRRecommendation.run()
    # MRRecommendation2.run()