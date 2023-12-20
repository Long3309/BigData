from mrjob.job import MRJob, MRStep
import ast
class MRRecommendation(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_preprocess,reducer=self.reducer_preprocess)
        ]
    # Phase 3: Recommendation
    def mapper_preprocess(self, _, line):
        try: 
            userid, values = line.strip().split("\t")
            userid = userid.split('"')[1]
            values = ast.literal_eval(values)
            for value in values:
                yield value[0], [("user_rating", userid, value[1])]
        except:
            pair_items, similarity = line.strip().split("\t")
            pair_items = ast.literal_eval(pair_items)
            yield pair_items[0], [("item_simmilarity",pair_items[1], similarity)]


    def reducer_preprocess(self, itemId, item_rating_lists):
        self.user_rating = {}
        self.item_simmilarity = {}
        for sublist in item_rating_lists:
            for pair in sublist:
                if pair[0] == "user_rating":
                    userID = pair[1]
                    rating = pair[2]
                    self.user_rating[userID] = rating
                if pair[0] == "item_simmilarity":
                    itemID = pair[1]
                    similarity = pair[2]
                    # if float(similarity) < 1:
                    self.item_simmilarity[itemID] = similarity
        for user, rating in self.user_rating.items():
            # Tính toán phép nhân với toàn bộ giá trị trong item_simmilarity
            product =  {key: float(rating) * float(self.item_simmilarity[key]) for key in self.item_simmilarity}
            # Lấy ra 5 giá trị lớn nhất
            top_5 = sorted(product.items(), key=lambda x: x[1], reverse=True)[:5]
            yield user, top_5

if __name__ == '__main__':
    MRRecommendation.run()
