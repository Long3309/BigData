from mrjob.job import MRJob, MRStep

class MRPhase1(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_preprocess, reducer=self.reducer_preprocess),
        ]
    # Phase 1: Preprocessing
    def mapper_preprocess(self, _, line):
        userId, *item_ratings = line.split('\t')
        yield userId, [(item, rating) for item, rating in zip(item_ratings[::2], item_ratings[1::2])]

    def reducer_preprocess(self, userId, item_rating_lists):
        yield userId, [item_rating for sublist in item_rating_lists for item_rating in sublist]

if __name__ == '__main__':
    MRPhase1.run()