class EvaluationMetrics:
    def __init__(self, user_data):
        self.user_data = user_data
        self.total_precision = 0
        self.total_users = len(user_data)

    def calculate_precision(self):
        precision_sum = 0.0

        for user in self.user_data:
            proposed_movies = self.user_data[user][0]
            actual_interests = self.user_data[user][1]

            intersection = len(set(proposed_movies) & set(actual_interests))
            precision = intersection / len(set(proposed_movies)) if len(set(proposed_movies)) > 0 else 0.0

            precision_sum += precision

        average_precision = precision_sum / len(self.user_data)
        return average_precision

    def calculate_recall(self):
        recall_sum = 0.0

        for user in self.user_data:
            proposed_movies = self.user_data[user][0]
            actual_interests = self.user_data[user][1]

            intersection = len(set(proposed_movies) & set(actual_interests))
            recall = intersection / len(set(actual_interests)) if len(set(actual_interests)) > 0 else 0.0

            recall_sum += recall

        average_recall = recall_sum / len(self.user_data)
        return average_recall

    def calculate_f1_score(self):
        precision = self.calculate_precision()
        recall = self.calculate_recall()

        f1_score = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
        return f1_score
    
    def calculate_precision_at_k(self, user_id, k):
        user_entry = self.user_data[user_id]
        recommended_movies = user_entry[0][:k]
        correct_movies = user_entry[1]

        precision_at_k = len(set(recommended_movies) & set(correct_movies)) / k if k > 0 else 0
        return precision_at_k

    def calculate_average_precision(self, user_id):
        user_entry = self.user_data[user_id]
        correct_movies = user_entry[1]
        precision_values = []

        for i, recommended_movie in enumerate(user_entry[1]):
            if recommended_movie in correct_movies:
                precision_at_i = len(set(user_entry[1][:i+1]) & set(correct_movies)) / (i+1)
                precision_values.append(precision_at_i)

        average_precision = sum(precision_values) / len(correct_movies) if correct_movies else 0
        return average_precision

    def calculate_map(self, k):
        for user_id in self.user_data:
            self.total_precision += self.calculate_precision_at_k(user_id, k)

        map_score = self.total_precision / self.total_users
        return map_score
