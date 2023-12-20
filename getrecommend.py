import pandas as pd
trainData = pd.read_csv("./train/train.txt", sep="\t", 
            names = ["userid","movieid","rating","timestamp"])

folder_paths = ["cosine", "hamming", "jaccard"]
for folder_path in folder_paths:
    predict = open(f"./{folder_path}/phase3.txt","r")
    lines = predict.readlines()
    predict = {}
    for line in lines:
        user_id, movie_data = line.strip().split("\t")
        user_id = int(user_id.split('"')[1])
        movie_data = eval(movie_data)  # Chuyển đổi chuỗi thành list Python
        if user_id in predict:
                for item in movie_data:
                    predict[user_id].append(item)
        else:
                predict[user_id] = movie_data
    result_dict = {}
    for user, values in predict.items():
        # Lọc ra các values có giá trị bằng giá trị lớn nhất
        max_values = sorted(values, key=lambda x: float(x[1]), reverse=True)
        result_dict[user] = max_values
    recommend = {user_id: list(set([int(value[0]) for value in values])) for user_id, values in result_dict.items()}
    for user, values in recommend.items():
        movies_filter = set(recommend[user]) - set(trainData[trainData["userid"] == user]["movieid"].tolist())
        recommend[user] = list(movies_filter)
    with open(f"./{folder_path}/predict.txt", 'w') as file:
        for key, value in recommend.items():
            line = f"{key}\t{value}"
            file.write(line + '\n')