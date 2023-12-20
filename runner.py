from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import subprocess
import os
import pandas as pd

class MyHandler(FileSystemEventHandler):
    def __init__(self, callback):
        super().__init__()
        self.callback = callback

    def on_modified(self, event):
        if event.is_directory and event.src_path == "./train": # Replace with the actual path to your second file
            self.callback()

def main():
    path = "./train"  # Đường dẫn đến folder train
    def script():
        print("============= UPDATED PHASE 1 =============")
        subprocess.run("python3 phase1.py -r local ./train/train.txt > phase1.txt", shell=True)

        print("============= UPDATED PHASE 2 =============")
        subprocess.run("python3 phase2_jaccard.py -r local ./train/train.txt > phase2_jaccard.txt", shell=True)
        subprocess.run("cat phase1.txt phase2_jaccard.txt > ./jaccard/processed_data.txt", shell=True)

        subprocess.run("python3 phase2_hamming.py -r local ./train/train.txt > phase2_hamming.txt", shell=True)
        subprocess.run("cat phase1.txt phase2_hamming.txt > ./hamming/processed_data.txt", shell=True)

        subprocess.run("python3 phase2_cosine.py -r local ./train/train.txt > phase2_cosine.txt", shell=True)
        subprocess.run("cat phase1.txt phase2_cosine.txt > ./cosine/processed_data.txt", shell=True)

        print("============= UPDATED PHASE 3 =============")
        subprocess.run("python3 phase3.py -r local ./jaccard/processed_data.txt > ./jaccard/phase3.txt", shell=True)
        subprocess.run("python3 phase3.py -r local ./hamming/processed_data.txt > ./hamming/phase3.txt", shell=True)
        subprocess.run("python3 phase3.py -r local ./cosine/processed_data.txt > ./cosine/phase3.txt", shell=True)
        os.remove("phase2_jaccard.txt")
        os.remove("phase2_hamming.txt")
        os.remove("phase2_cosine.txt")
        print("************************ TASK DONE ************************")
    event_handler = MyHandler(callback=script)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
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