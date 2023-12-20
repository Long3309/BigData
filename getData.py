from collections import defaultdict
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

user_ratings_count = {}


def mapper(line):
    userid, movieid, rating, timestamp = map(int, line.strip().split('\t'))

    # Tính toán số lần đánh giá của userid
    user_ratings_count[userid] = user_ratings_count.get(userid, 0) + 1

    # Sử dụng số lần đánh giá của userid để quyết định liệu dữ liệu này thuộc tập train hay test
    is_train = user_ratings_count[userid] % 5 < 4  # 80% là tập train

    if is_train:
        yield (userid, ('train', movieid, rating, timestamp))
    else:
        yield (userid, ('test', movieid, rating, timestamp))


def reducer(key, values):
    train_data = []
    test_data = []

    for value_type, movieid, rating, timestamp in values:
        if value_type == 'train':
            train_data.append((movieid, rating, timestamp))
        elif value_type == 'test':
            test_data.append((movieid, rating, timestamp))

    yield (key, {'trainData': train_data, 'testData': test_data})


def process_folder_and_save(folder_path, output_file_path):
    grouped_data = defaultdict(list)

    # Đọc dữ liệu từ tất cả các file trong thư mục và ghi vào một file duy nhất
    with open(output_file_path, 'w') as output_file:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as input_file:
                lines = input_file.readlines()
                output_file.writelines(lines)  # Ghi dữ liệu vào file duy nhất

                mapped_data = [mapper(line) for line in lines]
                flattened_mapped_data = [item for sublist in mapped_data for item in sublist]

                for k, v in flattened_mapped_data:
                    grouped_data[k].append(v)

    # Chạy reducer
    result = [item for sublist in [reducer(k, grouped_data[k]) for k in grouped_data] for item in sublist]

    # Xử lý kết quả
    train = []
    test = []
    for userid, data in result:
        for movieid, rating, timestamp in data["trainData"]:
            train.append((userid, movieid, rating, timestamp))
        for movieid, rating, timestamp in data["testData"]:
            test.append((userid, movieid, rating, timestamp))

    return train, test

class FolderWatcher(FileSystemEventHandler):
    def __init__(self, callback):
        super().__init__()
        self.callback = callback
    def on_modified(self, event):
        if event.is_directory and event.src_path == "./ratingdata":
            print(f"======== Updated ========")
            self.callback()


def main():
    input_folder = "./ratingdata"
    output_file_path = "temp.txt"
    train_text_file_path = "./train/train.txt"
    test_text_file_path = "test.txt"

    def process_and_save():
        train_result, test_result = process_folder_and_save(input_folder, output_file_path)

        with open(train_text_file_path, mode='w') as train_file:
            for item in train_result:
                train_file.write("{}\n".format("\t".join(map(str, item))))
        print(f"File train đã được tạo tại: {train_text_file_path}")
        print(f"Độ lớn tập train: {len(train_result)}")

        with open(test_text_file_path, mode='w') as test_file:
            for item in test_result:
                test_file.write("{}\n".format("\t".join(map(str, item))))
        print(f"File test đã được tạo tại: {test_text_file_path}")
        print(f"Độ lớn tập test: {len(test_result)}")


    # Initial processing
    process_and_save()

    # Watch for changes in the input folder
    event_handler = FolderWatcher(callback=process_and_save)
    observer = Observer()
    observer.schedule(event_handler, path=input_folder, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
    os.remove("temp.txt")