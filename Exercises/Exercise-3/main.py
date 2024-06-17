import boto3
import io
import os 
import gzip
from tqdm import tqdm


def create_new_directory(path) -> bool:

    # Create a new directory if it doesn't exist
    if not os.path.exists(path):
        os.makedirs(path)
        print(f'\'{path}\' created successfully.')
        return True
    
    print(f'\'{path}\' already existed.')
    return False


def get_obj_stream_from_s3(bucket, key) -> io.BytesIO:

    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)

    return io.BytesIO(obj.get()['Body'].read())


def extract_key_from_gzip_file(obj_stream) -> str:

    with gzip.GzipFile(fileobj=obj_stream) as f:
        file_path = f.readline()

    return file_path.decode('ascii')[:-1]


def write_data_from_stream_to_file(obj_stream, out_file, destination):

    with gzip.GzipFile(fileobj=obj_stream) as f:
        with open(f'{destination}/{out_file}', 'wb+') as f_out:
            for line in tqdm(f.readlines()):
                f_out.write(line)


def main():

    s3_bucket = 'commoncrawl'
    bucket_key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    destination_dir = './data'
    output_file = 'output.txt'

    obj_stream = get_obj_stream_from_s3(bucket=s3_bucket, key=bucket_key)
    bucket_key = extract_key_from_gzip_file(obj_stream=obj_stream)

    create_new_directory(destination_dir)
    obj_stream = get_obj_stream_from_s3(bucket=s3_bucket, key=bucket_key)
    
    write_data_from_stream_to_file(
        obj_stream=obj_stream,
        out_file=output_file,
        destination=destination_dir
    )

    pass


if __name__ == "__main__":
    main()
