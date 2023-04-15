import sqlite3
import yaml
from pathlib import Path
from sqlite3 import Error
import argparse


def get_image(batch_id, dir_files):
    """
    gets small image data blob from directory
    """
    image = [
        file
        for file in list(dir_files)
        if batch_id in file.name and ".png" in file.name and "esrgan4x" not in file.name
    ]
    if len(image) > 1 or len(image) == 0:
        raise Exception("invalid batch id duplicate images")

    with open(image[0], "rb") as stream:
        file_blob = stream.read()

    return file_blob


def parse_result_files(web_result_path):
    web_ui_results = []
    result_dir_files = list(web_result_path.iterdir())

    for file in result_dir_files:
        if ".yaml" in file.name:
            web_ui_results.extend(process_file(result_dir_files, file))
        elif file.is_dir():
            sub_dir_files = list(file.iterdir())
            for sub_file in sub_dir_files:
                if ".yaml" in sub_file.name:
                    web_ui_results.extend(process_file(sub_dir_files, sub_file))

    return web_ui_results


def process_file(dir_files, file):
    web_ui_results = []
    batch_id = file.name.split("-")[0]
    with open(file, "r") as stream:
        try:
            parsed_file = yaml.safe_load(stream)
            image = get_image(batch_id, dir_files)
            web_ui_results.append(
                {**parsed_file, "filename": file.name, "image": image}
            )
        except yaml.YAMLError as exc:
            raise exc
    return web_ui_results


def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


def add_results_db(results):
    conn = create_connection("database.db")
    for result in results:
        try:
            cur = conn.cursor()
            filename = result["filename"].split(".")[0]
            row_exist = cur.execute(
                "SELECT filename FROM prompts WHERE filename = ?", (filename,)
            ).fetchall()

            if len(row_exist) == 0:
                print(f"adding file {result['filename']}")
                query = f"INSERT INTO prompts (filename, prompt, ai_app, seed, image, type, cfg_scale, sampling_steps, sampler_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                cur.execute(
                    query,
                    (
                        filename,
                        result["prompt"],
                        "web-ui",
                        result["seed"],
                        result["image"],
                        result["target"],
                        result["cfg_scale"],
                        result["ddim_steps"],
                        result["sampler_name"],
                    ),
                )
                conn.commit()
        except Error as e:
            conn.close()
            raise e
    conn.close()


def main(result_path):
    results = parse_result_files(result_path)
    add_results_db(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="seed prompt db with web ui results")
    parser.add_argument(
        "--result-path",
        default="/home/bob/dev/ai-art/sd-webui/stable-diffusion-webui/outputs/txt2img/samples",
        help="the path where the image result it",
    )
    args = parser.parse_args()
    main(Path(args.result_path))
