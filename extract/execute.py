import time
import os
import sys
import requests
import json
from zipfile import ZipFile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time



def download_zip_file(logger, url, output_dir):
    response = requests.get(url,stream=True)
    os.makedirs(output_dir, exist_ok=True)
    logger.debug("Downloading start")
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename,"wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"Downloaded zip file : {filename}")
        return filename
    else:
        logger.error("Unsuccessfull")
        raise Exception(f"Failed to download file. Status coe {response.status_code}")
    
def extract_zip_file(logger,zip_filename, output_dir):
    logger.info("Extracting the zip file")
    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)
    
    logger.info(f"Extracted files written to : {output_dir}")
    logger.info("Removing the zip file")
    os.remove(zip_filename)

def fix_json_dict(logger, output_dir):
    # imported json at first
    file_path = os.path.join(output_dir,"dict_artists.json")
    with open(file_path, "r") as f:
        data = json.load(f)

    with open(os.path.join(output_dir, "fixed_da.json"), "w", encoding="utf-8") as f_out:
        for key, value in data.items():
            record = {"id": key, "related ids": value}
            json.dump(record, f_out, ensure_ascii=False)
            f_out.write("\n")
        logger.info("Removing the original file")
        os.remove(file_path)

if __name__ == "__main__":

    logger = setup_logging("extract.log")

    if len(sys.argv) < 2:
        logger.error("Extraction path is required")
        logger.error("Exame Usage:")
        logger.error("python3 execute.py /home/prajwal/Data/Extraction")
    else:
        try:
            logger.info("Starting Extraction Engine...")
            EXTRACT_PATH = sys.argv[1]
            KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1993933/3294812/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250728%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250728T021514Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=3ddce2418f2b89fa8eae5c3e950a77dcece936fed74a1f7598b6b8cb834206a75e49494a80503ca5ff9ad10cc9834324a27e823baf615de7d63dec5e56cf5a1762058b6e38c9e0d33687ab787da14acc23e50cf57cf927975fa0d5625f10d21372905bd15028fe9dc6e24e3a67e1563b64050a65f0997ad49dbbce25e859ab7e743db449d2fc61ca939f53fc4c6fa3021e678d25365f490fe5b7e0c543c68243f494b11c9d508cb99415847e7d25b4b39c82d27f36a867734afee56fa240e6303510b18b7843241a459ced65f4a3b04f68047790d29b9d5f56ebea02ec098623bafd851df180197bf9fae13853affd5d2205a82f6517e815623ddb14291d2cb8"
            
            start = time.time()
            
            zip_filename = download_zip_file(logger, KAGGLE_URL,EXTRACT_PATH)
            extract_zip_file(logger, zip_filename, EXTRACT_PATH)
            fix_json_dict(logger, EXTRACT_PATH)
            end = time.time()
            logger.info("Extraction Sucessfully Complete!!!")
            logger.info(f"Total time taken {format_time(end-start)}")

        except Exception as e:
            logger.error(f"Error: {e}")