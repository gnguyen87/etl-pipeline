import requests
import json
import os
from datetime import datetime

year = "2013"
grade = "3"

def build_url(resource, grade, year):
    base_url = "https://educationdata.urban.org/api/v1/school-districts"
    url_dict = {
        "directory": f"{base_url}/ccd/directory/{year}",
        "enrollment": f"{base_url}/ccd/enrollment/{year}/grade-{grade}/race",
        "assessment": f"{base_url}/edfacts/assessments/{year}/grade-{grade}/race"
    }
    final_url = url_dict[resource]
    return(final_url)

def fetch_data(url):
    data = []
    response = requests.get(url).json()
    data.extend(response["results"])

    while response["next"] is not None:
        url = response["next"]
        response = requests.get(url).json()
        data.extend(response["results"])

    return(data)


def extract_education_data(resource):
    url = build_url(resource, grade, year)
    data = fetch_data(url)

    run_date = datetime.now().strftime('%Y-%m-%d')
    file_path = os.path.join('/efs', f"{run_date}_{resource}_grade{grade}_{year}.csv")

    with open(file_path, 'w') as f:
        for row in data:
            f.write(json.dumps(row) + '\n')

    return file_path

# if __name__ == "__main__":
#     extract_education_data('directory')
    

   






