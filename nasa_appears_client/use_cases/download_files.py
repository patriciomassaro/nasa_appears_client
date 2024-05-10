import sys
import os


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

from nasa_appears_client.nasa_api import NasaApiConnection


# get current file parent
def get_done_tasks(nasa_api: NasaApiConnection):
    tasks = nasa_api.list_tasks()
    tasks = [(x['task_id'], x['task_name']) for x in tasks
             if
             x['status'] == 'done' and
             x['task_type'] == 'area'
             ]
    print( f"Found {len(tasks)} done tasks")
    return tasks



def download_tasks(nasa_api,download_path:str):
    tasks = get_done_tasks(nasa_api)

    for task_id, _ in tasks:
        task_files= nasa_api.get_files_from_task(task_id)
        files_to_download = [ 
                (task_file['file_id'],(task_file['file_name'].split("/")[-1]))
                for task_file in task_files['files']
                if task_file['file_type'] == 'tif'
        ]

        file_ids = [x[0] for x in files_to_download]
        file_paths = [os.path.join(download_path,f"{task_id}_{x[1]}") for x in files_to_download]

        status=nasa_api.download_files_in_parallel(task_id=task_id,
                                            file_ids=file_ids,
                                            file_paths=file_paths
                                            )


if __name__ == "__main__":
    nasa_api = NasaApiConnection(credentials_path=os.path.join(ROOT_DIR, "credentials.json"),
                                 log_file=os.path.join(ROOT_DIR,"logs","download_tasks.log")
                                 )
    download_tasks(nasa_api,
                   download_path=os.path.join(ROOT_DIR,"nasa_appears_client","use_cases","downloaded_files")
                   )