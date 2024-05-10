import sys
import os
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

sys.path.append(ROOT_DIR)
from nasa_appears_client.nasa_api import NasaApiConnection

def delete_all_tasks(nasa_api):
    tasks= nasa_api.list_tasks()

    ids_to_delete = [x['task_id'] for x in tasks]

    for task_id in ids_to_delete:
        nasa_api.delete_task(task_id=task_id)
    return True

if __name__ == "__main__":
    nasa_api = NasaApiConnection(credentials_path=os.path.join(ROOT_DIR, "credentials.json"),
                                 log_file="logs/delete_tasks.log")
    delete_all_tasks(nasa_api)
    print("All tasks deleted")
