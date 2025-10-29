from common.base_tasks import Task
from utils.file_utils import get_path

start_task = Task("start_task", lambda: print("Pipeline started"))
get_file_path_task = Task(
    "print_file_path", lambda: get_path("data/stations/seoul_station.xlsx")
)
print_task = Task("end_task", lambda: print("File_path: ", get_file_path_task.result))

start_task >> get_file_path_task >> print_task


def run_test_pipeline():
    start_task.run()