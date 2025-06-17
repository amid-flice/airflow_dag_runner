import requests
import time


def list_of_str(arg):
    return arg.split(',')


class AirflowDagRunner:
    _API_DAGS_URI = 'api/v2/dags'

    def __init__(self, airflow_url: str, username: str, password: str, log_sources: list[str]):
        self.airflow_url = airflow_url.rstrip('/')
        self.log_sources = log_sources
        self.session = requests.Session()
        self.access_token = self._get_access_token(username, password)
        self.session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        })

    def _get_access_token(self, username, password):
        """Получение JWT-токена для аутентификации."""
        auth_url = f"{self.airflow_url}/auth/token"
        try:
            response = self.session.post(
                auth_url,
                json={"username": username, "password": password}
            )
            response.raise_for_status()
            return response.json().get("access_token")
        except Exception as e:
            raise Exception(f"Ошибка аутентификации: {str(e)}")

    def trigger_dag(self, dag_id, conf=None):
        """Запуск DAG с параметрами."""
        trigger_url = f"{self.airflow_url}/{self._API_DAGS_URI}/{dag_id}/dagRuns"
        payload = {
            "logical_date": None,
            "conf": conf or {}
        }
        try:
            response = self.session.post(trigger_url, json=payload)
            response.raise_for_status()
            return response.json().get("dag_run_id")
        except Exception as e:
            raise Exception(f"Ошибка запуска DAG: {str(e)}")

    def monitor_dag(self, dag_id, dag_run_id, interval=5):
        """Мониторинг статуса DAG и вывод логов."""
        status_url = f"{self.airflow_url}/{self._API_DAGS_URI}/{dag_id}/dagRuns/{dag_run_id}"
        tasks_url = f"{self.airflow_url}/{self._API_DAGS_URI}/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        completed_states = {"success", "failed"}
        running_states = {"running"}
        logged_tasks = set()
        task_log_cutoffs = dict()

        while True:
            try:
                # Проверка статуса DAG
                status_resp = self.session.get(status_url)
                status_resp.raise_for_status()
                state = status_resp.json().get("state")

                # Проверка логов задач
                tasks_resp = self.session.get(tasks_url)
                tasks_resp.raise_for_status()
                tasks = tasks_resp.json().get("task_instances", [])

                for task in tasks:
                    task_id = task.get("task_id")
                    task_state = task.get("state")
                    if task_id in logged_tasks:
                        continue
                    if task_state in [*running_states, *completed_states]:
                        cutoff = self._print_task_logs(dag_id, dag_run_id, task_id,
                                                       cutoff=task_log_cutoffs.get(task_id))
                        task_log_cutoffs[task_id] = cutoff
                    if task_state in completed_states:
                        logged_tasks.add(task_id)


                if state in completed_states:
                    print(f"DAG завершен со статусом: {state}")
                    break

                time.sleep(interval)

            except Exception as e:
                print(f"Ошибка мониторинга: {str(e)}")
                break

    def _print_task_logs(self, dag_id, dag_run_id, task_id, cutoff: str = None) -> str:
        """Вывод логов задачи."""
        log_url = f"{self.airflow_url}/{self._API_DAGS_URI}/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/1"

        try:
            log_resp = self.session.get(log_url, params={"full_content": True})
            log_resp.raise_for_status()
            log_content = log_resp.json().get("content", "Логи отсутствуют")
            filtered_logs = [r for r in log_content if not self.log_sources or r.get("logger") in self.log_sources
                             and (not cutoff or r.get('timestamp') > cutoff)]
            for log_item in filtered_logs:
                ts = log_item['timestamp']
                event = log_item.get('event')
                level = log_item.get('level')

                error_detail = None
                try:
                    error_detail = log_item['error_detail'][0]['exc_value']
                except (IndexError, KeyError, TypeError):
                    pass

                msg = f"{event}: {error_detail}" \
                    if level.lower() == 'error' and error_detail \
                    else event
                print(f"{ts} - {task_id}: [{level.upper()}] {msg}")
            return filtered_logs[-1]['timestamp'] if filtered_logs else cutoff
        except Exception as e:
            print(f"Не удалось получить логи для задачи {task_id}: {str(e)}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Airflow DAG Runner")
    parser.add_argument("--url", required=True, help="Airflow Base URL (e.g., http://localhost:8080)")
    parser.add_argument("--dag", required=True, help="DAG ID to trigger")
    parser.add_argument("--user", required=True, help="Airflow username")
    parser.add_argument("--password", required=True, help="Airflow password")
    parser.add_argument("--interval", type=int, default=5, help="Status check interval in seconds")
    parser.add_argument("--log_sources", type=list_of_str, default="root,task",
                        help="Names of log sources, separated by comma without spaces.")
    args = parser.parse_args()

    try:
        runner = AirflowDagRunner(args.url, args.user, args.password, args.log_sources)
        dag_run_id = runner.trigger_dag(args.dag)
        print(f"DAG Run ID: {dag_run_id}")
        runner.monitor_dag(args.dag, dag_run_id, args.interval)
    except Exception as e:
        print(f"Ошибка: {str(e)}")
        exit(1)
