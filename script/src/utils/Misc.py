import os
import subprocess
from kubernetes import client as Client, config as Kubeconfig

class Misc:
    def __init__(self):
        pass
    def create_credentials_file(self):

        try:
            with open('/run/secrets/mysecretuser', 'r') as user_file, open('/run/secrets/mysecretpass',
                                                                           'r') as password_file:
                username = user_file.read().strip()
                password = password_file.read().strip()
            home_directory = os.path.expanduser("~")
            file_path = os.path.join(home_directory, '.python-grid5000.yaml')

            # Store credentials file
            with open(file_path, 'w') as credentials_file:
                credentials_file.write(f'username: {username}\n')
                credentials_file.write(f'password: {password}\n')
            os.chmod(file_path, 0o600)
            print('Credentials file created successfully.')

        except FileNotFoundError:
            print('Error: Secrets files not found.')
        except Exception as e:
            print(f'Error: {str(e)}')

    def run_command(self,pod_name, command):
        kubeconfig = Kubeconfig.load_kube_config(os.environ["KUBECONFIG"])

        client = Client.CoreV1Api()
        pod_list = client.list_pod_for_all_namespaces(watch=False)
        target_pod = None
        for pod in pod_list.items:
            if pod.metadata.name.startswith(pod_name):
                target_pod = pod
                break

        if not target_pod:
            raise ValueError(f"No pod found with name: {pod_name}")

        # Execute the shell command on the pod
        command = f"kubectl exec -it {target_pod.metadata.name} -- {command}"
        subprocess.call(command, shell=True)