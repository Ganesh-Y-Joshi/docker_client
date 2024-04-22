import logging
from datetime import datetime

import docker


class FileLogger:
    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self._configure_logging()

    def _configure_logging(self):
        logging.basicConfig(
            filename=self.log_file_path,
            level=logging.INFO,
            format="[%(asctime)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

    def log(self, data):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{current_time}] {data}"
        logging.info(log_entry)


class Container:
    def __init__(self, init_capacity, client=docker.client.from_env()):
        if init_capacity <= 0:
            init_capacity = 1_000
        self.container_registry = ConsistentHashing(init_capacity)
        self.client = client
        self.logger = FileLogger("WAL_LOG")
        self.containers = []

    def create_container(self, image, command, network_name=None, restart_policy=None, ports=None,
                         environment=None, volumes=None):
        container = self.client.containers.create(
            image=image,
            command=command,
            network=network_name,
            restart_policy=restart_policy,
            ports=ports,
            environment=environment,
            volumes=volumes,
            detach=True
        )
        self.containers.append(container)
        self.container_registry.put(ConsistentNode(container.name))
        self.logger.log(f"Container '{container.name}' created and added to the consistent hash ring.")
        return container

    def connect_container_to_network(self, container, network_name):
        try:
            container.connect(network_name)
            self.logger.log(f"Container '{container.name}' connected to network '{network_name}'.")
        except docker.errors.APIError as e:
            self.logger.log(f"Error connecting container '{container.name}' to network '{network_name}': {e}")

    def start_containers(self):
        for container in self.containers:
            container.start()
        self.logger.log("All containers started.")

    def stop_one(self, container):
        for _container in self.containers:
            if _container == container:
                _container.stop()
                self.container_registry.soft_delete(ConsistentNode(_container.name))
                self.logger.log(f"Stopped container {_container.id}")

    def remove_containers(self):
        for container in self.containers:
            container.remove()
            self.container_registry.soft_delete(ConsistentNode(container.name))
            self.logger.log(f"Container '{container.name}' removed and marked as down in the consistent hash ring.")
        self.logger.log("All containers removed.")

    def prune(self, filters=None):
        result = self.client.containers.prune(filters=filters)
        self.logger.log(f"Pruned containers: {result}")

    def get_all(self):
        return self.client.containers.list()

    def get_running_containers(self):
        filters = {"status": ["running", "starting"]}
        return self.client.containers.list(filters=filters)

    def get_container_logs(self, container, tail=10):
        try:
            logs = container.logs(tail=tail).decode("utf-8")
            self.logger.log(f"Retrieved logs for container '{container.name}':\n{logs}")
            return logs
        except docker.errors.APIError as e:
            self.logger.log(f"Error retrieving logs for container '{container.name}': {e}")
            return None

    def monitor_container_stats(self, container):
        try:
            stats_stream = container.stats(decode=True, stream=True)
            for stats in stats_stream:
                # Process container stats as needed
                self.logger.log(f"Container stats for '{container.name}': {stats}")
        except docker.errors.APIError as e:
            self.logger.log(f"Error monitoring container stats for '{container.name}': {e}")


class NetworkManager:
    def __init__(self, client=docker.client.from_env()):
        self.client = client
        self.logger = FileLogger("WAL_LOG")
        self.networks = []

    def create_network(self, name, driver="bridge", options=None):
        network = self.client.networks.create(
            name=name,
            driver=driver,
            options=options
        )
        self.networks.append(network)
        self.logger.log(f"Network '{network.name}' created.")
        return network

    def remove_network(self, network):
        try:
            network.remove()
            self.networks.remove(network)
            self.logger.log(f"Network '{network.name}' removed.")
        except docker.errors.APIError as e:
            self.logger.log(f"Error removing network '{network.name}': {e}")

    def get_all_networks(self):
        return self.client.networks.list()

    def connect_container_to_network(self, container, network):
        try:
            network.connect(container)
            self.logger.log(f"Container '{container.name}' connected to network '{network.name}'.")
        except docker.errors.APIError as e:
            self.logger.log(f"Error connecting container '{container.name}' to network '{network.name}': {e}")

    def disconnect_container_from_network(self, container, network):
        try:
            network.disconnect(container)
            self.logger.log(f"Container '{container.name}' disconnected from network '{network.name}'.")
        except docker.errors.APIError as e:
            self.logger.log(f"Error disconnecting container '{container.name}' from network '{network.name}': {e}")


class ImageManager:
    def __init__(self, client=docker.client.from_env()):
        self.client = client
        self.logger = FileLogger("WAL_LOG")
        self.images = []

    def pull_image(self, repository, tag="latest"):
        try:
            image = self.client.images.pull(repository, tag=tag)
            self.images.append(image)
            self.logger.log(f"Image '{repository}:{tag}' pulled.")
            return image
        except docker.errors.APIError as e:
            self.logger.log(f"Error pulling image '{repository}:{tag}': {e}")
            return None

    def remove_image(self, image, force=False):
        try:
            self.client.images.remove(image.id, force=force)
            self.images.remove(image)
            self.logger.log(f"Image '{image.id}' removed.")
        except docker.errors.APIError as e:
            self.logger.log(f"Error removing image '{image.id}': {e}")

    def get_all_images(self):
        return self.client.images.list()

    def search_images(self, term):
        return self.client.images.search(term)

    def build_image(self, path, tag, dockerfile=None, buildargs=None):
        """
        Build a Docker image from a specified build context.

        Args:
            path (str): Path to the build context (directory containing Dockerfile and build files).
            tag (str): Tag to assign to the built image.
            dockerfile (str, optional): Path to the Dockerfile within the build context. Defaults to None.
            buildargs (dict, optional): A dictionary of build arguments to pass to the build process. Defaults to None.

        Returns:
            docker.models.images.Image: The built Docker image.
        """
        try:
            image, build_logs = self.client.images.build(
                path=path,
                tag=tag,
                dockerfile=dockerfile,
                buildargs=buildargs,
            )

            # Optionally, you can log build logs if needed
            for log_line in build_logs:
                self.logger.log(log_line)

            self.images.append(image)
            self.logger.log(f"Image '{tag}' created from build context at '{path}'.")
            return image
        except docker.errors.BuildError as build_error:
            self.logger.log(f"Build failed for image '{tag}': {build_error}")
            return None
        except docker.errors.APIError as api_error:
            self.logger.log(f"Error building image '{tag}': {api_error}")
            return None
