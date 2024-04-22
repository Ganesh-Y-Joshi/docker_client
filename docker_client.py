import logging
from datetime import datetime

import hashlib

import docker


class ConsistentNode:
    """
    Represents a node in consistent hashing.

    Attributes:
        node (str): The identifier for the node.
        is_down (bool): Indicates whether the node is currently running or not.

    Methods:
        __init__(self, _node): Initializes a ConsistentNode instance.
        __str__(self): Returns a string representation of the ConsistentNode.
    """

    def __init__(self, _node):
        self.node = _node
        self.is_down = False

    def __str__(self):
        return f'{self.node} {self.is_down}'


class ConsistentHashing:
    """
        Implements consistent hashing with virtual nodes.

        Attributes:
            __ring (ConsistentHashing.Ring): The ring structure to store nodes.

        Methods:
            __init__(self, capacity): Initializes a ConsistentHashing instance with a specified capacity.
            put(self, node): Adds a node to the consistent hashing ring.
            get_request_server(self, request): Returns the node responsible for handling a given request.
            get_ring(self): Returns the underlying ring structure.
            soft_delete(self, node): Marks a node as down in a soft delete fashion.
    """

    class Hash:
        """
                Provides hashing functionality for consistent hashing.

                Methods:
                    find_index(_val, final_capacity): Calculates the hash index for a given value.
        """

        @staticmethod
        def find_index(_val, final_capacity: int):
            if _val is None:
                raise ValueError("None types are forbidden")
            return int(hashlib.md5(str(_val).encode()).hexdigest(), 16) % final_capacity

    class Ring:
        """
                Represents the ring structure in consistent hashing.

                Attributes:
                    __ring (list): The array representing the ring.
                    capacity (int): The current capacity of the ring.

                Methods:
                    __init__(self, capacity): Initializes a Ring instance with a specified capacity.
                    __str__(self): Returns a string representation of the Ring.
                    get_ring(self): Returns the underlying array representing the ring.
                    put_server(self, _node): Adds a server node to the ring.
                    should_expand(self): Checks if the ring needs expansion.
                    expand_ring(self): Expands the ring if needed.
                    get_server(self, request): Returns the server node responsible for handling a given request.
                    update(self, _node, n_node): Updates a node in the ring.
                    delete(self, _node): Deletes a node from the ring.
                    soft_delete(self, _node): Marks a node as down in a soft delete fashion.
        """

        def __init__(self, capacity: int = 1_000):
            self.__ring = [None] * capacity
            self.capacity = capacity

        def __str__(self):
            return f'{self.__ring}'

        def get_ring(self):
            """
            Give the current ring instance

            :return Current ring instance:
            """
            return self.__ring

        def put_server(self, _node):
            """
            Put the data in the following way
            find index if node at index is None
            then it will put the data else will find
            the empty spot to fill it.

            :param _node:
            :return None:
            """
            if _node is not None:
                c_index = ConsistentHashing.Hash.find_index(_node, len(self.__ring))
                if self.__ring[c_index] is None:
                    self.__ring[c_index] = _node
                else:
                    c_idx = c_index
                    for i in range(c_index, len(self.__ring)):
                        if self.__ring[i] is None:
                            self.__ring[i] = _node
                            # c_idx += 1
                            break
                        c_idx += 1

                    if c_idx == len(self.__ring):
                        for i in range(0, c_index):
                            if c_index == i + 1 and self.__ring[i] is not None:
                                self.__ring.append(_node)
                                self.expand_ring()

        def should_expand(self):
            """
            Check whether the ring should expand or not

            :return bool:
            """
            for val in self.__ring:
                if val is None:
                    return False
            return True

        def expand_ring(self):
            """
            Implements the expansion of ring and
            rearrangement of the nodes based on
            the new capacity.

            :return None:
            """
            if self.should_expand():
                n_capacity = self.capacity * 2 + 1
                n_ring = [None] * n_capacity
                for val in self.__ring:
                    idx = ConsistentHashing.Hash.find_index(val, n_capacity)
                    if n_ring[idx] is None:
                        n_ring[idx] = val
                    else:
                        v = 0
                        for i in range(idx, n_capacity):
                            if n_ring[i] is None:
                                n_ring[i] = val
                                v += 1

                        if v is n_capacity - 1 and n_ring[v] is not val:
                            for i in range(0, idx):
                                if n_ring[idx] is None:
                                    n_ring[idx] = val

                self.__ring = n_ring
                self.capacity = n_capacity

        def get_server(self, request):
            """
            Return the server instance mapped
            according to the consistent hashing algorithm

            :param request:
            :return ConsistentNode:
            """
            idx = ConsistentHashing.Hash.find_index(request, self.capacity)
            if self.__ring[idx] is not None:
                return self.__ring[idx]
            else:
                for i in range(idx, self.capacity):
                    if self.__ring[idx] is not None:
                        return self.__ring[idx]

                for i in range(0, idx):
                    if self.__ring[idx] is not None:
                        return self.__ring[idx]

                return None

        def update(self, _node, n_node):
            """
            Updates the instance of the
            server based on the consistent
            hashing algorithm.

            :param n_node:
            :param _node:
            :return bool:
            """
            expected_idx = ConsistentHashing.Hash.find_index(_node, self.capacity)
            if (self.__ring[expected_idx] is None or
                    self.__ring[expected_idx] is not _node
                    or n_node is not None):

                for i in range(expected_idx, self.capacity):
                    if self.__ring[i] is _node:
                        self.__ring[i] = n_node
                        return True

                for i in range(0, expected_idx):
                    if self.__ring[i] is _node:
                        self.__ring[i] = n_node
                        return True

            return False

        def delete(self, _node):
            """
            Deletes by updating the
            _node with None in consistent ring

            :param _node:
            :return bool:
            """
            return self.update(_node, None)

        def soft_delete(self, _node: ConsistentNode):
            """
            Soft deletes the node by
            updating the is_down attribute

            :param _node:
            :return bool:
            """

            n_node = ConsistentNode(_node.node)
            n_node.is_down = True
            return self.update(_node, n_node)

    def __init__(self, capacity):
        self.__ring = ConsistentHashing.Ring(capacity)

    def put(self, node: ConsistentNode):
        """
        api for easy calling and
        interacting with the Ring

        :param node:
        :return None:
        """
        if node is not None:
            self.__ring.put_server(node)

    def get_request_server(self, request):
        """
        Returns the server instance
        by consistent hashing algorithm

        :param request:
        :return ConsistentNode:
        """
        if request is not None:
            return self.__ring.get_server(request)

    def get_ring(self):
        """
        To return the Ring instance
        currently used

        :return Ring:
        """
        return self.__ring

    def soft_delete(self, node):
        """
        Soft deletes the instance from the ring

        :param node:
        :return bool:
        """
        if node is not None:
            return self.__ring.soft_delete(node)


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



def create_docker_instances():
    # Create a Container instance
    container_manager = Container(init_capacity=5)

    # Create 5 containers
    # for i in range(5):
    container_ports = {"5000/tcp": 9999}
    container = container_manager.create_container(
        image="my_custom_image",
        command="python3 app/app.py",
        # network_name="my_network",
        ports=container_ports
    )

    print(container)
    # List all containers
    print("All Containers:")
    # for c in container_manager.get_all():
    #     print(f"- {c.name}")

    # Start all containers
    container_manager.start_containers()


if __name__ == "__main__":
    create_docker_instances()
