"""Module for client docker."""
from typing import Optional, Union

import constants
from clients.base_client import BaseClient, ResultT
from helpers import exceptions, output_handler
from helpers.cli_cmd_maker import CommandMapperBase, Field


class DockerCommandBase(CommandMapperBase):
    """Docker command creator."""

    main_command = 'docker'


class DockerCommandRun(DockerCommandBase):
    """Command `run` attributes."""

    inner_command = 'run'

    detach: bool = Field(
        option='-d',
        default=True,
        description='Run container in background and print container ID.',
    )
    env: Optional[list[str]] = Field(
        option='-e',
        description='Set environment variables.',
    )
    publish: Optional[list[str]] = Field(
        option='-p',
        description='Publish a container’s port(s) to the host. Example: 5000:5000.',
    )
    net: Optional[str] = Field(
        option='--net',
        description='Connect a container to a network',
    )
    restart: Optional[str] = Field(
        option='--restart',
        description='Restart policy to apply when a container exits.',
    )
    name: Optional[str] = Field(
        option='--name',
        description='Assign a name to the container.',
    )
    volume: Optional[str] = Field(
        option='-v',
        description='Bind mount a volume.',
    )
    image: str = Field(
        option='',
        description='Image to run.',
    )


class DockerCommandPull(DockerCommandBase):
    """Command `pull` attributes."""

    inner_command = 'pull'

    image: str = Field(
        option='',
        description='Image name.',
    )


class DockerCommandPush(DockerCommandPull):
    """Command `push` attributes."""

    inner_command = 'push'


class DockerCommandStop(DockerCommandBase):
    """Command `stop` attributes."""

    inner_command = 'stop'

    container: Union[str, list] = Field(
        option='',
        description='Container name.',
    )


class DockerCommandRm(DockerCommandStop):
    """Command `rm` attributes."""

    inner_command = 'rm'


class DockerCommandTag(DockerCommandBase):
    """Command `tag` attributes."""

    inner_command = 'tag'

    source_image: str = Field(
        option='',
        description='Source image.',
    )
    target_image: str = Field(
        option='',
        description='Target image.',
    )


class DockerCommandImages(DockerCommandBase):
    """
    Command `images` attributes.

    Example output:
        {
        'Containers': 'N/A',
        'CreatedAt': '2023-04-25 10:30:49 -0700 PDT',
        'CreatedSince': '4 weeks ago',
        'Digest': '<none>',
        'ID': '3b418d7b466a',
        'Repository': 'ubuntu',
        'SharedSize': 'N/A',
        'Size': '77.8MB',
        'Tag': 'latest',
        'UniqueSize': 'N/A',
        'VirtualSize': '77.81MB',
        }

    """

    inner_command = 'images'

    format: Optional[str] = Field(
        option='--format',
        default='"{{json .}}"',  # https://docs.docker.com/config/formatting/
        description='Format output using a custom template.',
    )


class DockerGroupImage(DockerCommandBase):
    """Group of Docker commands `image`."""

    command_group = 'image'


class DockerImageCommandPrune(DockerGroupImage):
    """Command Image 'prune'."""

    inner_command = 'prune'

    all: bool = Field(
        option='--all',
        default=False,
        description='Remove all unused images, not just dangling ones.',
    )
    force: bool = Field(
        option='--force',
        default=True,
        description='Do not prompt for confirmation.',
    )


class DockerImageCommandRm(DockerGroupImage):
    """Command Image 'rm'."""

    inner_command = 'rm'

    name: str = Field(
        option='',
        description='Image name.',
    )
    force: bool = Field(
        option='--force',
        default=True,
        description='Do not prompt for confirmation.',
    )


class DockerBase(BaseClient):
    """Base class for DOcker related clients."""

    client_reaction = output_handler.OutputReaction(
        prefix='DOCKER',
        module_name=__name__,
    )

    def invoke_command(self, command: str) -> ResultT:
        """
        Invoke command.

        Args:
            command (str): command name

        Returns:
            (ResultT): result of command, process
        """
        try:
            return super().invoke_command(command=command)

        # docker considers warnings as errors.
        # i.e. it is not clear how to catch warnings (not all of them contain word `Warning`)
        except exceptions.InvokingCommandError as err_invoke:
            self.client_reaction(
                msg=[
                    'Caught error from invoke command `{0}`'.format(
                        command,
                    ),
                    err_invoke,
                ],
                severity=constants.SEV_WARNING,
            )


class DockerImage(DockerBase):
    """Subcommand `image` to manage images."""

    def prune(self, **options):
        """
        Invoke Image `prune` command.

        Remove unused images

        Args:
            options: command options

        Returns:
            out (str): result of command

        """
        command_maker = DockerImageCommandPrune(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )

    def rm(self, **options) -> ResultT:
        """
        Invoke Image `rm` command.

        Remove one or more images.

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = DockerImageCommandRm(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )


class Docker(DockerBase):
    """Main DOcker client class representation."""

    def __init__(self):
        super().__init__()
        self.image = DockerImage()

    def run(self, **options) -> ResultT:
        """
        Invoke Docker `run` command.

        Create and run a new container from an image.

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = DockerCommandRun(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )

    def pull(self, **options) -> ResultT:
        """
        Invoke Docker `pull` command.

        Download an image from a registry.

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = DockerCommandPull(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )

    def push(self, **options) -> ResultT:
        """
        Invoke Docker `push` command.

        Upload an image to a registry.

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = DockerCommandPush(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )

    def tag(self, **options) -> ResultT:
        """
        Invoke Docker `tag` command.

        Create a tag TARGET_IMAGE that refers to SOURCE_IMAGE.

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = DockerCommandTag(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )

    def stop(self, **options) -> ResultT:
        """
        Invoke Docker `stop` command.

        Stop one or more running containers

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = DockerCommandStop(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )

    def rm(self, **options) -> ResultT:
        """
        Invoke Docker `rm` command.

        Remove one or more containers

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = DockerCommandRm(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )

    def images(self, **options) -> ResultT:
        """
        Invoke Docker `images` command.

        List images.

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = DockerCommandImages(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )
