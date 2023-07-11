"""Module for hadoop-related clients."""
import os
from typing import Optional
from xml.etree import ElementTree

import constants
from clients.base_client import BaseClient, ResultT
from helpers.cli_cmd_maker import CommandMapperBase, Field
from helpers.output_handler import OutputReaction


class StartDFS(CommandMapperBase):
    """Hadoop: start DFS. Starts the Hadoop DFS daemons, the namenode and datanodes."""

    main_command = os.path.join(
        constants.HADOOP_HOME,
        'sbin/start-dfs.sh',
    )


class StopDFS(CommandMapperBase):
    """Hadoop: stop DFS. Stops the Hadoop DFS daemons, the namenode and datanodes."""

    main_command = os.path.join(
        constants.HADOOP_HOME,
        'sbin/stop-dfs.sh',
    )


class StopAll(CommandMapperBase):
    """Hadoop: stop all. Stops all Hadoop daemons."""

    main_command = os.path.join(
        constants.HADOOP_HOME,
        'sbin/stop-all.sh',
    )


class HadoopCmd(CommandMapperBase):
    """Hadoop main command."""

    main_command = os.path.join(
        constants.HADOOP_HOME,
        'bin/hadoop',
    )

    jar: Optional[str] = Field(
        option='jar',
        description='Run a jar file.',
    )


class HadoopStreaming(HadoopCmd):
    """
    Hadoop Streaming: create and run Map/Reduce jobs.

    https://hadoop.apache.org/docs/r1.0.4/streaming.html#More+usage+examples
    """

    input: str = Field(
        option='-input',
        description='Input location for mapper.',
    )
    output: str = Field(
        option='-output',
        description='Output location for reducer.',
    )
    mapper: str = Field(
        option='-mapper',
        description='Mapper executable.',
    )
    reducer: str = Field(
        option='-reducer',
        description='Reducer executable.',
    )


class JPS(CommandMapperBase):
    """Tool lists the instrumented HotSpot Java Virtual Machines (JVMs) on the target system."""

    main_command = 'jps'


class Hadoop(BaseClient):
    """Hadoop client class."""

    client_reaction = OutputReaction(
        prefix='HADOOP',
        module_name=__name__,
    )
    hadoop_home = constants.HADOOP_HOME

    def start_dfs(self, **options) -> ResultT:
        """
        Invoke Hadoop `start-dfs.sh` command.

        Starts the Hadoop DFS daemons, the namenode and datanodes.

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = StartDFS(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )

    def stop_dfs(self, **options):
        """
        Invoke Hadoop `stop-dfs.sh` command.

        Stops the Hadoop DFS daemons, the namenode and datanodes.

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = StopDFS(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )

    def stop_all(self, **options):
        """
        Invoke Hadoop `stop-all.sh` command.

        Stops all Hadoop daemons.

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = StopAll(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )

    def run_mapreduce_stream(self, **options):
        """
        Create and run Hadoop Map/Reduce job.

        Args:
            options: command options

        Returns:
            (ResultT): result of command, process

        """
        command_maker = HadoopStreaming(**options)

        return self.invoke_command(
            command=command_maker.make_command(),
        )

    def check_nodes_state(self):
        """Check for state Hadoop DFS daemons: DataNode and SecondaryNameNode."""
        command_maker = JPS()
        response, _, _ = self.invoke_command(
            command=command_maker.make_command(),
        )
        if 'DataNode' in response or 'NameNode' in response:
            raise RuntimeError('Hadoop daemons are already running.')

    def edit_config_core_xml(self, access_key: str, secret_key: str, endpoint_url: str):
        """
        Edit config file: core-site.xml.

        Location: hadoop_home/etc/hadoop/core-site.xml

        Args:
            access_key (str): S3 service access key
            secret_key (str): S3 service secret key
            endpoint_url (str): S3 endpoint
        """
        core_cml_file = os.path.join(
            self.hadoop_home,
            'etc/hadoop/core-site.xml',
        )

        # name of props according Hadoop config file structure
        props = {
            'fs.s3a.access.key': access_key,
            'fs.s3a.secret.key': secret_key,
            'fs.s3a.endpoint': endpoint_url,
        }

        self._edit_hadoop_related_xml(core_cml_file, props)

    def _edit_hadoop_related_xml(self, xml_file: str, new_props: dict):
        """
        Edit Hadoop related config XML file.

        All Hadoop config XML files have same structure:
            <configuration>
                <property>
                      <name></name>
                      <value></value>
                      <description></description>
                </property>
                ...
            </configuration>

        Args:
            xml_file (str): config file to edit
            new_props (dict): new data for updating config

        """
        if not os.path.exists(xml_file):
            raise FileExistsError('Provided XML file does not exist.')

        xml = ElementTree.parse(xml_file)

        root = xml.getroot()

        edited_props = []

        # name of props according Hadoop config file structure
        for prop in root.iter('property'):
            prop_name = prop.find('name').text
            if prop_name in new_props:
                for parameter in prop.iter():
                    if parameter.tag == 'value':
                        parameter.text = new_props.get(prop_name)
                        edited_props.append(prop_name)

        xml.write(xml_file)

        self.client_reaction(
            msg='XML File `{0}` has been changed. Edited fields: {1}'.format(
                xml_file,
                edited_props,
            ),
            severity=constants.SEV_INFO,
        )


hadoop = Hadoop()
