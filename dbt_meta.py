import os
import re

from dbt.adapters.factory import get_adapter, Adapter
from dbt.config import RuntimeConfig
from dbt.contracts.connection import Connection
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedModelNode, DependsOn
from dbt.lib import get_dbt_config
from dbt.node_types import NodeType
from dbt.parser.manifest import ManifestLoader
from dbt.tracking import disable_tracking


class DbtProject:
    project_dir: str
    config: RuntimeConfig
    adapter: Adapter
    connection: Connection
    manifest: Manifest

    def __init__(self, project_dir: str, profiles_dir: str = None):
        disable_tracking()
        if profiles_dir is not None:
            os.environ['DBT_PROFILES_DIR'] = profiles_dir
        self.project_dir = project_dir
        self.config = get_dbt_config(self.project_dir, single_threaded=True)
        self.manifest = ManifestLoader.get_full_manifest(self.config)
        self.adapter = get_adapter(self.config)
        self.connection = self.adapter.acquire_connection()
        print('Init finish')

    def get_metrics(self):
        return self.manifest.metrics

    def render_metric(self, metric_name):
        ...

    def compile_sql(self, sql, depends_on):
        compiler = self.adapter.get_compiler()

        depends_on_data = {
            'macros': [
            ],
            'nodes': [
                'model.metrics.dbt_metrics_default_calendar',
            ]}

        checksum = FileHash.from_contents(sql)
        data = {
            # 'config': self.config.serialize(),
            'original_file_path': 'tmp.sql',
            'path': 'tmp.sql',
            'root_path': self.config.project_root,
            'checksum': checksum.to_dict(),
            'alias': 'custom_sql',
            'name': 'custom_sql',
            'depends_on': depends_on_data,
            'package_name': self.config.project_name,
            'unique_id': 'custom_sql',
            'fqn': ['custom_sql'],
            'database': self.config.credentials.database,
            'schema': self.config.credentials.schema,
            'resource_type': NodeType.Model,
            'raw_sql': sql,
        }

        ParsedModelNode.validate(data)
        node = ParsedModelNode.from_dict(data)

        self.manifest.nodes['custom_sql'] = node
        self.adapter.acquire_connection()
        r = compiler.compile_node(node, manifest=self.manifest)

        compiled_sql = r.compiled_sql
        # print('compiled:', compiled_sql)

        return compiled_sql
