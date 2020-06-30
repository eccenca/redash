"""Provide the query runner for eccenca Corporate Memory.

seeAlso: https://documentation.eccenca.com/
seeAlso: https://eccenca.com/
"""

import logging
from os import environ

from redash.query_runner import BaseQueryRunner
from redash.utils import json_dumps, json_loads
from . import register

try:
    from cmempy.queries import SparqlQuery
    enabled = True
except ImportError:
    enabled = False

logger = logging.getLogger(__name__)


class CorporateMemoryQueryRunner(BaseQueryRunner):
    """Use eccenca Corporate Memory as redash data source"""

    # These environment keys are used by cmempy
    KNOWN_CONFIG_KEYS = (
        'CMEM_BASE_PROTOCOL',
        'CMEM_BASE_DOMAIN',
        'CMEM_BASE_URI',
        'SSL_VERIFY',
        'REQUESTS_CA_BUNDLE',
        'DP_API_ENDPOINT',
        'DI_API_ENDPOINT',
        'OAUTH_TOKEN_URI',
        'OAUTH_GRANT_TYPE',
        'OAUTH_USER',
        'OAUTH_PASSWORD',
        'OAUTH_CLIENT_ID',
        'OAUTH_CLIENT_SECRET'
    )

    # These variables hold secret data and should be logged
    KNOWN_SECRET_KEYS = (
        'OAUTH_PASSWORD',
        'OAUTH_CLIENT_SECRET'
    )

    # This allows for an easy connection test
    noop_query = "SELECT ?noop WHERE {BIND('noop' as ?noop)}"

    # We do not want to have comment in our sparql queries
    # TODO?: Implement annotate_query in case the metadata is useful somewhere
    should_annotate_query = False

    def __init__(self, configuration):
        """init the class and configuration"""
        super(CorporateMemoryQueryRunner, self).__init__(configuration)
        """
        TODO: activate SPARQL support in the redash query editor
            Currently SPARQL syntax seems not to be available for react-ace
            component. However, the ace editor itself supports sparql mode:
            https://github.com/ajaxorg/ace/blob/master/lib/ace/mode/sparql.js
            then we can hopefully do: self.syntax = "sparql"

        TODO: implement the get_schema method to retrieve graph URIs and Query
            catalog URIs in order to use them in queries

        TODO?: implement a way to use queries from the query catalog
        """
        self.configuration = configuration

    def _setup_environment(self):
        """provide environment for cmempy

        cmempy environment variables need to match key in the properties
        object of the configuration_schema
        """
        for key in self.KNOWN_CONFIG_KEYS:
            if key in environ:
                environ.pop(key)
            value = self.configuration.get(key, None)
            if value is not None:
                environ[key] = value
                if key in self.KNOWN_SECRET_KEYS:
                    logger.info(
                        "{} set by config".format(key)
                    )
                else:
                    logger.info(
                        "{} set by config to {}".format(key, environ[key])
                    )

    def _transform_sparql_results(self, results):
        """transforms a SPARQL query result to a redash query result

        source structure: SPARQL 1.1 Query Results JSON Format
            - seeAlso: https://www.w3.org/TR/sparql11-results-json/

        target structure: redash result set
            there is no good documentation available
            so here an example result set as needed for redash:
            data = {
                "columns": [ {"name": "name", "type": "string", "friendly_name": "friendly name"}],
                "rows": [
                    {"name": "value 1"},
                    {"name": "value 2"}
                ]}

        TODO?: During the sparql_row loop, we could check the datatypes of the
            values and, in case they are all the same, choose something better than
            just string.
        """
        logger.info("results are: {}".format(results))
        # Not sure why we do not use the json package here but all other
        # query runner do it the same way :-)
        sparql_results = json_loads(results)
        # transform all bindings to redash rows
        rows = []
        for sparql_row in sparql_results["results"]["bindings"]:
            row = {}
            for var in sparql_results["head"]["vars"]:
                try:
                    row[var] = sparql_row[var]["value"]
                except KeyError:
                    # not bound SPARQL variables are set as empty strings
                    row[var] = ""
            rows.append(row)
        # transform all vars to redash columns
        columns = []
        for var in sparql_results["head"]["vars"]:
            columns.append(
                {
                    "name": var,
                    "friendly_name": var,
                    "type": "string"
                }
            )
        # Not sure why we do not use the json package here but all other
        # query runner do it the same way :-)
        return json_dumps({"columns": columns, "rows": rows})

    @classmethod
    def name(cls):
        return "eccenca Corporate Memory (SPARQL)"

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def type(cls):
        return "corporate_memory"

    def run_query(self, query, user):
        """send a sparql query to corporate memory

        TODO: check for query type and throw an error if there is a non-SELECT
                query executed
        TODO: between _setup_environment and .get_results call there is a
                possible race condition which should be avoided
        TODO: Provide error handling, especially SPARQL query error output
        """
        # allows for non-ascii chars in the query text
        query = query.encode("utf-8")
        logger.info("about to execute query: {}".format(query))
        self._setup_environment()
        data = self._transform_sparql_results(
                # allows for non-ascii chars in the result
                SparqlQuery(query).get_results().encode("utf-8")
        )
        error = None
        return data, error

    @classmethod
    def configuration_schema(cls):
        """provide the configuration of the data source as json schema

        TODO: support password grant type with OAUTH_USER and OAUTH_PASSWORD
            as well.
        TODO:
        """
        return {
            "type": "object",
            "properties": {
                "CMEM_BASE_URI": {
                    "type": "string",
                    "title": "CMEM_BASE_URL"
                },
                "OAUTH_GRANT_TYPE": {
                    "type": "string",
                    "title": "OAUTH_GRANT_TYPE (can be: password or client_credentials)",
                    "default": "client_credentials"
                },
                "OAUTH_CLIENT_ID": {
                    "type": "string",
                    "title": "OAUTH_CLIENT_ID (e.g. cmem-service-account)",
                    "default": "cmem-service-account"
                },
                "OAUTH_CLIENT_SECRET": {
                    "type": "string",
                    "title": "OAUTH_CLIENT_SECRET - only needed for grant type 'client_credentials'",
                },
                "OAUTH_USER": {
                    "type": "string",
                    "title": "OAUTH_USER (e.g. admin) - only needed for grant type 'password'"
                },
                "OAUTH_PASSWORD": {
                    "type": "string",
                    "title": "OAUTH_PASSWORD - only needed for grant type 'password'"
                },
            },
            "required": ["CMEM_BASE_URI", "OAUTH_GRANT_TYPE", "OAUTH_CLIENT_ID"],
            "secret": ["OAUTH_CLIENT_SECRET"]
        }


register(CorporateMemoryQueryRunner)
