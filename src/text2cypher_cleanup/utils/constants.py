# Column names
ISSUES_COLUMN_NAME = "issues"
DATABASE_REFERENCE_ALIAS = "database_reference_alias"
CYPHER = "cypher"
INSTANCE_ID = "instance_id"
QUESTION = "question"
SCHEMA = "schema"

# Exceptions
QUERY_RUN_EXCEPTION = "query_run_exception"

# URI for neo4j demo databases
NEO4JLABS_DEMO_URI = "neo4j+s://demo.neo4jlabs.com"

# Queries to be executed to produce standardized schema
nodes_props_typesOfProps_query = r"""
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE NOT type = "RELATIONSHIP" AND elementType = "node"
WITH label AS nodeLabels, collect({property:property, type:type}) AS properties
RETURN {label: nodeLabels, properties: properties}
"""
rels_directions_query = r"""
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE type = "RELATIONSHIP" AND elementType = "node"
RETURN {source: label, relationship: property, target: other}
"""
rels_props_typesOfProps_query = r"""
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE NOT type = "RELATIONSHIP" AND elementType = "relationship"
WITH label AS nodeLabels, collect({property:property, type:type}) AS properties
RETURN {relationship: nodeLabels, properties: properties}
"""
