from jobs.common.logger import logger
from jobs.indexing.elastic_client import ElasticsearchClient


INDEX_MAPPINGS = {

    "research_publications": {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {

                "record_id": {
                    "type": "keyword"
                },

                "doi": {
                    "type": "keyword"
                },

                "title": {
                    "type": "text"
                },

                "publication_year": {
                    "type": "integer"
                },

                "publication_date": {
                    "type": "date"
                },

                "journal_name": {
                    "type": "text"
                },

                "authors": {
                    "type": "text"
                },

                "language": {
                    "type": "keyword"
                },

                "normalized_text": {
                    "type": "text"
                },

                "source_system": {
                    "type": "keyword"
                },

                "source_url": {
                    "type": "keyword"
                },

                "business_timestamp": {
                    "type": "date"
                },

                "crawl_timestamp": {
                    "type": "date"
                },

                "is_deleted": {
                    "type": "boolean"
                }
            }
        }
    },

    "faculty_profiles": {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {

                "record_id": {
                    "type": "keyword"
                },

                "orcid": {
                    "type": "keyword"
                },

                "full_name": {
                    "type": "text"
                },

                "department": {
                    "type": "text"
                },

                "institution": {
                    "type": "text"
                },

                "country": {
                    "type": "keyword"
                },

                "email": {
                    "type": "keyword"
                },

                "normalized_text": {
                    "type": "text"
                },

                "source_system": {
                    "type": "keyword"
                },

                "source_url": {
                    "type": "keyword"
                },

                "business_timestamp": {
                    "type": "date"
                },

                "crawl_timestamp": {
                    "type": "date"
                },

                "is_deleted": {
                    "type": "boolean"
                }
            }
        }
    },

    "documents_registry": {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {

                "record_id": {
                    "type": "keyword"
                },

                "file_name": {
                    "type": "text"
                },

                "mime_type": {
                    "type": "keyword"
                },

                "file_extension": {
                    "type": "keyword"
                },

                "normalized_text": {
                    "type": "text"
                },

                "source_system": {
                    "type": "keyword"
                },

                "source_url": {
                    "type": "keyword"
                },

                "business_timestamp": {
                    "type": "date"
                },

                "crawl_timestamp": {
                    "type": "date"
                },

                "is_deleted": {
                    "type": "boolean"
                }
            }
        }
    }

}