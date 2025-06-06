import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
import json
import re
import time
from urllib.parse import urljoin, urlparse
import sqlite3
import hashlib
from typing import List, Dict, Tuple
import openai
from sentence_transformers import SentenceTransformer
import faiss
import pickle
import os
from dataclasses import dataclass
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DocumentChunk:
    """Represents a chunk of documentation with metadata"""
    content: str
    url: str
    title: str
    section: str
    chunk_id: str
    timestamp: datetime

class CelonisDocScraper:
    """Scrapes Celonis documentation and community content"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
# Key Celonis documentation URLs
        self.doc_urls = {
            'celonis_developer_center': 'https://docs.celonis.com/',
            'celonis_academy': 'https://www.celonis.com/academy/',
            'celonis_community': 'https://community.celonis.com/',
            'celonis_trust_center': 'https://www.celonis.com/trust-center/',
            'managing_user_profile': 'https://docs.celonis.com/en/platform/help/getting-started/managing-your-user-profile.html',
            'troubleshooting_access': 'https://docs.celonis.com/en/platform/help/getting-started/troubleshooting-access-to-your-celonis-platform.html',
            'feature_release_types': 'https://docs.celonis.com/en/platform/help/release-notes/feature-release-types.html',
            'private_public_preview_features': 'https://docs.celonis.com/en/platform/help/release-notes/features-currently-in-private-preview-and-public-preview.html',
            'planned_releases': 'https://docs.celonis.com/en/platform/help/release-notes/planned-releases.html',
            'june_2025_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/june-2025-release-notes.html',
            'may_2025_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/may-2025-release-notes.html',
            'april_2025_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/april-2025-release-notes.html',
            'march_2025_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/march-2025-release-notes.html',
            'february_2025_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/february-2025-release-notes.html',
            'january_2025_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/january-2025-release-notes.html',
            'december_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/december-2024-release-notes.html',
            'november_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/november-2024-release-notes.html',
            'october_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/october-2024-release-notes.html',
            'september_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/september-2024-release-notes.html',
            'august_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/august-2024-release-notes.html',
            'july_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/july-2024-release-notes.html',
            'june_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/june-2024-release-notes.html',
            'may_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/may-2024-release-notes.html',
            'april_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/april-2024-release-notes.html',
            'march_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/march-2024-release-notes.html',
            'february_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/february-2024-release-notes.html',
            'january_2024_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/january-2024-release-notes.html',
            'december_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/december-2023-release-notes.html',
            'november_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/november-2023-release-notes.html',
            'october_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/october-2023-release-notes.html',
            'september_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/september-2023-release-notes.html',
            'august_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/august-2023-release-notes.html',
            'july_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/july-2023-release-notes.html',
            'june_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/june-2023-release-notes.html',
            'may_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/may-2023-release-notes.html',
            'april_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/april-2023-release-notes.html',
            'march_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/march-2023-release-notes.html',
            'february_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/february-2023-release-notes.html',
            'january_2023_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/january-2023-release-notes.html',
            'december_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/december-2022-release-notes.html',
            'november_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/november-2022-release-notes.html',
            'october_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/october-2022-release-notes.html',
            'september_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/september-2022-release-notes.html',
            'august_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/august-2022-release-notes.html',
            'july_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/july-2022-release-notes.html',
            'june_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/june-2022-release-notes.html',
            'may_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/may-2022-release-notes.html',
            'april_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/april-2022-release-notes.html',
            'march_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/march-2022-release-notes.html',
            'february_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/february-2022-release-notes.html',
            'january_2022_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/january-2022-release-notes.html',
            'december_2021_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/december-2021-release-notes.html',
            'november_2021_release_notes': 'https://docs.celonis.com/en/platform/help/release-notes/november-2021-release-notes.html',
            'creating_managing_data_pools': 'https://docs.celonis.com/en/platform/help/data-integration/data-pools/creating-and-managing-data-pools.html',
            'data_pool_versioning': 'https://docs.celonis.com/en/platform/help/data-integration/data-pools/versioning.html',
            'data_pool_parameters': 'https://docs.celonis.com/en/platform/help/data-integration/data-pools/data-pool-parameters.html',
            'sharing_data_between_data_pools': 'https://docs.celonis.com/en/platform/help/data-integration/data-pools/sharing-data-between-data-pools.html',
            'process_connector_templates_quickstarts': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/using-process-connector-templates-quickstarts.html',
            'event_logs_file_upload': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/event-logs-file-upload.html',
            'google_sheets_quickstart': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/google-sheets.html',
            'csv_xlsx_quickstart': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/csv-and-xlsx.html',
            'extensible_event_stream_xes': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/extensible-event-stream-xes.html',
            'understanding_date_time_formats': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/understanding-date-and-time-formats.html',
            'sap_accounts_payable': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/sap-accounts-payable.html',
            'sap_order_to_cash': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/sap-order-to-cash.html',
            'sap_purchase_to_pay': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/sap-purchase-to-pay.html',
            'sap_accounts_receivable': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/sap-accounts-receivable.html',
            'oracle_ebs_accounts_payable': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/oracle-ebs-accounts-payable.html',
            'oracle_ebs_order_to_cash': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/oracle-ebs-order-to-cash.html',
            'oracle_ebs_purchase_to_pay': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/oracle-ebs-purchase-to-pay.html',
            'salesforce_case_management': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/salesforce-case-management.html',
            'salesforce_opportunity_management': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/salesforce-opportunity-management.html',
            'servicenow_incident_management': 'https://docs.celonis.com/en/platform/help/data-integration/process-connector-templates-quickstarts/servicenow-incident-management.html',
            'connecting_data_sources': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/connecting-data-sources.html',
            'cloud_based_data_sources': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/cloud-based-data-sources.html',
            'additional_cloud_based_sources': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/additional-cloud-based-sources.html',
            'celonis_platform_adoption_extractor': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/celonis-platform-adoption-extractor.html',
            'coupa': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/coupa.html',
            'adding_custom_tables_columns_coupa': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/adding-custom-tables-and-columns-to-coupa-extraction.html',
            'google_calendar': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/google-calendar.html',
            'gmail': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/gmail.html',
            'google_sheets': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/google-sheets.html',
            'oracle_bi_publisher': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/oracle-bi-publisher.html',
            'oracle_fusion_cloud': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/oracle-fusion-cloud.html',
            'oracle_fusion_cloud_bicc': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/oracle-fusion-cloud-bicc.html',
            'oracle_fusion_cloud_rest': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/oracle-fusion-cloud-rest.html',
            'salesforce_connector': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/salesforce.html',
            'sap_ariba': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/sap-ariba.html',
            'supported_sap_ariba_api_endpoints': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/supported-sap-ariba-api-endpoints.html',
            'asynchronous_apis_sap_ariba': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/asynchronous-apis-for-sap-ariba.html',
            'synchronous_apis_sap_ariba': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/synchronous-apis-for-sap-ariba.html',
            'configuring_sap_ariba_templates_async': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/configuring-sap-ariba-templates-for-asynchronous-apis.html',
            'optimizing_sap_ariba_extractions': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/optimizing-sap-ariba-extractions.html',
            'sap_s4hana_public_cloud': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/sap-s4hana-public-cloud.html',
            'servicenow_connector': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/servicenow.html',
            'workday': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/workday.html',
            'extracting_workday_reports': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/extracting-workday-reports.html',
            'custom_connection_python': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/custom-connection-via-python.html',
            'connecting_to_database': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/connecting-to-a-database.html',
            'using_custom_jdbc_driver': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/using-custom-jdbc-driver.html',
            'using_custom_jdbc_string': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/using-custom-jdbc-string.html',
            'setting_up_jdbc_extractor_docker': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/setting-up-jdbc-extractor-on-docker.html',
            'external_password_providers': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/external-password-providers.html',
            'free_text_filter': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/free-text-filter.html',
            'supported_database_connections': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/supported-database-connections.html',
            'additional_database_sources': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/additional-database-sources.html',
            'apache_hive_hadoop': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/apache-hive-apache-hadoop.html',
            'cloudera_impala': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/cloudera-impala.html',
            'databricks': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/databricks.html',
            'google_bigquery': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/google-bigquery.html',
            'microsoft_dynamics_ax': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/microsoft-dynamics-ax.html',
            'microsoft_sql': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/microsoft-sql.html',
            'netsuite': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/netsuite.html',
            'oracle_ebs': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/oracle-ebs.html',
            'snowflake': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/snowflake.html',
            'updating_on_premise_jdbc_extractor': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/updating-the-on-premise-jdbc-extractor.html',
            'extractor_builder': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/extractor-builder.html',
            'extractor_builder_endpoints': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/extractor-builder-endpoints.html',
            'configuring_request_parameters': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/configuring-request-parameters.html',
            'extractor_builder_authentication_methods': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/extractor-builder-authentication-methods.html',
            'microsoft_fabric': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/microsoft-fabric.html',
            'celonis_kafka_connector': 'https://docs.celonis.com/en/platform/help/data-integration/connecting-data-sources/celonis-kafka-connector.html',
            'system_requirements': 'https://docs.celonis.com/en/platform/help/data-integration/on-premise-extractors/system-requirements.html',
            'setting_up_on_premise_extractor': 'https://docs.celonis.com/en/platform/help/data-integration/on-premise-extractors/setting-up.html',
            'configuring_on_premise_extractor': 'https://docs.celonis.com/en/platform/help/data-integration/on-premise-extractors/configuring-an-on-premise-extractor.html',
            'uninstalling_on_premise_extractor': 'https://docs.celonis.com/en/platform/help/data-integration/on-premise-extractors/uninstalling.html',
            'updating_on_premise_extractor': 'https://docs.celonis.com/en/platform/help/data-integration/on-premise-extractors/updating.html',
            'connecting_multiple_teams': 'https://docs.celonis.com/en/platform/help/data-integration/on-premise-extractors/connecting-multiple-teams.html',
            'using_vault_password_provider': 'https://docs.celonis.com/en/platform/help/data-integration/on-premise-extractors/using-vault-as-a-password-provider-to-secure-the-clientsecret.html',
            'proxy_settings_on_prem_clients': 'https://docs.celonis.com/en/platform/help/data-integration/on-premise-extractors/proxy-settings-for-on-prem-clients.html',
            'fixing_certification_path_errors': 'https://docs.celonis.com/en/platform/help/data-integration/on-premise-extractors/fixing-unable-to-find-valid-certification-path-errors.html',
            'continuous_extraction': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/continuous-extraction.html',
            'data_extraction_overview': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/data-extraction-overview.html',
            'connect_sap_data_extraction': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/connect-with-sap-for-data-extraction.html',
            'advanced_configuration': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/advanced-configuration.html',
            'rfc_module': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/rfc-module.html',
            'rfc_module_system_requirements': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/rfc-module-system-requirements.html',
            'installing_rfc_module': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/installing-the-rfc-module.html',
            'updating_rfc_module_extractor': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/updating-rfc-module-and-extractor.html',
            'troubleshooting_rfc_module_installation': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/troubleshooting-rfc-module-installation.html',
            'create_users_sap_connection': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/create-users-for-sap-connection.html',
            'sap_celonis_extraction_role': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/sap-celonis-extraction-role.html',
            'supporting_content': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/supporting-content.html',
            'extracting_encoded_sap_tables': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/extracting-data-from-encoded-sap-tables.html',
            'extracting_aged_data': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/extracting-aged-data.html',
            'extraction_parameterized_views': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/extraction-parameterized-views.html',
            'sap_extraction_client_pi_po_sap_4_6c': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/sap-extraction-client-for-pi-po-and-sap-4-6c.html',
            'one_time_extraction': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/one-time-extraction.html',
            'local_extraction': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/local-extraction.html',
            'extracting_with_odp': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/extracting-with-odp.html',
            'uploading_data_files': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/uploading-data-files.html',
            'allowlisting_domain_names_ip_addresses': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-data-from-sap/allowlisting-domain-names-and-ip-addresses.html',
            'creating_extraction_tasks_visual_editor': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/creating-extraction-tasks-using-the-visual-editor.html',
            'best_practice_extraction_visual_editor': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/best-practice.html',
            'replacing_sap_cluster_tables_bseg': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/replacing-sap-cluster-tables-bseg.html',
            'enabling_partitioned_extractions_large_tables': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/enabling-partitioned-extractions-of-large-tables.html',
            'real_time_extractions_replication_cockpit': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/real-time-extractions-through-replication-cockpit.html',
            'creating_extraction_tasks_editor_ai_assistant': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/creating-extraction-tasks-using-the-extractions-editor-and-ai-assistant.html',
            'extractions_editor_ai_assistant_overview': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/extractions-editor-and-ai-assistant-overview.html',
            'creating_transformation_tasks': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/creating-transformation-tasks.html',
            'using_vertica_sql_syntax': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/using-vertica-sql-syntax.html',
            'best_practice_transformation': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/best-practice.html',
            'delta_transformations_data_jobs': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/delta-transformations-in-data-jobs.html',
            'creating_data_model_load_tasks': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/creating-data-model-load-tasks.html',
            'creating_data_job_task_templates': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/creating-data-job-task-templates.html',
            'executing_data_jobs': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/executing-data-jobs.html',
            'executing_delta_extractions': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/executing-delta-extractions.html',
            'data_job_execution_api': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/data-job-execution-api.html',
            'scheduling_data_jobs': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/scheduling-the-execution-of-data-jobs.html',
            'replication_cockpit': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/replication-cockpit.html',
            'real_time_connectivity_sap_ecc_s4hana_replication_cockpit': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/real-time-connectivity-to-sap-ecc-and-s4hana-via-the-replication-cockpit.html',
            'design_data_pipeline_replications_vs_data_jobs': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/how-to-design-the-data-pipeline-replications-vs-data-jobs.html',
            'set_up_sap_real_time_extension': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/set-up-sap-real-time-extension.html',
            'setup_replication_cockpit': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/setup-replication-cockpit.html',
            'data_pool_architecture_replication_cockpit': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/data-pool-architecture-with-the-replication-cockpit.html',
            'create_data_connection_replication_cockpit': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/create-a-data-connection-that-supports-the-replication-cockpit.html',
            'start_initialization_replication': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/start-an-initialization-or-replication.html',
            'set_up_replication_calendar': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/set-up-a-replication-calendar.html',
            'define_degradation_thresholds_subscribe_alerts': 'https://docs.celonis.com/en/platform/help/data-integration/extracting-and-transforming-data/define-degradation-thresholds-and-subscribe-to-alerts.html',
            'pql_main': 'https://docs.celonis.com/en/pql---process-query-language.html',
            'pql_functions': 'https://docs.celonis.com/en/pql-function-library.html',
            'pql_editor': 'https://docs.celonis.com/en/using-the-pql-editor.html',
            'community': 'https://community.celonis.com/',
            'developer': 'https://developer.celonis.com/'
        }
    
    def scrape_documentation(self, url: str, max_depth: int = 2) -> List[DocumentChunk]:
        """Scrape documentation from a given URL"""
        chunks = []
        visited = set()
        
        def scrape_page(current_url: str, depth: int = 0) -> List[DocumentChunk]:
            if depth > max_depth or current_url in visited:
                return []
            
            visited.add(current_url)
            page_chunks = []
            
            try:
                response = self.session.get(current_url, timeout=10)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract title
                title = soup.find('title')
                title_text = title.get_text().strip() if title else "Untitled"
                
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()
                
                # Extract main content
                content_selectors = [
                    'main', 'article', '.content', '.documentation', 
                    '.markdown-body', '.wiki-content'
                ]
                
                main_content = None
                for selector in content_selectors:
                    main_content = soup.select_one(selector)
                    if main_content:
                        break
                
                if not main_content:
                    main_content = soup.find('body')
                
                if main_content:
                    # Extract text content in chunks
                    sections = self._extract_sections(main_content, title_text, current_url)
                    page_chunks.extend(sections)
                
                # Find related links for deeper scraping
                if depth < max_depth:
                    links = soup.find_all('a', href=True)
                    for link in links[:10]:  # Limit to prevent infinite scraping
                        href = link['href']
                        if self._is_relevant_link(href, current_url):
                            full_url = urljoin(current_url, href)
                            page_chunks.extend(scrape_page(full_url, depth + 1))
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error scraping {current_url}: {str(e)}")
            
            return page_chunks
        
        return scrape_page(url)
    
    def _extract_sections(self, content, title: str, url: str) -> List[DocumentChunk]:
        """Extract sections from HTML content"""
        chunks = []
        
        # Find all headings and their content
        headings = content.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        
        for i, heading in enumerate(headings):
            section_title = heading.get_text().strip()
            
            # Get content until next heading
            content_parts = []
            current = heading.next_sibling
            
            while current:
                if hasattr(current, 'name') and current.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    break
                
                if hasattr(current, 'get_text'):
                    text = current.get_text().strip()
                    if text:
                        content_parts.append(text)
                
                current = current.next_sibling
            
            if content_parts:
                section_content = '\n'.join(content_parts)
                
                # Create chunks for large sections
                chunk_size = 1000
                if len(section_content) > chunk_size:
                    for j, chunk in enumerate(self._split_text(section_content, chunk_size)):
                        chunk_id = hashlib.md5(f"{url}_{section_title}_{j}".encode()).hexdigest()
                        chunks.append(DocumentChunk(
                            content=chunk,
                            url=url,
                            title=title,
                            section=f"{section_title} (Part {j+1})",
                            chunk_id=chunk_id,
                            timestamp=datetime.now()
                        ))
                else:
                    chunk_id = hashlib.md5(f"{url}_{section_title}".encode()).hexdigest()
                    chunks.append(DocumentChunk(
                        content=section_content,
                        url=url,
                        title=title,
                        section=section_title,
                        chunk_id=chunk_id,
                        timestamp=datetime.now()
                    ))
        
        return chunks
    
    def _split_text(self, text: str, chunk_size: int) -> List[str]:
        """Split text into chunks of specified size"""
        words = text.split()
        chunks = []
        current_chunk = []
        current_length = 0
        
        for word in words:
            if current_length + len(word) + 1 > chunk_size and current_chunk:
                chunks.append(' '.join(current_chunk))
                current_chunk = [word]
                current_length = len(word)
            else:
                current_chunk.append(word)
                current_length += len(word) + 1
        
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks
    
    def _is_relevant_link(self, href: str, base_url: str) -> bool:
        """Determine if a link is relevant for PQL documentation"""
        if not href or href.startswith('#'):
            return False
        
        # Check for PQL-related keywords
        pql_keywords = ['pql', 'process', 'query', 'language', 'function', 'operator']
        href_lower = href.lower()
        
        for keyword in pql_keywords:
            if keyword in href_lower:
                return True
        
        # Check if it's a Celonis documentation link
        if 'docs.celonis.com' in href or 'community.celonis.com' in href:
            return True
        
        return False

class VectorStore:
    """Vector store for document embeddings using FAISS"""
    
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2'):
        self.model = SentenceTransformer(model_name)
        self.index = None
        self.documents = []
        self.embeddings = None
    
    def add_documents(self, chunks: List[DocumentChunk]):
        """Add document chunks to the vector store"""
        if not chunks:
            return
        
        # Extract text content
        texts = [chunk.content for chunk in chunks]
        
        # Generate embeddings
        new_embeddings = self.model.encode(texts)
        
        if self.embeddings is None:
            self.embeddings = new_embeddings
            self.documents = chunks
        else:
            self.embeddings = np.vstack([self.embeddings, new_embeddings])
            self.documents.extend(chunks)
        
        # Build FAISS index
        self.index = faiss.IndexFlatIP(self.embeddings.shape[1])
        self.index.add(self.embeddings.astype(np.float32))
    
    def search(self, query: str, k: int = 5) -> List[Tuple[DocumentChunk, float]]:
        """Search for similar documents"""
        if self.index is None:
            return []
        
        query_embedding = self.model.encode([query])
        scores, indices = self.index.search(query_embedding.astype(np.float32), k)
        
        results = []
        for score, idx in zip(scores[0], indices[0]):
            if idx < len(self.documents):
                results.append((self.documents[idx], float(score)))
        
        return results
    
    def save(self, filepath: str):
        """Save vector store to file"""
        data = {
            'embeddings': self.embeddings,
            'documents': self.documents
        }
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
    
    def load(self, filepath: str):
        """Load vector store from file"""
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        
        self.embeddings = data['embeddings']
        self.documents = data['documents']
        
        if self.embeddings is not None:
            self.index = faiss.IndexFlatIP(self.embeddings.shape[1])
            self.index.add(self.embeddings.astype(np.float32))

class PQLAgent:
    """AI Agent for answering PQL questions"""
    
    def __init__(self, vector_store: VectorStore, openai_api_key: str = None):
        self.vector_store = vector_store
        self.openai_api_key = openai_api_key
        if openai_api_key:
            openai.api_key = openai_api_key
    
    def answer_question(self, question: str, max_context_length: int = 3000) -> Dict:
        """Answer a PQL question using RAG"""
        # Search for relevant documents
        relevant_docs = self.vector_store.search(question, k=5)
        
        if not relevant_docs:
            return {
                'answer': "I couldn't find relevant information in the Celonis documentation. Please try rephrasing your question.",
                'sources': [],
                'confidence': 0.0
            }
        
        # Prepare context
        context_parts = []
        sources = []
        
        for doc, score in relevant_docs:
            if len('\n'.join(context_parts)) < max_context_length:
                context_parts.append(f"Source: {doc.title} - {doc.section}\n{doc.content}")
                sources.append({
                    'title': doc.title,
                    'section': doc.section,
                    'url': doc.url,
                    'score': score
                })
        
        context = '\n\n---\n\n'.join(context_parts)
        
        # Generate answer
        if self.openai_api_key:
            answer = self._generate_answer_with_openai(question, context)
        else:
            answer = self._generate_answer_simple(question, context, relevant_docs)
        
        return {
            'answer': answer,
            'sources': sources,
            'confidence': max([score for _, score in relevant_docs]) if relevant_docs else 0.0
        }
    
    def _generate_answer_with_openai(self, question: str, context: str) -> str:
        """Generate answer using OpenAI API"""
        try:
            # Updated to use the new OpenAI client
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_api_key)
            
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system",
                        "content": """You are a Celonis PQL (Process Query Language) expert. 
                        Answer questions about PQL based on the provided documentation context. 
                        Be precise, provide code examples when relevant, and explain concepts clearly.
                        If the context doesn't contain enough information, say so."""
                    },
                    {
                        "role": "user",
                        "content": f"Context from Celonis documentation:\n{context}\n\nQuestion: {question}"
                    }
                ],
                max_tokens=500,
                temperature=0.3
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"OpenAI API error: {str(e)}")
            return self._generate_answer_simple(question, context, [])
    
    def _generate_answer_simple(self, question: str, context: str, relevant_docs: List) -> str:
        """Generate a simple answer without OpenAI API"""
        # Extract key information from the most relevant document
        if relevant_docs:
            best_doc, score = relevant_docs[0]
            
            # Look for code examples or function definitions
            content = best_doc.content
            
            # Simple pattern matching for PQL functions
            function_pattern = r'([A-Z_]+)\s*\([^)]*\)'
            functions = re.findall(function_pattern, content)
            
            answer_parts = [f"Based on the Celonis documentation from '{best_doc.title}':"]
            
            if functions:
                answer_parts.append(f"Relevant PQL functions: {', '.join(functions[:3])}")
            
            # Extract first few sentences as summary
            sentences = content.split('.')[:3]
            summary = '. '.join(sentences) + '.'
            answer_parts.append(summary)
            
            return '\n\n'.join(answer_parts)
        
        return "I found some relevant information, but couldn't generate a specific answer. Please check the sources below."

def main():
    st.set_page_config(
        page_title="Celonis PQL AI Agent",
        page_icon="🔍",
        layout="wide"
    )
    
    st.title("🔍 Celonis PQL AI Agent")
    st.markdown("*Ask questions about Celonis Process Query Language (PQL)*")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        
        # OpenAI API Key
        openai_key = st.text_input("OpenAI API Key (optional)", type="password")
        
        # Data source refresh
        if st.button("Refresh Documentation"):
            with st.spinner("Scraping Celonis documentation..."):
                refresh_documentation()
    
    # Initialize components
    vector_store = initialize_vector_store()
    agent = PQLAgent(vector_store, openai_key if openai_key else None)
    
    # Main interface
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("Ask a PQL Question")
        
        # Sample questions
        sample_questions = [
            "How do I calculate case duration in PQL?",
            "What is the difference between CASE_WHEN and CASE statement?",
            "How to use VARIANT function in PQL?",
            "What are the available aggregation functions in PQL?",
            "How to filter activities in PQL queries?"
        ]
        
        selected_sample = st.selectbox("Sample questions:", [""] + sample_questions)
        
        # User input
        user_question = st.text_area(
            "Your PQL question:",
            value=selected_sample if selected_sample else "",
            height=100,
            placeholder="e.g., How do I calculate the average case duration using PQL?"
        )
        
        if st.button("Get Answer", type="primary"):
            if user_question.strip():
                with st.spinner("Searching documentation and generating answer..."):
                    result = agent.answer_question(user_question)
                
                # Display answer
                st.subheader("Answer")
                st.write(result['answer'])
                
                # Display confidence
                confidence = result['confidence']
                st.metric("Confidence", f"{confidence:.2%}")
                
                # Display sources
                if result['sources']:
                    st.subheader("Sources")
                    for i, source in enumerate(result['sources'], 1):
                        with st.expander(f"Source {i}: {source['title']} - {source['section']}"):
                            st.write(f"**URL:** {source['url']}")
                            st.write(f"**Relevance Score:** {source['score']:.3f}")
            else:
                st.warning("Please enter a question.")
    
    with col2:
        st.header("Documentation Status")
        
        # Display vector store statistics
        if vector_store.documents:
            st.metric("Documents in Knowledge Base", len(vector_store.documents))
            
            # Show recent documents
            st.subheader("Recent Sources")
            recent_docs = sorted(vector_store.documents, key=lambda x: x.timestamp, reverse=True)[:5]
            
            for doc in recent_docs:
                st.write(f"📄 **{doc.title}**")
                st.write(f"   {doc.section}")
                st.write(f"   *Added: {doc.timestamp.strftime('%Y-%m-%d %H:%M')}*")
                st.write("---")
        else:
            st.info("No documents loaded. Use 'Refresh Documentation' to load data.")
        
        # PQL Quick Reference
        st.subheader("PQL Quick Reference")
        with st.expander("Common PQL Functions"):
            st.code("""
-- Case duration
DATEDIFF(dd, "Table"."Start", "Table"."End")

-- Activity occurrence
CASE WHEN "Table"."Activity" = 'Create Order' 
     THEN 1 ELSE 0 END

-- Variant analysis
VARIANT("Table"."Activity")

-- Filtering
FILTER "Table"."Status" = 'Completed'

-- Aggregations
COUNT("Table"."CaseID")
AVG("Table"."Duration")
SUM("Table"."Amount")
            """)

@st.cache_data
def initialize_vector_store():
    """Initialize or load the vector store"""
    vector_store = VectorStore()
    
    # Try to load existing data
    if os.path.exists("pql_knowledge_base.pkl"):
        try:
            vector_store.load("pql_knowledge_base.pkl")
            logger.info("Loaded existing knowledge base")
        except Exception as e:
            logger.error(f"Error loading knowledge base: {str(e)}")
            # Create new one if loading fails
            create_initial_knowledge_base(vector_store)
    else:
        create_initial_knowledge_base(vector_store)
    
    return vector_store

def create_initial_knowledge_base(vector_store: VectorStore):
    """Create initial knowledge base with sample PQL content"""
    # Sample PQL documentation chunks
    sample_chunks = [
        DocumentChunk(
            content="PQL (Process Query Language) is a domain-specific query language developed by Celonis for process mining. It allows you to translate your process-related questions into executable queries.",
            url="https://docs.celonis.com/en/pql---process-query-language.html",
            title="PQL - Process Query Language",
            section="Introduction",
            chunk_id="intro_1",
            timestamp=datetime.now()
        ),
        DocumentChunk(
            content="CASE_WHEN is used for conditional logic in PQL. Syntax: CASE WHEN condition THEN result ELSE alternative END. Example: CASE WHEN \"Activities\".\"Activity\" = 'Create Order' THEN 1 ELSE 0 END",
            url="https://docs.celonis.com/en/pql-function-library.html",
            title="PQL Function Library",
            section="Conditional Functions",
            chunk_id="case_when_1",
            timestamp=datetime.now()
        ),
        DocumentChunk(
            content="DATEDIFF calculates the difference between two dates. Syntax: DATEDIFF(unit, start_date, end_date). Units include: dd (days), hh (hours), mm (minutes), ss (seconds).",
            url="https://docs.celonis.com/en/pql-function-library.html",
            title="PQL Function Library",
            section="Date Functions",
            chunk_id="datediff_1",
            timestamp=datetime.now()
        ),
        DocumentChunk(
            content="VARIANT function returns the variant of a case, which is the sequence of activities. Syntax: VARIANT(\"Table\".\"Activity_Column\"). This is useful for process variant analysis.",
            url="https://docs.celonis.com/en/pql-function-library.html",
            title="PQL Function Library",
            section="Process Functions",
            chunk_id="variant_1",
            timestamp=datetime.now()
        ),
        DocumentChunk(
            content="COUNT function counts the number of rows. COUNT(column) counts non-null values, COUNT(*) counts all rows. COUNT(DISTINCT column) counts unique values.",
            url="https://docs.celonis.com/en/pql-function-library.html",
            title="PQL Function Library",
            section="Aggregation Functions",
            chunk_id="count_1",
            timestamp=datetime.now()
        )
    ]
    
    vector_store.add_documents(sample_chunks)
    
    # Save the initial knowledge base
    try:
        vector_store.save("pql_knowledge_base.pkl")
        logger.info("Created and saved initial knowledge base")
    except Exception as e:
        logger.error(f"Error saving knowledge base: {str(e)}")

def refresh_documentation():
    """Refresh documentation from Celonis sources"""
    scraper = CelonisDocScraper()
    vector_store = VectorStore()
    
    all_chunks = []
    
    # Scrape main documentation URLs
    for name, url in scraper.doc_urls.items():
        st.write(f"Scraping {name}...")
        try:
            chunks = scraper.scrape_documentation(url, max_depth=1)
            all_chunks.extend(chunks)
            st.write(f"Found {len(chunks)} chunks from {name}")
        except Exception as e:
            st.error(f"Error scraping {name}: {str(e)}")
    
    if all_chunks:
        vector_store.add_documents(all_chunks)
        vector_store.save("pql_knowledge_base.pkl")
        st.success(f"Successfully updated knowledge base with {len(all_chunks)} document chunks!")
        st.rerun()  # Updated from st.experimental_rerun()
    else:
        st.warning("No new content was scraped.")

if __name__ == "__main__":
    main()
