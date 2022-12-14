"""Library for creating custom document processors."""
import collections
import datetime
import json
import mimetypes
import os
import time
from typing import Any, Dict, Mapping, Sequence
import urllib

from model_factory import http_client
from google.api_core.retry import Retry
from google.cloud import storage
from joblib import delayed
from joblib import Parallel
from tqdm import tqdm

_THREADS_COUNT = 30
_MAX_BATCH_SIZE = 50
_DATASET_FOLDER = 'datasets'
_ANNOTATION_FOLDER = 'annotations'
_STATE_FOLDER = 'states'
_LRO_FOLDER = 'lros'
_LABELING_LRO_EXT = '.labeling-lro.txt'
_SCHEMA_FILE = 'schema.json'
_DUMMY_DATASET_FOLDER = 'dummy'
_SNAPSHOT_FOLDER = 'snapshots'
_VALID_DOCUMENT_MIME_TYPES = frozenset(
    ['application/pdf', 'image/png', 'image/jpeg', 'image/gif', 'image/tiff'])


def _parse_gs_uri(uri: str):
    parsed_uri = urllib.parse.urlsplit(uri)
    return parsed_uri.netloc, parsed_uri.path[1:]


def _shift_text_segment(text_segment: Dict[str, Any], offset: int):
    text_segment['startIndex'] = str(offset + (
        int(text_segment['startIndex']) if 'startIndex' in text_segment else 0))
    text_segment['endIndex'] = str(offset + (
        int(text_segment['endIndex']) if 'endIndex' in text_segment else 0))


def _shift_layout(layout: Dict[str, Any], offset: int):
    if 'textAnchor' not in layout or 'textSegments' not in layout['textAnchor']:
        return
    for text_segment in layout['textAnchor']['textSegments']:
        _shift_text_segment(text_segment, offset)


def _shift_table_row(table_row: Dict[str, Any], offset: int):
    if 'cells' not in table_row:
        return
    for cell in table_row['cells']:
        _shift_layout(cell['layout'], offset)


def _get_shard_index(document: Dict[str, Any]) -> int:
    return int(document['shardInfo']
               ['shardIndex']) if 'shardIndex' in document['shardInfo'] else 0


def _merge_document_proto(original: Dict[str, Any], shard: Dict[str, Any]):
    """Merges document shards into one document."""
    original['text'] = original.get('text', '') + shard.get('text', '')
    if 'pages' not in shard:
        return
    original['pages'] = original.get('pages', [])
    offset = int(shard['shardInfo'].get('textOffset', 0))
    page_number = original['pages']
    print("page_number")
    print(page_number)
    for page in shard['pages']:
        _shift_layout(page['layout'], offset)
        if 'blocks' in page:
            for block in page['blocks']:
                _shift_layout(block['layout'], offset)
        if 'paragraphs' in page:
            for paragraph in page['paragraphs']:
                _shift_layout(paragraph['layout'], offset)
        if 'lines' in page:
            for line in page['lines']:
                _shift_layout(line['layout'], offset)
        if 'tokens' in page:
            for token in page['tokens']:
                _shift_layout(token['layout'], offset)
        if 'visualElements' in page:
            for visual_element in page['visualElements']:
                _shift_layout(visual_element['layout'], offset)
        if 'tables' in page:
            for table in page['tables']:
                _shift_layout(table['layout'], offset)
                if 'headerRows' in table:
                    for table_row in table['headerRows']:
                        _shift_table_row(table_row, offset)
                if 'bodyRows' in table:
                    for table_row in table['bodyRows']:
                        _shift_table_row(table_row, offset)
        if 'formFields' in page:
            for form_field in page['formFields']:
                _shift_layout(form_field['fieldName'], offset)
                _shift_layout(form_field['fieldValue'], offset)
        page_number = page_number + 1
        page['pageNumber'] = page_number
        original['pages'].append(page)


@Retry()
def _blob_exists(blob: storage.Blob) -> bool:
    return blob and blob.exists()


@Retry()
def _download(blob: storage.Blob) -> bytes:
    return blob.download_as_string()


@Retry()
def _upload(blob: storage.Blob, content: bytes):
    blob.upload_from_string(content)


class BaseProcessor:
    """Base class for all custom document processors."""

    def __init__(self,
                 workspace: str,
                 processor_type: str,
                 endpoint: str = 'https://us-documentai.googleapis.com/'):
        """Initiates a base processor.

        Args:
          workspace: Path to a GCS folder to be used for storing configuration
            files.
          processor_type: Type of the processor to build. For example,
            "CUSTOM_EXTRACTION_PROCESSOR".
          endpoint:  DocAI endpoint that we want to use.
        """
        self._workspace = workspace
        self._gcs_client = storage.Client()
        self._dai_client = http_client.DocumentAIClient(endpoint=endpoint)
        bucket, path = _parse_gs_uri(workspace)
        config_path = os.path.join(path, 'config.json')
        config_blob = self._gcs_client.bucket(bucket).get_blob(config_path)
        self._processor = ''
        self._import_processor = ''
        if _blob_exists(config_blob):
            print('Loading existing processor...')
            config = json.loads(_download(config_blob))
            self._processor = config['processor']
            self._import_processor = config['import_processor']
            loaded_processor_type = config['processor_type']
            if processor_type != loaded_processor_type:
                raise ValueError(
                    f'Expected {loaded_processor_type} processor type, found {processor_type} instead.'
                )
        if not self._processor:
            print('Creating new processor...')
            self._processor = self._dai_client.create_processor(
                processor_type, f'Processor ({path})')
        if not self._import_processor:
            self._import_processor = self._create_import_processor()
        config = {
            'processor': self._processor,
            'import_processor': self._import_processor,
            'processor_type': processor_type
        }
        self._gcs_client.bucket(bucket).blob(config_path).upload_from_string(
            json.dumps(config))
        print(
            f'Processor type: {processor_type}\nProcessor name: {self._processor}.\nDisplay name: Processor ({path}).\nDone.'
        )

    def processor_name(self) -> str:
        """Returns processor name."""
        return self._processor

    def _create_import_processor(self) -> str:
        """Creates a new import processor.

        Returns:
          Name of the created import processor.
        """
        # To be implemented by subclasses.
        return ''

    def _get_merged_document(self, lro_name: str,
                             index: str) -> Mapping[str, Any]:
        """Reads shards produced by processor and merges them into one document."""
        lro_id = lro_name.split('/')[-1]
        bucket, prefix = _parse_gs_uri(self._workspace)
        shards = []
        for blob in self._gcs_client.list_blobs(
                bucket, prefix=os.path.join(prefix, _LRO_FOLDER, lro_id, index)):
            shard = json.loads(_download(blob))
            shards.append(shard)
        sorted(shards, key=_get_shard_index)
        if not shards:
            raise FileNotFoundError('Found 0 shards.')
        if len(shards) == 1:
            return shards[0]
        merged_document = None
        for shard in shards:
            if not merged_document:
                merged_document = shard
            else:
                _merge_document_proto(merged_document, shard)
        return merged_document

    def import_documents(self, path: str, dataset_name: str):
        """Imports documents.

    Args:
      path: Path to the folder storing all the documents to import.
      dataset_name: Name of the dataset to import document to. For example,
        "training".
    """
        documents_to_process = set()
        imported_documents = set()
        processing_documents = set()
        lros_to_monitor = collections.defaultdict(dict)
        documents = set()

        bucket, prefix = _parse_gs_uri(path)
        workspace_bucket, workspace_path = _parse_gs_uri(self._workspace)
        output_path = os.path.join(
            workspace_path, _DATASET_FOLDER, dataset_name)
        state_path = os.path.join(workspace_path, _STATE_FOLDER)

        # Find all documents to process
        for blob in self._gcs_client.list_blobs(bucket, prefix=prefix):
            uri = os.path.join('gs://', bucket, blob.name)
            mime_type, _ = mimetypes.guess_type(uri)
            if mime_type and mime_type in _VALID_DOCUMENT_MIME_TYPES:
                documents.add(os.path.basename(blob.name))

        # Find all imported JSON files
        for blob in self._gcs_client.list_blobs(
                workspace_bucket, prefix=output_path):
            if blob.name.endswith('.json'):
                imported_documents.add(os.path.basename(blob.name)[:-5])

        # Find all LRO state files
        for blob in self._gcs_client.list_blobs(
                workspace_bucket, prefix=state_path):
            if blob.name.endswith('.lro.json'):
                document_name = os.path.basename(blob.name)[:-len('.lro.json')]
                if document_name in documents:
                    processing_documents.add(document_name)

        # Filter out imported documents
        processing_documents = processing_documents - imported_documents
        documents_to_process = documents - imported_documents - processing_documents
        print(
            f'Found {len(documents) - len(imported_documents)} new documents to import.'
        )

        # Load LRO states
        def load_lro_state(document_name):
            lro_state_path = os.path.join(
                state_path, f'{document_name}.lro.json')
            lro_blob = self._gcs_client.bucket(
                workspace_bucket).blob(lro_state_path)
            lro_state = json.loads(_download(lro_blob))
            lros_to_monitor[lro_state['name']
                            ][lro_state['index']] = document_name

        _ = Parallel(
            n_jobs=_THREADS_COUNT, require='sharedmem')(
                delayed(load_lro_state)(document_name) for document_name in tqdm(
                    processing_documents, desc='Read LRO states'))

        # Create new LROs
        with tqdm(total=len(documents_to_process), desc='Create LROs') as pbar:
            while documents_to_process:
                batch = []
                while len(batch) < _MAX_BATCH_SIZE and documents_to_process:
                    batch.append(documents_to_process.pop())
                document_paths = []
                for doc in batch:
                    document_paths.append(os.path.join(path, doc))
                lro_name = self._dai_client.batch_process_documents(
                    self._import_processor, document_paths,
                    os.path.join(self._workspace, _LRO_FOLDER))
                for index in range(len(batch)):
                    lros_to_monitor[lro_name][str(index)] = batch[index]
                    state_blob = self._gcs_client.bucket(workspace_bucket).blob(
                        os.path.join(state_path, f'{batch[index]}.lro.json'))
                    _upload(state_blob, json.dumps({
                        'name': lro_name,
                        'index': str(index)
                    }))
                    processing_documents.add(batch[index])
                    pbar.update(1)

        # Wait for all LROs to complte
        with tqdm(total=len(lros_to_monitor), desc='Wait for LROs') as pbar:
            pending_lros = set(lros_to_monitor.keys())
            while pending_lros:
                completed_lros = set()
                for lro_name in pending_lros:
                    lro = self._dai_client.get_operation(lro_name)
                    if 'done' in lro:
                        completed_lros.add(lro_name)
                        pbar.update(1)
                        if lro['metadata']['state'] != 'SUCCEEDED':
                            print(lro)
                            return
                        for individual_status in lro['metadata'][
                                'individualProcessStatuses']:
                            if 'status' in individual_status:
                                print(individual_status)
                pending_lros -= completed_lros
                time.sleep(10)

        # Process LRO outputs
        def process_lro(lro_name: str):
            for index, doc in lros_to_monitor[lro_name].items():
                try:
                    merged_document = self._get_merged_document(
                        lro_name, index)
                except FileNotFoundError:
                    print(('Skipping a document because of file not found for document '
                           '%s (LRO: %s, index: %s).') % (doc, lro_name, index))
                    continue
                _upload(
                    self._gcs_client.bucket(workspace_bucket).blob(
                        os.path.join(output_path, f'{doc}.json')),
                    json.dumps(merged_document))

        _ = Parallel(
            n_jobs=_THREADS_COUNT, require='sharedmem')(
                delayed(process_lro)(lro_name)
                for lro_name in tqdm(lros_to_monitor, desc='Process LRO outputs'))

    def update_schema(self, schema: Mapping[str, Any]):
        lro_name = self._dai_client.update_labeling_schema(
            self._processor, schema)
        print('Waiting for LRO to complete...')
        while True:
            if 'done' in self._dai_client.get_operation(lro_name):
                break
            time.sleep(2)
        print('Schema updated.')

    def update_data_labeling_config(self, schema: Mapping[str, Any],
                                    instruction_gcs_uri: str,
                                    labeler_pools: Sequence[str]):
        """Updates data labeling config.

        Args:
          schema: Schema of the documents to label.
          instruction_gcs_uri: Path to the instruction PDF document. Format:
            gs://<bucket_name>/<path_to_pdf>.
          labeler_pools: Pools of labelers that will receive the task of labeling
            documents.

        Raises:
          RuntimeError: if fails to update the config.
        """
        config = {
            'labelingSchema': schema,
            'state': 'ENABLED',
            'instructionGcsUri': instruction_gcs_uri,
            'labelerPools': labeler_pools,
            'labelerSource': 'USER_DEFINED',
            'documentOutputConfig': {
                'gcsOutputConfig': {
                    'gcsUri': os.path.join(self._workspace, 'data_labeling_output')
                }
            },
            'criteria': {
                'type': 'SELF_VALIDATION',
                'documentLevelCriteria': {
                    'confidenceThreshold': 0.7
                }
            },
        }
        lro_name = self._dai_client.update_human_review_config(
            self._processor, config)
        print('Updating data labeling config...')
        lro = self._dai_client.wait_for_lro(lro_name, poll_interval_seconds=2)
        if lro['metadata']['commonMetadata']['state'] != 'SUCCEEDED':
            raise RuntimeError(lro)
        print('Done.')

    def label_dataset(self, dataset_name: str):
        """Labels a dataset.

        HumanReview must be enabled on the processor before labeling a dataset.

        Args:
          dataset_name: Name of the dataset to label. For example, "training".
        """
        workspace_bucket, workspace_path = _parse_gs_uri(self._workspace)
        annotation_path = os.path.join(workspace_path, _ANNOTATION_FOLDER,
                                       dataset_name)
        state_path = os.path.join(workspace_path, _STATE_FOLDER)
        dataset_path = os.path.join(
            workspace_path, _DATASET_FOLDER, dataset_name)
        documents_to_label = set()
        documents_to_monitor = dict()
        labeling_documents = set()
        for blob in self._gcs_client.list_blobs(
                workspace_bucket, prefix=dataset_path):
            if blob.name.endswith('.json'):
                documents_to_label.add(os.path.basename(blob.name)[:-5])
        for blob in self._gcs_client.list_blobs(
                workspace_bucket, prefix=state_path):
            if blob.name.endswith(_LABELING_LRO_EXT):
                document_name = os.path.basename(
                    blob.name)[:-len(_LABELING_LRO_EXT)]
                if document_name in documents_to_label:
                    labeling_documents.add(document_name)
        for blob in self._gcs_client.list_blobs(
                workspace_bucket, prefix=annotation_path):
            if blob.name.endswith('.json'):
                documents_to_label.discard(os.path.basename(blob.name)[:-5])
                labeling_documents.discard(os.path.basename(blob.name)[:-5])

        # Load LRO names
        def load_lro_name(document_name):
            lro_state_path = os.path.join(state_path,
                                          f'{document_name}{_LABELING_LRO_EXT}')
            lro_blob = self._gcs_client.bucket(
                workspace_bucket).blob(lro_state_path)
            lro_name = _download(lro_blob).decode()
            documents_to_monitor[document_name] = lro_name

        _ = Parallel(
            n_jobs=_THREADS_COUNT, require='sharedmem')(
                delayed(load_lro_name)(document_name) for document_name in tqdm(
                    labeling_documents, desc='Read LRO names'))

        documents_to_label = documents_to_label - labeling_documents

        # Create new LROs
        def start_labeling_task(document_name: str):
            blob = self._gcs_client.bucket(workspace_bucket).blob(
                os.path.join(dataset_path, f'{document_name}.json'))
            document = json.loads(_download(blob))
            lro_name = self._dai_client.review_document(
                self._processor, document)
            lro_state_path = os.path.join(state_path,
                                          f'{document_name}{_LABELING_LRO_EXT}')
            lro_blob = self._gcs_client.bucket(
                workspace_bucket).blob(lro_state_path)
            _upload(lro_blob, lro_name.encode())
            documents_to_monitor[document_name] = lro_name

        _ = Parallel(
            n_jobs=_THREADS_COUNT, require='sharedmem')(
                delayed(start_labeling_task)(document_name)
                for document_name in tqdm(documents_to_label, desc='Create LROs'))

        print(
            'Labeling task has been created.\n'
            'Please make sure the task has been assigned to raters / labelers.\n'
            'The program will not proceed until all documents are labeled and processed.'
        )

        # Move labeling results to annotation folder
        def move_annotation(document_name: str):
            lro_id = documents_to_monitor[document_name].split('/')[-1]
            contents = []
            for blob in self._gcs_client.list_blobs(
                    output_bucket, prefix=os.path.join(output_path, lro_id)):
                contents.append(_download(blob))
            if not contents or len(contents) > 1:
                raise RuntimeError(
                    f'Found {len(contents)} annotations files for {document_name}. Expected 1.'
                )
            target_path = os.path.join(
                annotation_path, f'{document_name}.json')
            _upload(
                self._gcs_client.bucket(workspace_bucket).blob(target_path),
                contents[0])

        # Read output location
        human_review_config = self._dai_client.get_human_review_config(
            self._processor)
        output_location = human_review_config['outputGcsUri']
        output_bucket, output_path = _parse_gs_uri(output_location)

        # Wait for LROs to complete
        with tqdm(total=len(documents_to_monitor), desc='Wait for LROs') as pbar:
            pending_documents = set(documents_to_monitor.keys())
            while pending_documents:
                completed_documents = set()
                for document_name in pending_documents:
                    if 'done' in self._dai_client.get_operation(
                            documents_to_monitor[document_name]):
                        completed_documents.add(document_name)
                        move_annotation(document_name)
                        pbar.update(1)
                pending_documents -= completed_documents
                time.sleep(10)

    def _train_internal(self, display_name: str, schema: Mapping[str, Any],
                        training_data_path: str, test_data_path: str):
        return self._dai_client.train_processor_version(self._processor,
                                                        display_name, schema,
                                                        training_data_path,
                                                        test_data_path)

    def _snapshot_annotations(self, snapshot_name: str, dataset_name: str) -> str:
        """Snapshot annotations of a dataset for processor training."""
        snapshot_path = os.path.join(self._workspace, _SNAPSHOT_FOLDER,
                                     snapshot_name, dataset_name)
        print(f'Creating snapshot: {snapshot_path}')
        workspace_bucket, workspace_path = _parse_gs_uri(self._workspace)
        annotation_file_names = []
        for blob in self._gcs_client.list_blobs(
                workspace_bucket,
                prefix=os.path.join(workspace_path, _ANNOTATION_FOLDER, dataset_name)):
            annotation_file_names.append(os.path.basename(blob.name))
        print(f'Found {len(annotation_file_names)} annotations.')
        valid_annotations = []

        def move_annotation_if_valid(file_name: str):
            blob = self._gcs_client.bucket(workspace_bucket).blob(
                os.path.join(workspace_path, _ANNOTATION_FOLDER, dataset_name,
                             file_name))
            if blob is None or not blob:
                return
            annotation_bytes = _download(blob)
            annotation = json.loads(annotation_bytes)
            if 'revisions' in annotation:
                for revision in annotation['revisions']:
                    if 'humanReview' in revision and revision['humanReview'][
                            'state'] == 'rejected':
                        return
            output_blob = self._gcs_client.bucket(workspace_bucket).blob(
                os.path.join(workspace_path, _SNAPSHOT_FOLDER, snapshot_name,
                             dataset_name, file_name))
            _upload(output_blob, annotation_bytes)
            valid_annotations.append(file_name)

        _ = Parallel(
            n_jobs=_THREADS_COUNT, require='sharedmem')(
                delayed(move_annotation_if_valid)(file_name) for file_name in tqdm(
                    annotation_file_names, desc='Snapshot annotations'))
        print(
            f'Created snapshot {dataset_name} with {len(valid_annotations)} valid annotations.'
        )
        return snapshot_path

    def train(self,
              training_dataset_name: str,
              test_dataset_name: str,
              display_name: str = '') -> str:
        """Trains a new processor version.

        Args:
          training_dataset_name: Name of the training set. For example, "training".
          test_dataset_name: Name of the test set. For example, "test".
          display_name: Name of the processor version. If empty, a datetime in ISO
            format will be used.

        Returns:
          Trained processor version name.

        Raises:
          RuntimeError: when fails to train.
        """
        if not display_name:
            display_name = datetime.datetime.now().isoformat()

        workspace_bucket, workspace_path = _parse_gs_uri(self._workspace)
        lro_name_path = os.path.join(workspace_path, _SNAPSHOT_FOLDER, display_name,
                                     'lro.txt')
        lro_name_blob = self._gcs_client.bucket(workspace_bucket).get_blob(
            lro_name_path)
        if _blob_exists(lro_name_blob):
            print('Loading existing training LRO...')
            lro_name = _download(lro_name_blob).decode()
        else:
            human_review_config = self._dai_client.get_human_review_config(
                self._processor)
            training_data_path = self._snapshot_annotations(display_name,
                                                            training_dataset_name)
            test_data_path = self._snapshot_annotations(display_name,
                                                        test_dataset_name)
            print(f'Creating snapshot ({display_name})...')
            lro_name = self._train_internal(display_name,
                                            human_review_config['labelingSchema'],
                                            training_data_path, test_data_path)
            # Persist training LRO
            self._gcs_client.bucket(workspace_bucket).blob(
                lro_name_path).upload_from_string(lro_name)
            print(
                f'Created LRO {lro_name} for training Processor {display_name}.')

        lro = self._dai_client.wait_for_lro(lro_name)
        if lro['metadata']['commonMetadata']['state'] != 'SUCCEEDED':
            raise RuntimeError(lro)
        print('Training completed.')
        return lro['response']['processorVersion']


class SampleProcessor(BaseProcessor):
    """Processor for fast verification purposes."""

    def __init__(self, workspace: str):
        super().__init__(workspace, 'SAMPLE_PROCESSOR')

    def _create_import_processor(self) -> str:
        return self._processor


class ExtractionProcessor(BaseProcessor):
    """Processor for extracting entities."""

    def __init__(self,
                 workspace: str,
                 endpoint: str = 'https://us-documentai.googleapis.com/'):
        super().__init__(workspace, 'CUSTOM_EXTRACTION_PROCESSOR', endpoint)
        self.active_algorithms = []
        self.processing_options = {}

    def _create_import_processor(self) -> str:
        return self._dai_client.create_processor(
            'FORM_PARSER_PROCESSOR', f'Import Processor ({self._processor})')

    def _train_internal(self, display_name: str, schema: Mapping[str, Any],
                        training_data_path: str, test_data_path: str) -> str:
        extraction_options = {'activeAlgorithms': self.active_algorithms}
        if self.processing_options:
            extraction_options['options'] = self.processing_options

        return self._dai_client.train_processor_version(
            self._processor,
            display_name,
            schema,
            training_data_path,
            test_data_path,
            extraction_options=extraction_options)


class ClassificationProcessor(BaseProcessor):
    """Processor for classifying documents."""

    def __init__(self,
                 workspace: str,
                 endpoint: str = 'https://us-documentai.googleapis.com/'):
        super().__init__(workspace, 'CUSTOM_CLASSIFICATION_PROCESSOR', endpoint)

    def _create_import_processor(self) -> str:
        return self._dai_client.create_processor(
            'CUSTOM_CLASSIFICATION_PROCESSOR',
            f'Import Processor ({self._processor})')


class SplittingProcessor(BaseProcessor):
    """Processor for classifying documents."""

    def __init__(self,
                 workspace: str,
                 endpoint: str = 'https://us-documentai.googleapis.com/'):
        super().__init__(workspace, 'CUSTOM_SPLITTING_PROCESSOR', endpoint)

    def _create_import_processor(self) -> str:
        return self._dai_client.create_processor(
            'CUSTOM_SPLITTING_PROCESSOR', f'Import Processor ({self._processor})')


class SpecializedProcessor(BaseProcessor):
    """Google-prebuilt processor."""

    # pylint: disable=super-init-not-called
    def __init__(self,
                 workspace: str,
                 endpoint: str = 'https://us-documentai.googleapis.com/',
                 processor_name: str = ''):
        self._workspace = workspace
        self._gcs_client = storage.Client()
        self._dai_client = http_client.DocumentAIClient(endpoint=endpoint)
        self._processor = processor_name
        self._import_processor = processor_name
