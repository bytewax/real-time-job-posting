import json
import logging
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from tqdm import tqdm

from haystack import component
from haystack.dataclasses import ByteStream
from haystack.dataclasses import Document

logger = logging.getLogger(__name__)

@component
class JsonToDocument:
    """
    Converts JSON content into Haystack Documents. Specifically looks for the `job_description`
    in each item of the 'data' list within the JSON and uses it as the document content, alongside
    other metadata. This class provides options to flatten JSON and create either a single document
    from the entire table or one document per row.

    Usage example:
    ```python
    from your_module import JsonToDocument  # Replace with actual module name

    converter = JsonToDocument(flatten_field="data", one_doc_per_row=True)
    results = converter.run(sources=["complex_json.json"])
    documents = results["documents"]
    # Each document represents a row if one_doc_per_row is True, with `job_description` as content.
    ```
    """

    def __init__(self, flatten_field: str = None, one_doc_per_row: bool = False, progress_bar: bool = True):
        """
        :param flatten_field: The field in JSON to flatten using json_normalize. Set to None if not needed. Defaults to None.
        :param one_doc_per_row: Whether to create a separate document for each row of the DataFrame. Defaults to False.
        :param progress_bar: Show a progress bar for the conversion. Defaults to True.
        """
        self.flatten_field = flatten_field
        self.one_doc_per_row = one_doc_per_row
        self.progress_bar = progress_bar

    @component.output_types(documents=List[Document])
    def run(self, sources: List[Union[str, Path, ByteStream]]):
        documents = []

        for source in tqdm(sources, desc="Converting JSON files to Documents", disable=not self.progress_bar):
            try:
                file_content = self._extract_content(source)
                json_data = json.loads(file_content)

                if 'data' in json_data and isinstance(json_data['data'], list):
                    for item in json_data['data']:
                        # Optionally flatten the item if a flatten_field is provided
                        if self.flatten_field and self.flatten_field in item:
                            item = pd.json_normalize(item[self.flatten_field]).to_dict(orient='records')[0]

                        # Directly use `job_description` as the content
                        content = item.get('job_description', None)
                        document = Document(content=content, meta=item)
                        documents.append(document)
                else:
                    logger.warning(f"The JSON structure of {source} does not match the expected format.")
            except Exception as e:
                logger.warning("Failed to process %s. Error: %s", source, e)

        return {"documents": documents}

    def _extract_content(self, source: Union[str, Path, ByteStream]) -> str:
        """
        Extracts content from the given data source.
        :param source: The data source to extract content from.
        :return: The extracted content.
        """
        if isinstance(source, (str, Path)):
            with open(source, 'r', encoding='utf-8') as file:
                return file.read()
        elif isinstance(source, ByteStream):
            return source.data.decode('utf-8')
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")
