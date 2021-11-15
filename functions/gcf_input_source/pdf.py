
import io
import os
from typing import List, Union

from PyPDF2.pdf import PdfFileReader, PdfFileWriter

import gcs
from function_variables import FunctionVariables

var = FunctionVariables()

def write_single_page_pdf(inputpdf: PdfFileReader, num_page: int, filename: str) -> str:
    """
    Function that exports a page of an input pdf into a local file of a single page

    Parameters
    ----------
    inputpdf: PdfFileReader
        pdf file reader of raw input pdf
    num_page: int
        page number of input pdf to export
    filename: str
        filename of raw input

    Returns
    -------
    output_paths: list
        list of local paths where mono page pdf files are stored
    """
    outputpdf = PdfFileWriter()
    outputpdf.addPage(inputpdf.getPage(num_page))
    filepath_out = os.path.join(
        var.PDF_FOLDER, filename.replace(".pdf", f"_page_{num_page}.pdf")
    )
    with open(filepath_out, "wb") as f:
        outputpdf.write(f)
    return filepath_out


def split_one_pdf_pages(gcs_uri_in: str) -> Union[List[str], bool]:
    """
    Function that splits one pdf of N pages into N pdfs of 1 page

    Parameters
    ----------
    gcs_uri_in: str
        gcs uri of pdf file to split

    Returns
    -------
    output_paths: list
        list of local paths where mono page pdf files are stored
    success: bool
        True if splitting was a success, False else
    """
    if not gcs_uri_in.endswith("pdf"):
        print(f"File in {gcs_uri_in} is not a pdf file")
        local_filepath = os.path.join(var.ERROR_FOLDER, os.path.basename(gcs_uri_in))
        gcs.get_file(gcs_uri_in, local_filepath)
        return local_filepath, False
    filename = os.path.basename(gcs_uri_in)
    raw_data = gcs.get_data(gcs_uri_in)
    inputpdf = PdfFileReader(io.BytesIO(raw_data), strict=False)
    output_paths = []
    for num_page in range(inputpdf.numPages):
        filepath_out = write_single_page_pdf(inputpdf, num_page, filename)
        output_paths.append(filepath_out)
    return output_paths, True





def split_pdf(inputpdf, start_page, end_page, uri, gcs_output_uri: str, gcs_output_uri_prefix: str):

    with io.StringIO() as stream:

        print("numPages: {}".format(inputpdf.numPages))

        output = PdfFileWriter()
        for i in range(start_page, end_page+1):
            output.addPage(inputpdf.getPage(i))
            print("add page {}".format(i))

        file = uri.path[:-4] + \
            "-page-{}-to-{}.pdf".format(start_page, end_page)
        print(file)

        buf = io.BytesIO()
        output.write(buf)
        data = buf.getvalue()
        outputBlob = gcs_output_uri_prefix + file
        print("Start write:"+outputBlob)
        #bucket = storage_client.get_bucket(urlparse(gcs_output_uri).hostname)

        gcs.write_bytes(data, outputBlob,'application/pdf')
        #bucket.blob(outputBlob).upload_from_string(            data, content_type='application/pdf')

        stream.truncate(0)

    print("split finish")


def pages_split(text: str, document: dict, uri, gcs_output_uri: str, gcs_output_uri_prefix: str):
    """
    Document AI identifies possible page splits
    in document. This function converts page splits
    to text snippets and prints it.    
    """
    for i, entity in enumerate(document.entities):
        confidence = entity.confidence
        text_entity = ''
        for segment in entity.text_anchor.text_segments:
            start = segment.start_index
            end = segment.end_index
            text_entity += text[start:end]

        pages = [p.page for p in entity.page_anchor.page_refs]
        print(f"*** Entity number: {i}, Split Confidence: {confidence} ***")
        print(
            f"*** Pages numbers: {[p for p in pages]} ***\nText snippet: {text_entity[:100]}")
        print("type: " + entity.type_)
        start_page = pages[0]
        end_page = pages[len(pages)-1]
        print(start_page)
        print(end_page)

        bytes, _ = gcs.get_raw_bytes(uri)
        #bucket = storage_client.get_bucket(uri.hostname)
        #blob = bucket.get_blob(uri.path[1:])

        inputpdf = PdfFileReader(
            io.BytesIO(bytes), strict=False)

        split_pdf(inputpdf, start_page, end_page, uri, gcs_output_uri,
                  gcs_output_uri_prefix + "/" + entity.type_)
