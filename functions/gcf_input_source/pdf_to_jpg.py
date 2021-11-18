from typing import List
from ghostscript import Ghostscript
import gcs


def convert_to_image(filepath_in: str) -> str:
    """
    Function that converts a single page pdf file into an image
    taken from https://stackoverflow.com/questions/331918/converting-a-pdf-to-a-series-of-images-with-python

    Parameters
    ----------
    filepath_in: str
        local filepath of a pdf file

    Returns
    -------
    filepath_out: str
        local filepath of the converted jpg file
    """
    filepath_out = filepath_in.replace("pdf", "jpg")
    args = [
        "pdf2jpeg",  # actual value doesn't matter
        "-dNOPAUSE",
        "-sDEVICE=jpeg",
        "-r300",
        "-sOutputFile=" + filepath_out,
        filepath_in,
    ]
    Ghostscript(*args)
    return filepath_out


def convert_all_pdf_to_jpg(pdf_filepaths: List[str]) -> List[str]:
    """
    Function that converts a single page pdf file into an image

    Parameters
    ----------
    pdf_filepaths: List[str]
        list of local filepaths to single pages pdf files

    Returns
    -------
    jpg_filepaths: str
        list of local filepaths to single pages jpg files
    """
    jpg_filepaths = []
    for filepath_in in pdf_filepaths:
        try:
            filepath_out = convert_to_image(filepath_in)
            jpg_filepaths.append(filepath_out)
        except Exception as err:
            print("An error occured with file: " + filepath_in )
            print(err)
            print("WARNING: Ignoring the ERROR try to convert the next file.")
    return jpg_filepaths


    


def convertToImage(file, outputBucket):
    from pdf2image import convert_from_path

    # Set poppler path
    poppler_path = "/var/task/lib/poppler-utils-0.26/usr/bin"

    images = convert_from_path(file, dpi=300, poppler_path=poppler_path)

    #bucket = storage_client.get_bucket(outputBucket)

    for i in range(len(images)):
        file = "/tmp/images/" + \
            file[4:].replace("/", "").replace(".pdf",
                                           "").replace(":", "") + '-p' + str(i) + '.jpg'

        # Save pages as images in the pdf
        images[i].save(file, 'JPEG')
        gcs.send_file(outputBucket, file)
        #bucket.upload_from_filename(file)

