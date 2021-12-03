from typing import List
from ghostscript import Ghostscript
import gcs


def convert_to_image(filepath_in: str, dpi = 300) -> str:
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
        f"-r{dpi}",
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


import os
from PIL import Image
import shutil

def compress_under_size(size, file_path):
    '''file_path is a string to the file to be custom compressed
    and the size is the maximum size in bytes it can be which this 
    function searches until it achieves an approximate supremum'''

    current_size = os.stat(file_path).st_size
    quality = int(size/current_size*100) 

    print(f"current_size:{current_size} - file: {file_path} ")
    file_path_out = file_path.replace(".jp", "_scaled.jp")

    if current_size < size:
        print(f"Nothing to do {current_size}  {size}")
        shutil.copy(file_path, file_path_out)
    while current_size > size or quality == 0:
        print(f"quality : {quality} - {current_size}")
        if quality == 0:
            os.remove(file_path_out)
            print(f"Error: File cannot be compressed below this size: {current_size}")
            return False, None

        current_size = compress_pic(file_path, file_path_out, quality)
        current_size = os.stat(file_path_out).st_size
        if quality < 30:
            quality -= 5
        else:
            quality -= 20
    
    print(f"current_size:{current_size} - file: {file_path} ")
    return True, file_path_out


def compress_pic(file_path, file_path_out, qual):
    '''File path is a string to the file to be compressed and
    quality is the quality to be compressed down to'''
    with Image.open(file_path) as picture :
        dim = picture.size
        print(f"{dim}")
        picture = picture.resize((int(dim[0]*qual/100),int(dim[1]*qual/100)))
        dim = picture.size
        print(f"{dim}")
        picture.save(file_path_out,"JPEG", optimize=True, quality=qual) 

        processed_size = os.stat(file_path_out).st_size

        return processed_size


