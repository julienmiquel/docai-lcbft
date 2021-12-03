# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START aiplatform_predict_image_classification_sample]
import base64
import os
import tempfile
from google.api_core.exceptions import FailedPrecondition

from google.cloud import aiplatform
from google.cloud.aiplatform.gapic.schema import predict

from pdf_to_jpg import compress_under_size




def predict_image_classification(
    file_content,
    project="660199673046",
    endpoint_id="1007486902577659904",
    location="europe-west4",
    api_endpoint: str = "europe-west4-aiplatform.googleapis.com",
    
):
    # The AI Platform services require regional API endpoints.
    client_options = {"api_endpoint": api_endpoint}
    # Initialize client that will be used to create and send requests.
    # This client only needs to be created once, and can be reused for multiple requests.
    client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)

    # The format of each instance should conform to the deployed model's prediction input schema.
    encoded_content = base64.b64encode(file_content).decode("utf-8")
    instance = predict.instance.ImageClassificationPredictionInstance(
        content=encoded_content,
    ).to_value()
    instances = [instance]
    # See gs://google-cloud-aiplatform/schema/predict/params/image_classification_1.0.0.yaml for the format of the parameters.
    parameters = predict.params.ImageClassificationPredictionParams(
        confidence_threshold=0.5, max_predictions=1,
    ).to_value()
    endpoint = client.endpoint_path(
        project=project, location=location, endpoint=endpoint_id
    )

    try:
        response = client.predict(
            endpoint=endpoint, instances=instances, parameters=parameters
        )
        print("response")
        print(" deployed_model_id:", response.deployed_model_id)
    except FailedPrecondition as err:
        print(f"ERROR: {err.args}")
        if "exceeds 1.500MB limit" in f"{err.args}":
            print("Try to reduce size and relaunch because of the automl 1.500MB limit")
            tmpFile = os.path.join(tempfile.gettempdir(), "tmp.jpg" ) 
            with open(tmpFile, "wb") as f:
                f.write(file_content)
            success, outpath =compress_under_size(1.2*1024*1024, tmpFile)
            if success==True:
                with open(outpath, "rb") as f:
                    content = f.read()
                    return predict_image_classification(content)
            else:
                raise err


    # See gs://google-cloud-aiplatform/schema/predict/prediction/classification.yaml for the format of the predictions.
    predictions = response.predictions
    for prediction in predictions:
        print(" prediction:", dict(prediction))

    try:
        return predictions[0]['displayNames'][0]
    except Exception as err :
        print(f"ERROR in result predict_image_classification: {err}")
    return "_unknown_"





