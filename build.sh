echo replace project_ID by your GCP project_ID
terraform destroy -auto-approve -var="env=test_lcbft_1" -var="project=project_ID"
terraform apply -auto-approve -var="deletion_protection=false" -var="env=test_lcbft_1" -var="project=project_ID"
