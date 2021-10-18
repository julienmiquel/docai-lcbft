echo replace project_ID by your GCP project_ID
export project_ID=XXXXXX
export ENV=kyc_kb_geo_demo
echo -var="env=${ENV}" -var="project=${project_ID}"

terraform destroy -auto-approve -var="env=${ENV}" -var="project=${project_ID}"
terraform apply -auto-approve -var="deletion_protection=false" -var="env=${ENV}" -var="project=${project_ID}"
