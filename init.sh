terraform -chdir=./init/ init
terraform -chdir=./init/ apply -auto-approve -var=project=docai-335917
