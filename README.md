aws cloudformation create-stack --profile malawi  --stack-name my-eks-vpc-stack --template-url https://s3.us-west-2.amazonaws.com/amazon-eks/cloudformation/2020-10-29/amazon-eks-vpc-private-subnets.yaml



aws iam create-role --profile malawi   --role-name myAmazonEKSClusterRole   --assume-role-policy-document file://"eks-cluster-role-trust-policy.json"


aws iam attach-role-policy --profile malawi  --policy-arn arn:aws:iam::aws:policy/AmazonEKSClusterPolicy  --role-name myAmazonEKSClusterRole


aws eks update-kubeconfig --profile malawi --name my-cluster


aws iam create-role --profile malawi  --role-name AmazonEKSFargatePodExecutionRole  --assume-role-policy-document file://"pod-execution-role-trust-policy.json"



aws iam attach-role-policy --profile malawi  --policy-arn arn:aws:iam::aws:policy/AmazonEKSFargatePodExecutionRolePolicy   --role-name AmazonEKSFargatePodExecutionRole


kubectl patch deployment coredns     -n kube-system     --type json   -p="[{'op': 'remove', 'path': '/spec/template/metadata/annotations/eks.amazonaws.com~1compute-type'}]"



# to switch between AWS and minoi
need to update the mino catalog properties in trino (minio.properties) and the metastore docker compose config and the init.sql create schema statement. 
s3 endpoint needs to point to minio or s3 respectively
hive.s3.ssl.enabled to false for minio , true for aws
can leave region set and pointing to eu-central-1 for minio