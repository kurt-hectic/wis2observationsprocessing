aws cloudformation create-stack --profile malawi  --stack-name my-eks-vpc-stack --template-url https://s3.us-west-2.amazonaws.com/amazon-eks/cloudformation/2020-10-29/amazon-eks-vpc-private-subnets.yaml



aws iam create-role --profile malawi   --role-name myAmazonEKSClusterRole   --assume-role-policy-document file://"eks-cluster-role-trust-policy.json"


aws iam attach-role-policy --profile malawi  --policy-arn arn:aws:iam::aws:policy/AmazonEKSClusterPolicy  --role-name myAmazonEKSClusterRole


aws eks update-kubeconfig --profile malawi --name my-cluster


aws iam create-role --profile malawi  --role-name AmazonEKSFargatePodExecutionRole  --assume-role-policy-document file://"pod-execution-role-trust-policy.json"



aws iam attach-role-policy --profile malawi  --policy-arn arn:aws:iam::aws:policy/AmazonEKSFargatePodExecutionRolePolicy   --role-name AmazonEKSFargatePodExecutionRole


kubectl patch deployment coredns     -n kube-system     --type json   -p="[{'op': 'remove', 'path': '/spec/template/metadata/annotations/eks.amazonaws.com~1compute-type'}]"