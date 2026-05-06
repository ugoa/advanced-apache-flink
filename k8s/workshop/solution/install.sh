
# install cert
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.18.2/cert-manager.yaml

# add and install helm chart
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.13.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator

# start basic flink job
kubectl create -f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/release-1.13/examples/basic.yaml
