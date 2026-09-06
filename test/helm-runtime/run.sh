#!/usr/bin/env bash
set -euo pipefail

# This fixture owns a disposable CI cluster, never a user's current context.
[[ "${GITHUB_ACTIONS:-}" == true ]] || { echo 'Run through Deployment Validation in GitHub Actions'; exit 1; }
cluster="cursus-helm-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
scratch=$(mktemp -d)
export KUBECONFIG="$scratch/kubeconfig"
if kind get clusters | grep -Fxq "$cluster"; then
  echo 'Refusing to reuse an existing kind cluster'
  exit 1
fi
cleanup() {
  result=$?
  trap - EXIT
  if [[ $result != 0 ]]; then
    kubectl get pods,pvc -A -o wide || true
    kubectl get events -A --sort-by=.lastTimestamp || true
    for ns in helm-standalone helm-cluster; do
      kubectl -n "$ns" describe pods || true
      kubectl -n "$ns" logs -l app.kubernetes.io/name=cursus --all-containers --prefix --tail=150 --max-log-requests=5 || true
      kubectl -n "$ns" logs -l cursus-client=true --all-containers --prefix --tail=150 || true
    done
  fi
  kind delete cluster --name "$cluster" || result=1
  exit "$result"
}
trap cleanup EXIT
kind create cluster --name "$cluster" --config test/helm-runtime/kind.yaml --wait 180s \
  --image kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0
kind load docker-image cursus:helm-runtime cursus-helm-smoke:runtime --name "$cluster"

run_client() {
  local phase=$1
  kubectl -n "$ns" apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: sdk-$phase
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 210
  template:
    metadata:
      labels:
        cursus-client: "true"
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: sdk
          image: cursus-helm-smoke:runtime
          imagePullPolicy: Never
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: [ALL]
          resources:
            requests: {cpu: 100m, memory: 64Mi}
            limits: {cpu: "1", memory: 256Mi}
          env:
            - {name: HELM_TEST_PHASE, value: "$phase"}
            - {name: HELM_BROKER_ADDRESS, value: "cursus.$ns.svc.cluster.local:9000"}
            - {name: SSL_CERT_FILE, value: /tls/ca.crt}
            - name: HELM_AUTH_TOKEN
              valueFrom:
                secretKeyRef: {name: test-auth, key: token}
          volumeMounts:
            - {name: tls, mountPath: /tls, readOnly: true}
      volumes:
        - name: tls
          secret: {secretName: test-tls}
EOF
  if ! kubectl -n "$ns" wait --for=condition=complete "job/sdk-$phase" --timeout=240s; then
    kubectl -n "$ns" logs "job/sdk-$phase" || true
    return 1
  fi
  kubectl -n "$ns" logs "job/sdk-$phase"
}

for mode in standalone cluster; do
  ns="helm-$mode"
  kubectl create namespace "$ns"
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj '/CN=helm-runtime-ca' \
    -keyout "$scratch/ca.key" -out "$scratch/ca.crt" 2>/dev/null
  openssl req -new -newkey rsa:2048 -nodes -subj '/CN=cursus' \
    -keyout "$scratch/tls.key" -out "$scratch/tls.csr" 2>/dev/null
  printf 'subjectAltName=DNS:cursus.%s.svc.cluster.local,DNS:*.cursus-headless.%s.svc.cluster.local\nextendedKeyUsage=serverAuth,clientAuth\n' "$ns" "$ns" > "$scratch/extensions"
  openssl x509 -req -in "$scratch/tls.csr" -CA "$scratch/ca.crt" -CAkey "$scratch/ca.key" \
    -CAcreateserial -days 1 -extfile "$scratch/extensions" -out "$scratch/tls.crt" 2>/dev/null
  kubectl -n "$ns" create secret generic test-tls --from-file="$scratch/tls.crt" --from-file="$scratch/tls.key" --from-file="$scratch/ca.crt"
  token=$(openssl rand -hex 24)
  printf '[{"principal":"runtime-test","token":"%s","permissions":["*"]}]' "$token" > "$scratch/users.json"
  kubectl -n "$ns" create secret generic test-auth --from-file="$scratch/users.json" --from-literal="token=$token"
  replicas=1
  extra=()
  if [[ $mode == cluster ]]; then
    replicas=3
    extra=(--set cluster.internalSecretName=test-auth --set cluster.internalTLSSecretName=test-tls)
  fi
  helm install cursus manifests/helm --namespace "$ns" --wait --timeout 8m \
    --set "mode=$mode" --set "replicaCount=$replicas" \
    --set image.repository=cursus --set image.tag=helm-runtime --set image.pullPolicy=Never \
    --set tls.enabled=true --set tls.secretName=test-tls \
    --set authentication.enabled=true --set authentication.secretName=test-auth \
    --set monitoring.enabled=true --set monitoring.serviceMonitor.enabled=false \
    --set networkPolicy.enabled=true --set production=true --set persistence.logDir.size=1Gi \
    "${extra[@]}"
  [[ $(kubectl -n "$ns" get pvc -o json | jq '[.items[] | select(.status.phase == "Bound")] | length') == "$replicas" ]]
  if [[ $mode == cluster ]]; then
    [[ $(kubectl -n "$ns" get pods -l app.kubernetes.io/name=cursus -o json | jq '[.items[].spec.nodeName] | unique | length') == 3 ]]
  fi
  run_client seed
  claims=$(kubectl -n "$ns" get pvc -o json | jq -c '[.items[] | {name:.metadata.name,uid:.metadata.uid,volume:.spec.volumeName}] | sort_by(.name)')
  pods=$(kubectl -n "$ns" get pods -l app.kubernetes.io/name=cursus -o name)
  for pod in $pods; do
    old_uid=$(kubectl -n "$ns" get "$pod" -o jsonpath='{.metadata.uid}')
    kubectl -n "$ns" delete "$pod" --wait=true --timeout=90s
    if [[ $mode == standalone ]]; then
      kubectl -n "$ns" rollout status deployment/cursus --timeout=180s
    else
      kubectl -n "$ns" wait --for=create "$pod" --timeout=90s
      kubectl -n "$ns" wait --for=condition=Ready "$pod" --timeout=180s
    fi
    [[ $(kubectl -n "$ns" get pods -l app.kubernetes.io/name=cursus -o json | jq --arg uid "$old_uid" '[.items[] | select(.metadata.uid == $uid)] | length') == 0 ]]
  done
  [[ $(kubectl -n "$ns" get pvc -o json | jq -c '[.items[] | {name:.metadata.name,uid:.metadata.uid,volume:.spec.volumeName}] | sort_by(.name)') == "$claims" ]]
  run_client restored
  kubectl -n "$ns" get pods,pvc -o wide
done
