#!/usr/bin/env bash
#set -e # Exit with nonzero exit code if anything fails


IMAGE_NAME='cai-scraper'
IMAGE_UUID=$(uuidgen)
TMP_IMAGE_NAME="${IMAGE_NAME}-${IMAGE_UUID}"
WATCHER_ENDPOINT='https://10-0-0-190:8080/api/restart_branch_jobs'


# check env vars from Jenkins
[ -z "$DOCKER_REGISTRY_LOGIN" ] && {
    echo "DOCKER_REGISTRY_LOGIN is not set" && exit 2
}
[ -z "$DOCKER_REGISTRY_PASSWD" ] && {
    echo "DOCKER_REGISTRY_PASSWD is not set" && exit 2
}
[ -z "$DOCKER_REGISTRY_REPO" ] && {
    echo "DOCKER_REGISTRY_REPO is not set" && exit 2
}
[ -z "$GIT_BRANCH" ] && {
    echo "GIT_BRANCH is not set" && exit 2
}
# image build version
[ -z "$PROMOTED_NUMBER" ] && {
    echo "PROMOTED_NUMBER is not set" && exit 2
}
# token for restarting kube jobs
#[ -z "$WATCHER_SECURE_TOKEN" ] && {
#    echo "$WATCHER_SECURE_TOKEN is not set" && exit 2
#}


push_image() {
    NEW_IMAGE="${DOCKER_REGISTRY_REPO}/${IMAGE_NAME}:${PROMOTED_NUMBER}"  # example: nexus3-registry.contentanalyticsinc.com/cai-scraper:0.2
    # link new image to latest image
    LATEST_IMAGE="${DOCKER_REGISTRY_REPO}/${IMAGE_NAME}:latest"  # example: nexus3-registry.contentanalyticsinc.com/cai-scraper:latest

    echo "Pushing ${NEW_IMAGE} and ${LATEST_IMAGE} to docker registry"
    docker tag "$IMAGE_NAME" "$NEW_IMAGE"
    docker tag "$NEW_IMAGE" "$LATEST_IMAGE"
    docker push "$NEW_IMAGE"
    docker push "$LATEST_IMAGE"
}


# currently we just keep them running
restart_branch_jobs() {
    # sending HTTP request to the watcher
    #curl --data "branch=${GIT_BRANCH}&token=${WATCHER_SECURE_TOKEN}" "$WATCHER_ENDPOINT"
    echo "Sending restart request $WATCHER_ENDPOINT"
}


# put some build info into a file for docker to pull into the container
date > build_info
echo $(git status) >> build_info

# Do docker build and run it so we can test it
echo "Built: $TMP_IMAGE_NAME and run tests"
docker build -t "$TMP_IMAGE_NAME" .
docker run --rm -ti --entrypoint /bin/sh "$TMP_IMAGE_NAME" -c /run_unit_tests.sh  # override default entrypoint

if [ $? -ne 0 ]; then
    echo "Tests failed"
else
    echo "Tests succeeded"
    echo "Login to the docker registry"
    docker login -u "$DOCKER_REGISTRY_LOGIN" -p "$DOCKER_REGISTRY_PASSWD" "$DOCKER_REGISTRY_REPO" && \
        push_image && restart_branch_jobs && \
        echo "Remove: $TMP_IMAGE_NAME" && \
        docker rmi "$TMP_IMAGE_NAME" && \
        exit 0
fi

exit 1
