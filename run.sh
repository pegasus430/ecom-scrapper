#!/usr/bin/env bash
set -e # Exit with nonzero exit code if anything fails

RSA_KEY_PATH='/root/.ssh/id_rsa'
WORK_DIR='/app/content_analytics'


# check env vars
[ -z "$SCRAPERS_REPO_URL" ] && {
    echo "SCRAPERS_REPO_URL is not set"
    exit 2
}

[ -z "$SCRAPERS_REPO_RSA_KEY" ] && {
    echo "SCRAPERS_REPO_RSA_KEY is not set"
    exit 2
}

[ -z "$SCRAPERS_GIT_BRANCH" ] && {
    echo "SCRAPERS_GIT_BRANCH is not set"
    exit 2
}


create_rsa_key() {
    /bin/mkdir -p /root/.ssh/
    echo -e "$SCRAPERS_REPO_RSA_KEY" > "$RSA_KEY_PATH"
    /bin/chmod 400 "$RSA_KEY_PATH"
}


clone_repo() {
    GIT_SSH_COMMAND="/usr/bin/ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no" \
        /usr/bin/git clone "$SCRAPERS_REPO_URL" "$WORK_DIR"
}


run_scraper() {
    /usr/local/bin/pip install --no-cache-dir -r requirements.txt
    /usr/local/bin/python content_analytics/main.py
}


echo "Creating RSA key"
create_rsa_key
echo "Cloning the repo from $SCRAPERS_REPO_URL; git branch: $SCRAPERS_GIT_BRANCH"
clone_repo
cd "$WORK_DIR" && /usr/bin/git checkout "$SCRAPERS_GIT_BRANCH"
run_scraper

