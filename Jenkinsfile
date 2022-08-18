node {
    def scmVars = checkout scm
    currentBuild.displayName = "${BUILD_NUMBER} - ${scmVars.GIT_BRANCH}"
    
    try {

    stage('Build & Push') {
        def commit = sh(returnStdout: true, script: "git log -n 1 --pretty=format:'%h'").trim()
        def branch = scmVars.GIT_BRANCH.split('/').last()

        docker.withRegistry("https://${REGISTRY_HOST}", "${REGISTRY_CREDENTIALS}") {
            def args = '.'

            if (branch == 'master' || branch == 'production') {
                args = "-f Dockerfile.production ."
            }

            def image = docker.build("${APP}",args)

            if (args != '.') {
                image.push(branch)
            }
            else {
                image.push('latest')
            }

            image.push(commit)
        }
    }
    
    stage('Delete jobs') {
        withKubeConfig(caCertificate: '', credentialsId: "${KUBE_CREDENTIALS}", serverUrl: "https://${KUBE_ENDPOINT}") {
            sh '''
                for job in $(kubectl -n scraper get -o json jobs | jq -r '.items[].metadata.labels["job-name"]'); do
                    kubectl -n scraper delete job "$job" || true
                    kubectl -n scraper delete pods -l job-name="$job" || true
                done
               '''
        }
    }

    }
    catch (exception) {
        currentBuild.result = 'FAILURE'
    }
    finally {
        slackNotify('#new_scrapers_deploy')
    }
}

@NonCPS
def slackNotify(channel)
{
    def color = 'good'
    if (currentBuild.currentResult != 'SUCCESS') {
        color = 'danger'
    }

    def changeLogSets = currentBuild.changeSets
    def changeLogMsg = "Changes:\n"

    for (int i = 0; i < changeLogSets.size(); i++) {
        def entries = changeLogSets[i].items
        for (int j = 0; j < entries.length; j++) {
            def entry = entries[j]
            changeLogMsg += "- ${entry.msg} [${entry.author}]\n"
        }
    }

    def message = "${env.JOB_NAME} - ${currentBuild.displayName} ${currentBuild.currentResult} after ${currentBuild.durationString} ${changeLogMsg}"

    slackSend channel: channel, color: color, message: message
}
