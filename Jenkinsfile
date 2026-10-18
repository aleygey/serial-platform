// Build and release ownership moved to GitHub Actions in v0.8.9.
// Keep existing Jenkins webhooks harmless: no agent, credentials, build, tag,
// artifact upload, or GitHub Release publication is requested here.
pipeline {
    agent none
    stages {
        stage('Migrated to GitHub Actions') {
            steps {
                echo 'Serial Platform builds and releases now run in GitHub Actions: https://github.com/aleygey/serial-platform/actions'
                echo 'This legacy Jenkins entry is intentionally non-publishing. The Jenkins service itself is not changed.'
            }
        }
    }
}
