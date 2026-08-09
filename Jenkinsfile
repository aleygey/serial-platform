pipeline {
    agent {
        label 'rust'
    }

    stages {
        stage('Checkout') {
            steps {
                echo 'Checking out serial-platform repository...'
                sh "git clone 'git@github.com:aleygey/serial-platform.git'"
            }
        }

        stage('Build') {
            steps {
                echo 'Building...'
            }
        }
        stage('Test') {
            steps {
                echo 'Testing...'
            }
        }
        stage('Deploy') {
            steps {
                echo 'Deploying...'
            }
        }
    }
}