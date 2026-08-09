pipeline {
    agent {
        label 'serial-platform'
    }

    stages {
        stage('Checkout') {
            steps {
                echo 'Checking out serial-platform repository...'
                git clone 'git@github.com:aleygey/serial-platform.git'
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