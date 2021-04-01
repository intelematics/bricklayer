pipeline {

    agent any

    stages {

        stage('Build') {
            steps {
                sh 'make build'
            }
        }

        stage('Publish') {
            steps {
                sh 'make publish-release'
            }
        }
    }
}
