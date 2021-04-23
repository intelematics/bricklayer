pipeline {

    agent any

    stages {

        stage('Publish Dev') {
            steps {
                sh 'make publish-dev'
            }
        }

        stage('Publish Release') {
            steps {
                sh 'make publish-release'
            }
        }
    }
}
