pipeline {

    agent any

    stages {

        stage('Publish') {
            steps {
                sh 'make publish-release'
            }
        }
    }
}
