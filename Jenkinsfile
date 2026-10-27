pipeline {
  agent { label 'linux && docker' }   // needs Go 1.23 and Docker with the compose plugin

  options {
    timestamps()
    timeout(time: 45, unit: 'MINUTES')
    disableConcurrentBuilds(abortPrevious: true)
    buildDiscarder(logRotator(numToKeepStr: '30'))
  }

  environment {
    GOFLAGS              = '-mod=readonly'
    GOCACHE              = "${WORKSPACE}/.cache/go-build"
    GOMODCACHE           = "${WORKSPACE}/.cache/go-mod"
    COMPOSE_PROJECT_NAME = "loadforge-${env.BUILD_TAG}".toLowerCase().replaceAll('[^a-z0-9-]', '-')
    REGISTRY             = "${env.LOADFORGE_REGISTRY ?: 'registry.example.com/loadforge'}"
    VERSION              = "${env.GIT_COMMIT?.take(12) ?: 'dev'}"
  }

  stages {
    stage('Tools') {
      steps { sh 'make tools' }
    }

    stage('Lint') {
      steps { sh 'make lint' }
    }

    stage('Unit tests') {
      steps { sh 'make test' }
      post { always { junit allowEmptyResults: true, testResults: 'reports/unit.xml' } }
    }

    stage('Integration tests') {
      steps {
        // Compose binds fixed host ports, so integration runs are serialised per agent pool.
        lock(resource: 'loadforge-integration-ports') {
          sh 'make up'
          sh 'make test-integration'
        }
      }
      post {
        always {
          sh 'mkdir -p reports && docker compose -f deploy/compose/docker-compose.yml logs --no-color > reports/compose.log 2>&1 || true'
          sh 'make down || true'
          junit allowEmptyResults: true, testResults: 'reports/integration.xml'
          archiveArtifacts allowEmptyArchive: true, artifacts: 'reports/compose.log'
        }
      }
    }

    stage('Build images') {
      steps { sh 'make images REGISTRY=$REGISTRY VERSION=$VERSION' }
    }

    stage('Push images') {
      when { branch 'main' }
      steps {
        withCredentials([usernamePassword(credentialsId: 'loadforge-registry',
                                          usernameVariable: 'REG_USER',
                                          passwordVariable: 'REG_PASS')]) {
          sh '''
            echo "$REG_PASS" | docker login "${REGISTRY%%/*}" -u "$REG_USER" --password-stdin
            for svc in controlplane worker aggregator calibrate; do
              docker push "$REGISTRY/$svc:$VERSION"
            done
          '''
        }
      }
    }
  }

  post {
    cleanup { sh 'docker logout "${REGISTRY%%/*}" || true' }
  }
}
