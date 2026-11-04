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
    // Pinned tools, installed into ./bin by the Tools stage.
    GOLANGCI_LINT_VERSION = 'v1.61.0'
    GOTESTSUM_VERSION     = 'v1.12.0'
  }

  // There is no Makefile: these are the commands from CLAUDE.md.

  stages {
    stage('Tools') {
      steps {
        sh '''
          GOBIN="$WORKSPACE/bin" go install "github.com/golangci/golangci-lint/cmd/golangci-lint@$GOLANGCI_LINT_VERSION"
          GOBIN="$WORKSPACE/bin" go install "gotest.tools/gotestsum@$GOTESTSUM_VERSION"
        '''
      }
    }

    stage('Lint') {
      steps {
        // gofmt only the tracked files: GOMODCACHE lives under the workspace.
        sh '''
          out=$(git ls-files -z '*.go' | xargs -0 gofmt -l)
          if [ -n "$out" ]; then echo "gofmt needed:"; echo "$out"; exit 1; fi
          go vet ./...
          go vet -tags=integration ./...
          bin/golangci-lint run ./...
          bin/golangci-lint run --build-tags=integration ./...
        '''
      }
    }

    stage('Unit tests') {
      steps {
        sh 'mkdir -p reports && bin/gotestsum --junitfile reports/unit.xml -- -race -count=1 ./...'
      }
      post { always { junit allowEmptyResults: true, testResults: 'reports/unit.xml' } }
    }

    stage('Integration tests') {
      steps {
        // Compose binds fixed host ports, so integration runs are serialised per agent pool.
        lock(resource: 'loadforge-integration-ports') {
          sh '''
            docker compose -f deploy/compose/docker-compose.yml up -d --wait
            docker compose -f deploy/compose/docker-compose.yml exec -T kafka \\
              /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \\
              --create --if-not-exists --topic loadforge.hdr.snapshots --partitions 12 --replication-factor 1
          '''
          sh 'mkdir -p reports && bin/gotestsum --junitfile reports/integration.xml -- -race -count=1 -tags=integration ./...'
        }
      }
      post {
        always {
          sh 'mkdir -p reports && docker compose -f deploy/compose/docker-compose.yml logs --no-color > reports/compose.log 2>&1 || true'
          sh 'docker compose -f deploy/compose/docker-compose.yml down -v --remove-orphans || true'
          junit allowEmptyResults: true, testResults: 'reports/integration.xml'
          archiveArtifacts allowEmptyArchive: true, artifacts: 'reports/compose.log'
        }
      }
    }

    stage('Build images') {
      steps {
        sh '''
          for svc in controlplane worker aggregator calibrate; do
            docker build --build-arg SERVICE="$svc" -t "$REGISTRY/$svc:$VERSION" .
          done
        '''
      }
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
