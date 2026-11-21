// loadforge-ci: a single Pipeline job (not multibranch). BRANCH picks what
// to build; images are pushed only for main.
//
// Job configuration this file relies on:
//   - "This project is parameterized" is set by the parameters block below
//     on the first run; that first run builds main.
//   - To run a branch's own Jenkinsfile (not main's), set the job's SCM
//     branch specifier to */${BRANCH} and untick "Lightweight checkout".
pipeline {
  agent { label 'linux && docker' }   // needs Go 1.23 and Docker with the compose plugin

  parameters {
    string(name: 'BRANCH', defaultValue: 'main', trim: true,
           description: 'Branch to build and test. Images are pushed only when this is main.')
  }

  options {
    timestamps()
    timeout(time: 60, unit: 'MINUTES')
    // No abortPrevious: in a single job, a manual build of a feature branch
    // must not abort a running main build. Builds queue instead.
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '30'))
    skipDefaultCheckout(true)          // the Checkout stage checks out BRANCH
  }

  environment {
    GOFLAGS              = '-mod=readonly'
    GOCACHE              = "${WORKSPACE}/.cache/go-build"
    GOMODCACHE           = "${WORKSPACE}/.cache/go-mod"
    COMPOSE_PROJECT_NAME = "loadforge-${env.BUILD_TAG}".toLowerCase().replaceAll('[^a-z0-9-]', '-')
    REGISTRY             = "${env.LOADFORGE_REGISTRY ?: 'registry.example.com/loadforge'}"
    // VERSION and IS_MAIN are set in the Checkout stage, from the commit
    // actually checked out: GIT_COMMIT here would predate that checkout.
    // Pinned tools, installed into ./bin by the Tools stage.
    GOLANGCI_LINT_VERSION = 'v1.61.0'
    GOTESTSUM_VERSION     = 'v1.12.0'
    GOTOOLCHAIN           = 'go1.23.0'
  }

  // There is no Makefile: these are the commands from CLAUDE.md.

  stages {
    stage('Checkout') {
      steps {
        script {
          def branch = params.BRANCH ?: 'main'   // null on the very first, unparameterized run
          // A branch name only: no refspecs, SHAs or option-like values.
          if (!(branch ==~ /[A-Za-z0-9][A-Za-z0-9._\/-]*/) || branch.contains('..') || branch.endsWith('.lock')) {
            error("BRANCH '${branch}' is not a valid branch name")
          }
          checkout([$class: 'GitSCM',
                    branches: [[name: "refs/heads/${branch}"]],
                    userRemoteConfigs: scm.userRemoteConfigs,
                    extensions: [[$class: 'CleanBeforeCheckout'],
                                 [$class: 'LocalBranch', localBranch: branch]]])
          env.BUILD_BRANCH = branch
          env.VERSION = sh(script: 'git rev-parse --short=12 HEAD', returnStdout: true).trim()
          env.IS_MAIN = (branch == 'main') ? 'true' : 'false'
          currentBuild.displayName = "#${env.BUILD_NUMBER} ${branch}@${env.VERSION}"
        }
      }
    }

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

    // The timing tests assert on dispatch lag (CLAUDE.md: run them on an
    // otherwise idle machine). So test stages hold a per-node lock, keeping
    // other LoadForge builds off this agent while they run, and run one
    // package at a time (-p 1): a parallel build or another package's load
    // generator starves the scheduler and fails them. Compilation still
    // runs in parallel first (-run '^$' builds every test binary and runs
    // no test). Failed tests are never re-run: a retry would hide a real
    // coordinated-omission regression.
    stage('Unit tests') {
      steps {
        lock(resource: "loadforge-timing-${env.NODE_NAME}") {
          sh '''
            mkdir -p reports
            go test -race -count=1 -run '^$' ./...
            bin/gotestsum --junitfile reports/unit.xml -- -race -count=1 -p 1 ./...
          '''
        }
      }
      post { always { junit allowEmptyResults: true, testResults: 'reports/unit.xml' } }
    }

    stage('Integration tests') {
      steps {
        // Compose binds fixed host ports, so integration runs are serialised
        // per agent pool. Lock order is always ports, then timing.
        lock(resource: 'loadforge-integration-ports') {
          lock(resource: "loadforge-timing-${env.NODE_NAME}") {
            sh '''
              docker compose -f deploy/compose/docker-compose.yml up -d --wait
              docker compose -f deploy/compose/docker-compose.yml exec -T kafka \\
                /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \\
                --create --if-not-exists --topic loadforge.hdr.snapshots --partitions 12 --replication-factor 1
            '''
            // The integration build also compiles every unit test, which the
            // previous stage already ran: select the integration tests only.
            sh '''
              mkdir -p reports
              go test -race -count=1 -tags=integration -run '^$' ./...
              bin/gotestsum --junitfile reports/integration.xml -- \\
                -race -count=1 -p 1 -timeout 20m -tags=integration -run '^TestIntegration' ./...
            '''
          }
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
      // Only main is pushed, and only the commit this build checked out.
      when { expression { env.IS_MAIN == 'true' } }
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
