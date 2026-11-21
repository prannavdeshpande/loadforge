// loadforge-ci: a single Pipeline job (not multibranch). BRANCH picks the
// branch to build; images are pushed only for main.
//
// One-time job setup (also in CLAUDE.md, "CI (Jenkins)"):
//   1. loadforge-ci > Configure > Pipeline:
//        Definition        Pipeline script from SCM
//        SCM               Git, https://github.com/prannavdeshpande/loadforge.git
//        Branch Specifier  */main          (only this one: delete any other)
//        Script Path       Jenkinsfile
//        Lightweight checkout  unticked
//      Save.
//   2. Build Now, once. That run builds main and registers the BRANCH
//      parameter declared below (and the other job options).
//   3. From then on: Build with Parameters, BRANCH = any branch on origin.
//      Blank means main.
//
// This Jenkinsfile is always read from main; the Checkout stage then checks
// out BRANCH. A change to this file on a branch takes effect once merged
// (try it first with Replay). Never set the Branch Specifier to */${BRANCH}:
// the job has no BRANCH parameter until this file has been read once, so
// the specifier cannot resolve, the parameter is never registered and
// Jenkins keeps building an old revision.
pipeline {
  agent { label 'linux && docker' }   // needs Go 1.23 and Docker with the compose plugin

  parameters {
    string(name: 'BRANCH', defaultValue: 'main', trim: true,
           description: 'Branch on origin to build and test; blank means main. Images are pushed only for main.')
  }

  options {
    timestamps()
    timeout(time: 60, unit: 'MINUTES')
    // No abortPrevious: in a single job, a build of one branch must never
    // abort a running build of another (main included). Builds queue.
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
          // Blank or unset (the very first run has no parameter yet): main.
          def branch = (params.BRANCH ?: '').trim()
          if (!branch) {
            branch = 'main'
          }
          // First gate, before the name reaches any shell: a plain branch
          // name only (no refspecs, SHAs, options or whitespace). git
          // check-ref-format below applies git's full rules.
          if (branch.length() > 200 || !(branch ==~ /[A-Za-z0-9][A-Za-z0-9._\/-]*/) ||
              branch.contains('..') || branch.contains('//') || branch.endsWith('/') ||
              branch.endsWith('.') || branch.endsWith('.lock')) {
            error("BRANCH '${branch}' is not a valid branch name")
          }
          if (!scm || !scm.userRemoteConfigs) {
            error('loadforge-ci must be configured as "Pipeline script from SCM" (see the top of the Jenkinsfile)')
          }
          def remote = scm.userRemoteConfigs[0]
          def expected = ''
          // The name is passed in the environment, never interpolated into
          // the shell command.
          withEnv(["CHECK_BRANCH=${branch}", "REPO_URL=${remote.url}"]) {
            if (sh(returnStatus: true, script: 'git check-ref-format --branch "$CHECK_BRANCH" >/dev/null') != 0) {
              error("BRANCH '${branch}' is not a valid git branch name (git check-ref-format)")
            }
            // Exit 2: no such branch on origin; anything else non-zero:
            // origin could not be listed.
            def lsRemote = {
              sh(returnStdout: true, script: '''
                set +e
                out=$(git ls-remote --exit-code --heads "$REPO_URL" "refs/heads/$CHECK_BRANCH")
                rc=$?
                echo "$rc ${out%%[[:space:]]*}"
              ''').trim()
            }
            def result = remote.credentialsId ?
              withCredentials([gitUsernamePassword(credentialsId: remote.credentialsId)]) { lsRemote() } :
              lsRemote()
            def parts = result.tokenize(' ')
            if (parts[0] == '2') {
              error("BRANCH '${branch}' does not exist on origin (${remote.url})")
            }
            if (parts[0] != '0' || parts.size() < 2) {
              error("could not list branches on origin (${remote.url}): git ls-remote exited ${parts[0]}")
            }
            expected = parts[1]
          }
          echo "Building ${branch}: origin has ${expected}"

          checkout([$class: 'GitSCM',
                    branches: [[name: "refs/heads/${branch}"]],
                    userRemoteConfigs: scm.userRemoteConfigs,
                    extensions: [[$class: 'CleanBeforeCheckout'],
                                 [$class: 'LocalBranch', localBranch: branch]]])
          def head = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
          if (head != expected) {
            echo "Note: origin/${branch} moved from ${expected} to ${head} between the check and the checkout; building ${head}"
          }
          env.BUILD_BRANCH = branch
          env.VERSION = head.take(12)
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
    // otherwise idle machine), so both test stages:
    //   - hold a per-node lock, so no other LoadForge build runs its tests
    //     on this agent at the same time (within one build,
    //     internal/timingtest lets only one timing test run at once);
    //   - refuse to start while any loadforge-* container runs on the
    //     agent: a developer's stack, or one a crashed build left behind,
    //     takes memory the tests need (and, for integration, the ports);
    //   - record host pressure, memory and swap before and after, archived
    //     as reports/host-stats.txt. A failed timing test also logs the
    //     longest scheduler stall it saw; together they show whether the
    //     host stalled the test.
    // Failed tests are never re-run: a retry would hide a real
    // coordinated-omission regression.
    stage('Unit tests') {
      steps {
        lock(resource: "loadforge-timing-${env.NODE_NAME}") {
          sh '''
            stray=$(docker ps --format '{{.Names}}  (compose project: {{.Label "com.docker.compose.project"}})' | grep -E '^loadforge-' || true)
            if [ -n "$stray" ]; then
              echo "LoadForge containers are running on this agent; stop them before the tests run:"
              echo "$stray"
              echo "e.g. docker compose -p <project> -f deploy/compose/docker-compose.yml stop"
              exit 1
            fi
            bash deploy/ci/host-stats.sh unit:before
            mkdir -p reports
            bin/gotestsum --junitfile reports/unit.xml -- -race -count=1 ./...
          '''
        }
      }
      post {
        always {
          sh 'bash deploy/ci/host-stats.sh unit:after || true'
          junit allowEmptyResults: true, testResults: 'reports/unit.xml'
          archiveArtifacts allowEmptyArchive: true, artifacts: 'reports/host-stats.txt'
        }
      }
    }

    stage('Integration tests') {
      steps {
        // Compose binds fixed host ports, so integration runs are serialised
        // per agent pool. Lock order is always ports, then timing.
        lock(resource: 'loadforge-integration-ports') {
          lock(resource: "loadforge-timing-${env.NODE_NAME}") {
            // This build's own stack is not up yet, so any loadforge-*
            // container here belongs to someone else.
            sh '''
              stray=$(docker ps --format '{{.Names}}  (compose project: {{.Label "com.docker.compose.project"}})' | grep -E '^loadforge-' || true)
              if [ -n "$stray" ]; then
                echo "LoadForge containers are running on this agent; stop them before the tests run:"
                echo "$stray"
                echo "e.g. docker compose -p <project> -f deploy/compose/docker-compose.yml stop"
                exit 1
              fi
              bash deploy/ci/host-stats.sh integration:before
              docker compose -f deploy/compose/docker-compose.yml up -d --wait
              docker compose -f deploy/compose/docker-compose.yml exec -T kafka \\
                /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \\
                --create --if-not-exists --topic loadforge.hdr.snapshots --partitions 12 --replication-factor 1
            '''
            // The integration build also compiles every unit test, which the
            // previous stage already ran: select the integration tests only.
            sh '''
              mkdir -p reports
              bin/gotestsum --junitfile reports/integration.xml -- \\
                -race -count=1 -timeout 20m -tags=integration -run '^TestIntegration' ./...
            '''
          }
        }
      }
      post {
        always {
          sh 'bash deploy/ci/host-stats.sh integration:after || true'
          sh 'mkdir -p reports && docker compose -f deploy/compose/docker-compose.yml logs --no-color > reports/compose.log 2>&1 || true'
          sh 'docker compose -f deploy/compose/docker-compose.yml down -v --remove-orphans || true'
          junit allowEmptyResults: true, testResults: 'reports/integration.xml'
          archiveArtifacts allowEmptyArchive: true, artifacts: 'reports/compose.log, reports/host-stats.txt'
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
