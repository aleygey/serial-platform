def restorePinnedSource() {
    deleteDir()
    unstash 'pinned-source-bundle'
    sh '''
        set -eu

        bundle_path='jenkins-source/pinned-source.bundle'
        commit_path='jenkins-source/SOURCE_GIT_COMMIT'
        checksum_path='jenkins-source/pinned-source.bundle.sha256'

        expected_commit="$(sed -n '1p' "$commit_path")"
        expected_checksum="$(sed -n '1p' "$checksum_path")"
        test "$expected_commit" = "$SOURCE_GIT_COMMIT"

        if command -v sha256sum >/dev/null 2>&1; then
            actual_checksum="$(sha256sum "$bundle_path" | awk '{print $1}')"
        elif command -v shasum >/dev/null 2>&1; then
            actual_checksum="$(LC_ALL=C LANG=C shasum -a 256 "$bundle_path" | awk '{print $1}')"
        else
            printf 'sha256sum or shasum is required to verify the pinned source bundle.\n' >&2
            exit 1
        fi
        test "$actual_checksum" = "$expected_checksum"

        bundle_commit="$(git bundle list-heads "$bundle_path" HEAD | awk '$2 == "HEAD" { print $1 }')"
        test "$bundle_commit" = "$SOURCE_GIT_COMMIT"

        git init -q .
        git bundle verify "$bundle_path" >/dev/null
        git fetch --quiet --tags "$bundle_path" HEAD:refs/ci/pinned-source
        git -c advice.detachedHead=false checkout --quiet --detach "$SOURCE_GIT_COMMIT"
        test "$(git rev-parse HEAD)" = "$SOURCE_GIT_COMMIT"
        test "$(git rev-parse refs/ci/pinned-source)" = "$SOURCE_GIT_COMMIT"

        if test "$AUTOMATIC_RELEASE" = 'true'; then
            test "$(git cat-file -t "$RELEASE_TAG")" = 'tag'
            test "$(git rev-parse "$RELEASE_TAG^{}")" = "$SOURCE_GIT_COMMIT"
        fi

        git update-ref -d refs/ci/pinned-source
        rm -f jenkins-source/pinned-source.bundle \
            jenkins-source/SOURCE_GIT_COMMIT \
            jenkins-source/pinned-source.bundle.sha256
        rmdir jenkins-source
        printf 'Restored pinned source commit: %s\n' "$SOURCE_GIT_COMMIT"
    '''
}

pipeline {
    agent none

    options {
        skipDefaultCheckout(true)
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '20', artifactNumToKeepStr: '10'))
        timeout(time: 120, unit: 'MINUTES')
    }

    environment {
        CARGO_TERM_COLOR = 'always'
        CARGO_NET_RETRY = '5'
        CARGO_HTTP_TIMEOUT = '120'
        RUST_BACKTRACE = '1'
        ELECTRON_MIRROR = 'https://npmmirror.com/mirrors/electron/'
        ELECTRON_BUILDER_BINARIES_MIRROR = 'https://npmmirror.com/mirrors/electron-builder-binaries/'
        EFFECTIVE_PACKAGE_TARGETS = 'x86_64-unknown-linux-gnu,x86_64-pc-windows-gnu'
    }

    stages {
        stage('Prepare source and release mode') {
            agent {
                label 'rust'
            }

            steps {
                script {
                    deleteDir()
                    def checkoutState = checkout scm
                    env.SOURCE_GIT_COMMIT = checkoutState.GIT_COMMIT ?: sh(
                        returnStdout: true,
                        script: 'git rev-parse HEAD'
                    ).trim()
                    if (!(env.SOURCE_GIT_COMMIT ==~ /[0-9a-f]{40}/)) {
                        error("无法确定本次构建的完整 Git commit：${env.SOURCE_GIT_COMMIT}")
                    }
                    sh 'git -c advice.detachedHead=false checkout --quiet --detach "$SOURCE_GIT_COMMIT"'
                    def packageVersion = sh(
                        returnStdout: true,
                        script: "sed -n 's/^version = \\\"\\(.*\\)\\\"/\\1/p' Cargo.toml | head -n 1"
                    ).trim()
                    if (!packageVersion) {
                        error('无法从 Cargo.toml 读取 workspace 版本。')
                    }
                    env.RELEASE_TAG = "v${packageVersion}"
                    def tagExists = sh(
                        returnStatus: true,
                        script: 'git show-ref --verify --quiet "refs/tags/$RELEASE_TAG"'
                    ) == 0
                    env.EFFECTIVE_BUILD_PROFILE = 'debug'
                    env.AUTOMATIC_RELEASE = 'false'
                    if (tagExists) {
                        def tagType = sh(
                            returnStdout: true,
                            script: 'git cat-file -t "$RELEASE_TAG"'
                        ).trim()
                        if (tagType == 'tag') {
                            def tagCommit = sh(
                                returnStdout: true,
                                script: 'git rev-parse "$RELEASE_TAG^{}"'
                            ).trim()
                            if (tagCommit == env.SOURCE_GIT_COMMIT) {
                                env.EFFECTIVE_BUILD_PROFILE = 'release'
                                env.AUTOMATIC_RELEASE = 'true'
                            } else {
                                echo "${env.RELEASE_TAG} 指向 ${tagCommit}；本次提交按 debug 构建，不发布。"
                            }
                        } else {
                            echo "${env.RELEASE_TAG} 不是 annotated tag；本次提交按 debug 构建，不发布。"
                        }
                    }
                }
                sh '''
                    test "$(git rev-parse HEAD)" = "$SOURCE_GIT_COMMIT"
                    printf 'Pinned source commit: %s\n' "$SOURCE_GIT_COMMIT"
                    printf 'Build profile: %s\n' "$EFFECTIVE_BUILD_PROFILE"
                    printf 'Release tag: %s (%s)\n' "$RELEASE_TAG" "$AUTOMATIC_RELEASE"
                '''
                sh '''
                    set -eu
                    mkdir -p jenkins-source
                    git bundle create jenkins-source/pinned-source.bundle HEAD --tags
                    printf '%s\n' "$SOURCE_GIT_COMMIT" >jenkins-source/SOURCE_GIT_COMMIT

                    if command -v sha256sum >/dev/null 2>&1; then
                        bundle_checksum="$(sha256sum jenkins-source/pinned-source.bundle | awk '{print $1}')"
                    elif command -v shasum >/dev/null 2>&1; then
                        bundle_checksum="$(LC_ALL=C LANG=C shasum -a 256 jenkins-source/pinned-source.bundle | awk '{print $1}')"
                    else
                        printf 'sha256sum or shasum is required to seal the pinned source bundle.\n' >&2
                        exit 1
                    fi
                    printf '%s\n' "$bundle_checksum" >jenkins-source/pinned-source.bundle.sha256

                    test "$(git bundle list-heads jenkins-source/pinned-source.bundle HEAD | awk '$2 == "HEAD" { print $1 }')" = "$SOURCE_GIT_COMMIT"
                    git bundle verify jenkins-source/pinned-source.bundle >/dev/null
                '''
                stash(
                    name: 'pinned-source-bundle',
                    includes: 'jenkins-source/pinned-source.bundle,jenkins-source/SOURCE_GIT_COMMIT,jenkins-source/pinned-source.bundle.sha256',
                    allowEmpty: false
                )
            }
        }

        stage('Linux CI and packages') {
            agent {
                label 'rust'
            }

            stages {
                stage('Linux source restore') {
                    steps {
                        script {
                            restorePinnedSource()
                        }
                    }
                }

                stage('Environment') {
                    steps {
                        sh 'ci/build.sh env'
                    }
                }

                stage('Fetch Dependencies') {
                    steps {
                        retry(3) {
                            sh 'ci/build.sh fetch'
                        }
                    }
                }

                stage('Format') {
                    steps {
                        sh 'ci/build.sh fmt'
                    }
                }

                stage('Clippy') {
                    steps {
                        sh 'ci/build.sh clippy'
                    }
                }

                stage('Test') {
                    steps {
                        sh 'ci/build.sh test'
                    }
                }

                stage('Host build') {
                    steps {
                        withEnv(["BUILD_PROFILE=${env.EFFECTIVE_BUILD_PROFILE}"]) {
                            sh 'ci/build.sh build "$BUILD_PROFILE"'
                        }
                    }
                }

                stage('Host smoke test') {
                    steps {
                        withEnv(["BUILD_PROFILE=${env.EFFECTIVE_BUILD_PROFILE}"]) {
                            sh 'ci/build.sh smoke "$BUILD_PROFILE"'
                        }
                    }
                }

                stage('Linux and Windows packages') {
                    steps {
                        withEnv([
                            "BUILD_PROFILE=${env.EFFECTIVE_BUILD_PROFILE}",
                            "PACKAGE_TARGETS=${env.EFFECTIVE_PACKAGE_TARGETS}"
                        ]) {
                            retry(2) {
                                sh 'ci/build.sh package "$PACKAGE_TARGETS" "$BUILD_PROFILE"'
                            }
                        }
                        stash(
                            name: 'linux-windows-packages',
                            includes: 'target/artifacts/*.tar.gz,target/artifacts/*.zip',
                            allowEmpty: false
                        )
                    }
                }
            }
        }

        stage('macOS packages') {
            agent {
                label 'rust-macos && arm64'
            }

            stages {
                stage('macOS source restore') {
                    steps {
                        script {
                            restorePinnedSource()
                        }
                    }
                }

                stage('macOS environment') {
                    steps {
                        sh 'ci/build-macos.sh env'
                    }
                }

                stage('macOS fetch') {
                    steps {
                        retry(3) {
                            sh 'ci/build-macos.sh fetch'
                        }
                    }
                }

                stage('macOS test') {
                    steps {
                        withEnv(["BUILD_PROFILE=${env.EFFECTIVE_BUILD_PROFILE}"]) {
                            sh 'ci/build-macos.sh test "$BUILD_PROFILE"'
                        }
                    }
                }

                stage('macOS arm64 and x86_64 package') {
                    steps {
                        withEnv(["BUILD_PROFILE=${env.EFFECTIVE_BUILD_PROFILE}"]) {
                            retry(2) {
                                sh 'ci/build-macos.sh package "$BUILD_PROFILE"'
                            }
                        }
                        stash(
                            name: 'macos-packages',
                            includes: 'target/macos-artifacts/*.zip',
                            allowEmpty: false
                        )
                    }
                }
            }
        }

        stage('Collect and archive packages') {
            agent {
                label 'rust'
            }

            steps {
                script {
                    restorePinnedSource()
                }
                unstash 'linux-windows-packages'
                unstash 'macos-packages'
                sh '''
                    mkdir -p target/artifacts
                    cp target/macos-artifacts/*.zip target/artifacts/
                '''
                sh 'ci/build.sh checksums'
                archiveArtifacts(
                    artifacts: 'target/artifacts/*.tar.gz,target/artifacts/*.zip,target/artifacts/SHA256SUMS',
                    allowEmptyArchive: false,
                    fingerprint: true
                )
                stash(
                    name: 'collected-packages',
                    includes: 'target/artifacts/*.tar.gz,target/artifacts/*.zip,target/artifacts/SHA256SUMS',
                    allowEmpty: false
                )
            }
        }

        stage('Publish GitHub Release') {
            when {
                beforeAgent true
                expression {
                    return env.AUTOMATIC_RELEASE == 'true'
                }
            }

            agent {
                label 'rust'
            }

            steps {
                script {
                    restorePinnedSource()
                }
                unstash 'collected-packages'
                withCredentials([
                    string(credentialsId: 'github-release-token', variable: 'GH_TOKEN')
                ]) {
                    retry(2) {
                        sh 'ci/publish-github-release.sh release'
                    }
                }
            }
        }
    }

    post {
        success {
            script {
                def commit = env.SOURCE_GIT_COMMIT ? env.SOURCE_GIT_COMMIT.take(12) : 'unknown'
                def platforms = 'Linux + Windows + macOS arm64/x86_64'
                def published = env.AUTOMATIC_RELEASE == 'true' ?
                    ' · GitHub Release 已发布' : ''
                currentBuild.description = "${commit} · ${env.EFFECTIVE_BUILD_PROFILE} · ${platforms}${published}"
            }
        }

        failure {
            echo 'Rust 构建或发布失败，请检查首个失败的 stage 日志。'
        }

        always {
            echo "Build result: ${currentBuild.currentResult}"
        }
    }
}
