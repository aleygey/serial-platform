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
            }
        }

        stage('Linux CI and packages') {
            agent {
                label 'rust'
            }

            stages {
                stage('Linux checkout') {
                    steps {
                        deleteDir()
                        checkout scm
                        sh '''
                            git -c advice.detachedHead=false checkout --detach "$SOURCE_GIT_COMMIT"
                            test "$(git rev-parse HEAD)" = "$SOURCE_GIT_COMMIT"
                        '''
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
                stage('macOS checkout') {
                    steps {
                        deleteDir()
                        checkout scm
                        sh '''
                            git -c advice.detachedHead=false checkout --detach "$SOURCE_GIT_COMMIT"
                            test "$(git rev-parse HEAD)" = "$SOURCE_GIT_COMMIT"
                        '''
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
                deleteDir()
                checkout scm
                sh '''
                    git -c advice.detachedHead=false checkout --detach "$SOURCE_GIT_COMMIT"
                    test "$(git rev-parse HEAD)" = "$SOURCE_GIT_COMMIT"
                '''
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
                deleteDir()
                checkout scm
                sh '''
                    git -c advice.detachedHead=false checkout --detach "$SOURCE_GIT_COMMIT"
                    test "$(git rev-parse HEAD)" = "$SOURCE_GIT_COMMIT"
                '''
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
