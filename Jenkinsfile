pipeline {
    agent none

    options {
        skipDefaultCheckout(true)
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '20', artifactNumToKeepStr: '10'))
        timeout(time: 90, unit: 'MINUTES')
    }

    parameters {
        choice(
            name: 'BUILD_PROFILE',
            choices: ['release', 'debug'],
            description: '构建档位：release 生成正式优化产物；debug 生成带调试信息的测试产物，绝不会发布到 GitHub Release'
        )

        booleanParam(
            name: 'RUN_FORMAT_CHECK',
            defaultValue: true,
            description: '检查 Rust 代码格式'
        )

        booleanParam(
            name: 'RUN_CLIPPY',
            defaultValue: true,
            description: '运行 Clippy，并将警告视为错误'
        )

        booleanParam(
            name: 'RUN_TESTS',
            defaultValue: true,
            description: '运行 workspace 全部测试；启用 macOS 构建时也在 Mac 节点运行测试'
        )

        booleanParam(
            name: 'BUILD_PACKAGES',
            defaultValue: true,
            description: '打包并归档当前档位的可下载产物'
        )

        booleanParam(
            name: 'BUILD_MACOS',
            defaultValue: true,
            description: '在原生 Apple Silicon Jenkins Agent 上构建 macOS arm64 与 x86_64；需要 rust-macos && arm64 节点'
        )

        booleanParam(
            name: 'PUBLISH_GITHUB_RELEASE',
            defaultValue: false,
            description: '将完整的四平台 Release 产物直接发布到 GitHub；仅 release、全部质量检查和 macOS 构建均启用时允许'
        )

        string(
            name: 'PACKAGE_TARGETS',
            defaultValue: 'x86_64-unknown-linux-gnu,x86_64-pc-windows-gnu',
            description: 'Linux Agent 上的正式 target，逗号分隔；仅允许 x86_64 Linux glibc 2.31 与 Windows GNU/MinGW-w64'
        )

        booleanParam(
            name: 'BUILD_RELEASE',
            defaultValue: true,
            description: '[兼容旧调用，后续版本移除] release 档位的总开关；新构建请保持勾选，并使用 BUILD_PACKAGES 控制是否打包'
        )
    }

    environment {
        CARGO_TERM_COLOR = 'always'
        CARGO_NET_RETRY = '5'
        CARGO_HTTP_TIMEOUT = '120'
        RUST_BACKTRACE = '1'
        EFFECTIVE_BUILD_PROFILE = "${params.BUILD_PROFILE ?: 'release'}"
        EFFECTIVE_BUILD_PACKAGES = "${params.BUILD_PACKAGES == null ? true : params.BUILD_PACKAGES}"
        EFFECTIVE_BUILD_MACOS = "${params.BUILD_MACOS == null ? false : params.BUILD_MACOS}"
        EFFECTIVE_PUBLISH_GITHUB_RELEASE = "${params.PUBLISH_GITHUB_RELEASE == null ? false : params.PUBLISH_GITHUB_RELEASE}"
        EFFECTIVE_BUILD_RELEASE = "${params.BUILD_RELEASE == null ? true : params.BUILD_RELEASE}"
        EFFECTIVE_PACKAGE_TARGETS = "${params.PACKAGE_TARGETS ?: 'x86_64-unknown-linux-gnu,x86_64-pc-windows-gnu'}"
        EFFECTIVE_RUN_FORMAT_CHECK = "${params.RUN_FORMAT_CHECK == null ? true : params.RUN_FORMAT_CHECK}"
        EFFECTIVE_RUN_CLIPPY = "${params.RUN_CLIPPY == null ? true : params.RUN_CLIPPY}"
        EFFECTIVE_RUN_TESTS = "${params.RUN_TESTS == null ? true : params.RUN_TESTS}"
    }

    stages {
        stage('Prepare source and parameters') {
            agent {
                label 'rust'
            }

            steps {
                script {
                    if (env.EFFECTIVE_PUBLISH_GITHUB_RELEASE == 'true' &&
                        env.EFFECTIVE_BUILD_PROFILE != 'release') {
                        error('Debug 产物禁止发布到 GitHub Release。')
                    }
                    if (env.EFFECTIVE_PUBLISH_GITHUB_RELEASE == 'true' &&
                        (env.EFFECTIVE_BUILD_RELEASE != 'true' ||
                         env.EFFECTIVE_BUILD_PACKAGES != 'true' ||
                         env.EFFECTIVE_BUILD_MACOS != 'true')) {
                        error('GitHub Release 要求启用 BUILD_RELEASE、BUILD_PACKAGES 和 BUILD_MACOS。')
                    }
                    if (env.EFFECTIVE_PUBLISH_GITHUB_RELEASE == 'true' &&
                        (env.EFFECTIVE_RUN_FORMAT_CHECK != 'true' ||
                         env.EFFECTIVE_RUN_CLIPPY != 'true' ||
                         env.EFFECTIVE_RUN_TESTS != 'true')) {
                        error('GitHub Release 要求格式、Clippy 和测试三个质量检查全部启用。')
                    }
                    if (env.EFFECTIVE_PUBLISH_GITHUB_RELEASE == 'true' &&
                        env.EFFECTIVE_PACKAGE_TARGETS != 'x86_64-unknown-linux-gnu,x86_64-pc-windows-gnu') {
                        error('GitHub Release 要求构建完整且顺序固定的 Linux/Windows target 集。')
                    }

                    deleteDir()
                    def checkoutState = checkout scm
                    env.SOURCE_GIT_COMMIT = checkoutState.GIT_COMMIT ?: sh(
                        returnStdout: true,
                        script: 'git rev-parse HEAD'
                    ).trim()
                    if (!(env.SOURCE_GIT_COMMIT ==~ /[0-9a-f]{40}/)) {
                        error("无法确定本次构建的完整 Git commit：${env.SOURCE_GIT_COMMIT}")
                    }
                }
                sh '''
                    test "$(git rev-parse HEAD)" = "$SOURCE_GIT_COMMIT"
                    printf 'Pinned source commit: %s\n' "$SOURCE_GIT_COMMIT"
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
                    when {
                        expression {
                            return env.EFFECTIVE_RUN_FORMAT_CHECK == 'true'
                        }
                    }

                    steps {
                        sh 'ci/build.sh fmt'
                    }
                }

                stage('Clippy') {
                    when {
                        expression {
                            return env.EFFECTIVE_RUN_CLIPPY == 'true'
                        }
                    }

                    steps {
                        sh 'ci/build.sh clippy'
                    }
                }

                stage('Test') {
                    when {
                        expression {
                            return env.EFFECTIVE_RUN_TESTS == 'true'
                        }
                    }

                    steps {
                        sh 'ci/build.sh test'
                    }
                }

                stage('Host build') {
                    when {
                        expression {
                            return env.EFFECTIVE_BUILD_PROFILE == 'debug' ||
                                env.EFFECTIVE_BUILD_RELEASE == 'true'
                        }
                    }

                    steps {
                        withEnv(["BUILD_PROFILE=${env.EFFECTIVE_BUILD_PROFILE}"]) {
                            sh 'ci/build.sh build "$BUILD_PROFILE"'
                        }
                    }
                }

                stage('Host smoke test') {
                    when {
                        expression {
                            return env.EFFECTIVE_BUILD_PROFILE == 'debug' ||
                                env.EFFECTIVE_BUILD_RELEASE == 'true'
                        }
                    }

                    steps {
                        withEnv(["BUILD_PROFILE=${env.EFFECTIVE_BUILD_PROFILE}"]) {
                            sh 'ci/build.sh smoke "$BUILD_PROFILE"'
                        }
                    }
                }

                stage('Linux and Windows packages') {
                    when {
                        expression {
                            return env.EFFECTIVE_BUILD_PACKAGES == 'true' &&
                                (env.EFFECTIVE_BUILD_PROFILE == 'debug' ||
                                 env.EFFECTIVE_BUILD_RELEASE == 'true')
                        }
                    }

                    steps {
                        withEnv([
                            "BUILD_PROFILE=${env.EFFECTIVE_BUILD_PROFILE}",
                            "PACKAGE_TARGETS=${env.EFFECTIVE_PACKAGE_TARGETS}"
                        ]) {
                            sh 'ci/build.sh package "$PACKAGE_TARGETS" "$BUILD_PROFILE"'
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
            when {
                beforeAgent true
                expression {
                    return env.EFFECTIVE_BUILD_MACOS == 'true' &&
                        env.EFFECTIVE_BUILD_PACKAGES == 'true' &&
                        (env.EFFECTIVE_BUILD_PROFILE == 'debug' ||
                         env.EFFECTIVE_BUILD_RELEASE == 'true')
                }
            }

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
                    when {
                        expression {
                            return env.EFFECTIVE_RUN_TESTS == 'true'
                        }
                    }

                    steps {
                        withEnv(["BUILD_PROFILE=${env.EFFECTIVE_BUILD_PROFILE}"]) {
                            sh 'ci/build-macos.sh test "$BUILD_PROFILE"'
                        }
                    }
                }

                stage('macOS arm64 and x86_64 package') {
                    steps {
                        withEnv(["BUILD_PROFILE=${env.EFFECTIVE_BUILD_PROFILE}"]) {
                            sh 'ci/build-macos.sh package "$BUILD_PROFILE"'
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
            when {
                beforeAgent true
                expression {
                    return env.EFFECTIVE_BUILD_PACKAGES == 'true' &&
                        (env.EFFECTIVE_BUILD_PROFILE == 'debug' ||
                         env.EFFECTIVE_BUILD_RELEASE == 'true')
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
                unstash 'linux-windows-packages'
                script {
                    if (env.EFFECTIVE_BUILD_MACOS == 'true') {
                        unstash 'macos-packages'
                        sh '''
                            mkdir -p target/artifacts
                            cp target/macos-artifacts/*.zip target/artifacts/
                        '''
                    }
                }
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
                    return env.EFFECTIVE_PUBLISH_GITHUB_RELEASE == 'true'
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
                    sh 'ci/publish-github-release.sh release'
                }
            }
        }
    }

    post {
        success {
            script {
                def commit = env.SOURCE_GIT_COMMIT ? env.SOURCE_GIT_COMMIT.take(12) : 'unknown'
                def platforms = env.EFFECTIVE_BUILD_PACKAGES != 'true' ?
                    '未打包' : (env.EFFECTIVE_BUILD_MACOS == 'true' ?
                    'Linux + Windows + macOS arm64/x86_64' : 'Linux + Windows')
                def published = env.EFFECTIVE_PUBLISH_GITHUB_RELEASE == 'true' ?
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
