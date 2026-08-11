pipeline {
    agent {
        label 'rust'
    }

    options {
        skipDefaultCheckout(true)
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '20', artifactNumToKeepStr: '10'))
        timeout(time: 90, unit: 'MINUTES')
    }

    parameters {
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
            description: '运行 workspace 全部测试'
        )

        booleanParam(
            name: 'BUILD_RELEASE',
            defaultValue: true,
            description: '构建、验证并归档正式 Release 产物'
        )

        string(
            name: 'PACKAGE_TARGETS',
            defaultValue: 'x86_64-unknown-linux-gnu,x86_64-pc-windows-gnu',
            description: '正式 target，逗号分隔；Linux 由 cargo-zigbuild 固定到 glibc 2.31，Windows 使用 GNU/MinGW-w64 ABI'
        )
    }

    environment {
        CARGO_TERM_COLOR = 'always'
        CARGO_NET_RETRY = '5'
        CARGO_HTTP_TIMEOUT = '120'
        RUST_BACKTRACE = '1'
    }

    stages {
        stage('Clean checkout') {
            steps {
                deleteDir()
                checkout scm
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
                    return params.RUN_FORMAT_CHECK
                }
            }

            steps {
                sh 'ci/build.sh fmt'
            }
        }

        stage('Clippy') {
            when {
                expression {
                    return params.RUN_CLIPPY
                }
            }

            steps {
                sh 'ci/build.sh clippy'
            }
        }

        stage('Test') {
            when {
                expression {
                    return params.RUN_TESTS
                }
            }

            steps {
                sh 'ci/build.sh test'
            }
        }

        stage('Host Release Build') {
            when {
                expression {
                    return params.BUILD_RELEASE
                }
            }

            steps {
                sh 'ci/build.sh build'
            }
        }

        stage('Host Smoke Test') {
            when {
                expression {
                    return params.BUILD_RELEASE
                }
            }

            steps {
                sh 'ci/build.sh smoke'
            }
        }

        stage('Cross-build Release Packages') {
            when {
                expression {
                    return params.BUILD_RELEASE
                }
            }

            steps {
                withEnv(["RELEASE_PACKAGE_TARGETS=${params.PACKAGE_TARGETS}"]) {
                    sh 'ci/build.sh package "$RELEASE_PACKAGE_TARGETS"'
                }
            }
        }

        stage('Archive Release Packages') {
            when {
                expression {
                    return params.BUILD_RELEASE
                }
            }

            steps {
                archiveArtifacts(
                    artifacts: 'target/artifacts/*.tar.gz,target/artifacts/*.zip,target/artifacts/SHA256SUMS',
                    allowEmptyArchive: false,
                    fingerprint: true
                )
            }
        }
    }

    post {
        success {
            script {
                def commit = env.GIT_COMMIT ? env.GIT_COMMIT.take(12) : 'unknown'
                currentBuild.description = "${commit} · x86_64 Linux glibc 2.31 + Windows GNU"
            }
        }

        failure {
            echo 'Rust Release 构建失败，请检查首个失败的 stage 日志。'
        }

        always {
            echo "Build result: ${currentBuild.currentResult}"
        }
    }
}
