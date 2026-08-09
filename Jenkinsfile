pipeline {
    agent {
        label 'rust'
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
            description: '构建、冒烟验证并归档 Release 二进制'
        )

        stringParam(
            name: 'PACKAGE_TARGETS',
            defaultValue: 'x86_64-unknown-linux-gnu,x86_64-pc-windows-gnu',
            description: '在当前 Linux agent 上交叉打包的 Rust target，逗号分隔；Windows 需要 mingw-w64'
        )

        booleanParam(
            name: 'BUILD_MACOS',
            defaultValue: false,
            description: '是否打包 macOS 产物；需要可用的 macOS Jenkins agent'
        )

        stringParam(
            name: 'MACOS_AGENT_LABEL',
            defaultValue: 'macos',
            description: '用于构建 macOS 产物的 Jenkins agent label'
        )

        stringParam(
            name: 'MACOS_TARGETS',
            defaultValue: 'aarch64-apple-darwin,x86_64-apple-darwin',
            description: 'macOS agent 上构建的 Rust target，逗号分隔'
        )
    }

    environment {
        CARGO_TERM_COLOR = 'always'
        CARGO_NET_RETRY = '5'
        CARGO_HTTP_TIMEOUT = '120'
        RUST_BACKTRACE = '1'
    }

    stages {
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

        stage('Release Build') {
            when {
                expression {
                    return params.BUILD_RELEASE
                }
            }

            steps {
                sh 'ci/build.sh build'
            }
        }

        stage('Smoke Test') {
            when {
                expression {
                    return params.BUILD_RELEASE
                }
            }

            steps {
                sh 'ci/build.sh smoke'
            }
        }

        stage('Package Cross Artifacts') {
            when {
                expression {
                    return params.BUILD_RELEASE
                }
            }

            steps {
                script {
                    def packageBranches = [:]

                    if (params.PACKAGE_TARGETS?.trim()) {
                        packageBranches['linux-windows-cross'] = {
                            sh "ci/build.sh package ${params.PACKAGE_TARGETS}"
                            archiveArtifacts(
                                artifacts: 'target/artifacts/*.tar.gz',
                                allowEmptyArchive: false,
                                fingerprint: true
                            )
                        }
                    }

                    if (params.BUILD_MACOS) {
                        packageBranches['macos'] = {
                            node(params.MACOS_AGENT_LABEL) {
                                checkout scm
                                sh "ci/build.sh package ${params.MACOS_TARGETS}"
                                archiveArtifacts(
                                    artifacts: 'target/artifacts/*.tar.gz',
                                    allowEmptyArchive: false,
                                    fingerprint: true
                                )
                            }
                        }
                    }

                    if (packageBranches.isEmpty()) {
                        echo 'No cross package targets enabled.'
                    } else {
                        parallel packageBranches
                    }
                }
            }
        }
    }

    post {
        success {
            archiveArtifacts(
                artifacts: 'target/release/serial,target/release/seriald,target/release/serialctl,target/release/serial-mcp',
                allowEmptyArchive: true,
                fingerprint: true
            )
        }

        failure {
            echo 'Rust CI 构建失败，请检查首个失败的 stage 日志。'
        }

        always {
            echo "Build result: ${currentBuild.currentResult}"
        }
    }
}
